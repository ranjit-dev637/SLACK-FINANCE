# ── Load .env FIRST — before any other imports that consume env vars ─────────
# - find_dotenv() locates the .env regardless of working directory or IDE launch path.
# - override=True ensures .env values always win over VS Code environment injection.
from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv(), override=True)

# Debug confirmation — printed once at startup so you can verify the correct URL is active
DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL:
    print(f"[main.py] DATABASE_URL loaded: {DATABASE_URL[:55]}...")
else:
    print("[main.py] WARNING: DATABASE_URL is not set — check your .env file!")

# Read and validate the Slack bot token at startup
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")

import threading
import re
import uuid
from datetime import datetime
from typing import Any, Optional

from fastapi import FastAPI, Request, UploadFile, File, Depends, HTTPException, Form, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import requests
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel
from sqlalchemy.orm import Session

# Slack
from slack_bolt import App as SlackApp
from slack_bolt.adapter.fastapi import SlackRequestHandler

# DB
from database import SessionLocal
import models
from models import Expense, Income, Transaction

import logging
from services.parser import parse_razorpay_message
from services.db_service import (
    insert_razorpay_income,
    insert_income_form_record,
    get_pending_income,
    insert_expense_record,
    get_pending_expense,
)
from services.validation import validate_income_data
from services.file_handler import process_income_file
from services.supabase_storage import upload_file_to_storage
from services.google_drive import upload_to_drive
from services.slack_downloader import download_slack_file
from services.upload_pipeline import process_upload
from services.circuit_breaker import all_breaker_states

# ==============================
# FILE VALIDATION CONSTANTS
# ==============================
MAX_FILE_SIZE = 5 * 1024 * 1024  # 5 MB
ALLOWED_TYPES = ["image/jpeg", "image/png", "application/pdf"]

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Debug log — confirms the correct token is active at runtime
# ⚠️  REMINDER: restart the server after any .env changes
if SLACK_BOT_TOKEN:
    logger.info(f"ACTIVE SLACK TOKEN: {SLACK_BOT_TOKEN[:12]}...")
else:
    logger.error("SLACK_BOT_TOKEN is NOT set — check your .env file!")

app = FastAPI(title="Finance API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # allow all origins for development
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
@app.get("/", tags=["Health"])
def root():
    return {
        "status": "ok",
        "service": "TransactFlow API",
        "message": "API is running successfully"
    }

@app.get("/favicon.ico")
def favicon():
    return {}

# ==============================
# HEALTH CHECK
# ==============================
@app.get("/health", summary="Service health check", tags=["Ops"])
def health_check():
    """
    Returns the operational health of the service including:
    - Database connectivity
    - Circuit breaker states for Drive and Supabase
    """
    from datetime import timezone as tz

    # Probe the database with a lightweight query
    db_status = "ok"
    try:
        from database import SessionLocal
        from sqlalchemy import text as _text
        _db = SessionLocal()
        _db.execute(_text("SELECT 1"))
        _db.close()
    except Exception as _db_err:
        db_status = f"degraded: {_db_err}"
        logger.error("health_check_db_failed | error=%s", _db_err)

    circuits = all_breaker_states()
    overall  = "ok" if db_status == "ok" else "degraded"

    return {
        "status":           overall,
        "service":          "upload_pipeline",
        "db":               db_status,
        "drive_circuit":    circuits.get("drive",    "CLOSED"),
        "supabase_circuit": circuits.get("supabase", "CLOSED"),
        "timestamp":        datetime.now(tz.utc).isoformat(),
    }

# ==============================
# DATABASE DEPENDENCY
# ==============================
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ==============================
# UTILS
# ==============================
def safe_float(val):
    try:
        return float(val) if val else 0.0
    except (ValueError, TypeError):
        return 0.0


# ==============================
# SLACK APP INIT
# ==============================
slack_app = SlackApp(
    token=SLACK_BOT_TOKEN,
    signing_secret=os.getenv("SLACK_SIGNING_SECRET"),
    # process_before_response=True tells Bolt not to enforce its own 3-second
    # internal timeout. All handlers call ack() immediately and run heavy work
    # in daemon threads, so this is safe and eliminates operation_timeout warnings.
    process_before_response=True,
)

app_handler = SlackRequestHandler(slack_app)

# ══════════════════════════════════════════════════════════════════════════════
# DB HELPERS & BLOCK KIT MESSAGE BUILDERS
# ══════════════════════════════════════════════════════════════════════════════
def fetch_transaction_record(transaction_id: str, record_type: str):
    db = SessionLocal()
    try:
        from models import Income, Expense
        if record_type == "income":
            return db.query(Income).filter_by(transaction_id=transaction_id).first()
        else:
            return db.query(Expense).filter_by(transaction_id=transaction_id).first()
    finally:
        db.close()


# ── Message event handler ──────────────────────────────────────────────────────
@slack_app.event("message")
def handle_message_events(body, logger, client):
    # Capture all event fields immediately — before the thread starts,
    # because Bolt may reclaim the body dict after the handler returns.
    event   = body.get("event", {})
    text    = event.get("text", "")
    channel = event.get("channel")
    subtype = event.get("subtype")
    user_id = event.get("user", "UNKNOWN")
    files   = event.get("files", [])

    # ── Ignore bot messages immediately (no thread needed) ─────────────────
    if subtype == "bot_message":
        return

    # ── Run ALL heavy work in a background thread ──────────────────────────
    # This keeps the Bolt handler non-blocking and prevents operation_timeout.
    def _process_message():

        # ══════════════════════════════════════════════════════════════════
        # FILE UPLOAD (Now Handled by FastAPI Interceptor in /slack/events)
        # ══════════════════════════════════════════════════════════════════
        if subtype == "file_share":
            # Native file processing is removed from the Bolt handler to 
            # prevent 3-second timeouts. File parsing is now intercepted 
            # before it reaches Bolt.
            return

        # ══════════════════════════════════════════════════════════════════
        # AUTO RAZORPAY DETECTION
        # ══════════════════════════════════════════════════════════════════
        if subtype is None and text and "You received a new payment" in text:
            logger.info("Razorpay Payment Message Detected")
            try:
                parsed_data = parse_razorpay_message(text)

                logger.info("--- EXTRACTED DATA ---")
                logger.info(f"Raw Message: {text}")
                logger.info(f"Amount: {parsed_data.get('amount')}")
                logger.info(f"Contact: {parsed_data.get('contact')}")
                logger.info(f"Date: {parsed_data.get('captured_date')}")
                logger.info("----------------------")

                amount = insert_razorpay_income(parsed_data, user_id)

                if channel:
                    client.chat_postMessage(
                        channel=channel,
                        text=f"✅ Auto-recorded Razorpay payment: ₹{amount}"
                    )

                logger.info(f"Razorpay income processed | amount=₹{amount} | user={user_id}")

            except Exception as e:
                logger.error(f"Failed to process Razorpay message: {e}")

    threading.Thread(target=_process_message, daemon=True).start()

# ==============================
# PROPERTY LIST
# ==============================
PROPERTIES = [
    {"text": {"type": "plain_text", "text": "Clovera"},        "value": "Clovera"},
    {"text": {"type": "plain_text", "text": "Clover Villa"},   "value": "Clover Villa"},
    {"text": {"type": "plain_text", "text": "Central"},        "value": "Central"},
    {"text": {"type": "plain_text", "text": "Clover Connect"}, "value": "Clover Connect"},
    {"text": {"type": "plain_text", "text": "Kitchen"},        "value": "Kitchen"},
    {"text": {"type": "plain_text", "text": "Clover Woods"},   "value": "Clover Woods"},
    {"text": {"type": "plain_text", "text": "Default"},        "value": "Default"},
]





# ==============================
# PYDANTIC SCHEMAS (Swagger)
# ==============================
class ExpenseCreate(BaseModel):
    expense_name: str
    seller_name: str
    total_amount: float
    gst_amount: float = 0.0
    purchase_date: Optional[str] = None
    paid_by: str
    mode_of_payment: str
    priority: str = "Medium"
    for_property: Any 
    submitted_by: str


class IncomeCreate(BaseModel):
    name: str  # customer
    booking_number: str
    contact_number: Optional[str] = None
    captured_date: Optional[str] = None
    room_amount: float
    food_amount: float
    payment_type: str
    receipt_by: str
    for_property: Any
    submitted_by: str


class TransactionCreate(BaseModel):
    name: str
    email: str
    amount: Optional[float] = None




# ==============================
# SLASH COMMANDS
# ==============================
@slack_app.command("/income")
def handle_income_command(ack, body, client):
    print("SLASH COMMAND RECEIVED")
    ack()
    print("ACK SENT")
    print("OPENING MODAL")
    client.views_open(
        trigger_id=body["trigger_id"],
        view=get_income_modal()
    )
    print("MODAL OPENED")

@slack_app.command("/expense")
def handle_expense_command(ack, body, client):
    print("SLASH COMMAND RECEIVED")
    ack()
    print("ACK SENT")
    print("OPENING MODAL")
    client.views_open(
        trigger_id=body["trigger_id"],
        view=get_expense_modal()
    )
    print("MODAL OPENED")


# ==============================
# MODAL DEFINITIONS
# ==============================
def get_income_modal():
    return {
        "type": "modal",
        "callback_id": "income_form",
        "title": {"type": "plain_text", "text": "Record Income"},
        "submit": {"type": "plain_text", "text": "Submit"},
        "close": {"type": "plain_text", "text": "Cancel"},
        "blocks": [
            # ── SECTION 1: Property & Customer Details ──
            {"type": "header", "text": {"type": "plain_text", "text": "Property & Guest Details"}},
            {
                "type": "input",
                "block_id": "income_property",
                "element": {
                    "type": "radio_buttons",
                    "action_id": "property_input",
                    "options": [
                        {"text": {"type": "plain_text", "text": "Clover Villa"}, "value": "Clover Villa"},
                        {"text": {"type": "plain_text", "text": "Clovera"}, "value": "Clovera"},
                        {"text": {"type": "plain_text", "text": "Clover Woods"}, "value": "Clover Woods"},
                        {"text": {"type": "plain_text", "text": "Clover Connect"}, "value": "Clover Connect"},
                    ]
                },
                "label": {"type": "plain_text", "text": "Property"}
            },
            {
                "type": "input",
                "block_id": "income_name",
                "element": {"type": "plain_text_input", "action_id": "name_input", "placeholder": {"type": "plain_text", "text": "Guest / Customer name"}},
                "label": {"type": "plain_text", "text": "Name"}
            },
            {
                "type": "input",
                "block_id": "income_receipt_date",
                "element": {"type": "datepicker", "action_id": "receipt_date_input"},
                "label": {"type": "plain_text", "text": "Receipt Date"}
            },
            {
                "type": "input",
                "block_id": "income_booking",
                "element": {"type": "plain_text_input", "action_id": "booking_input", "placeholder": {"type": "plain_text", "text": "Booking number"}},
                "label": {"type": "plain_text", "text": "Booking Number"}
            },

            # ── SECTION 2: Payment Details ──
            {"type": "header", "text": {"type": "plain_text", "text": "Payment Details"}},
            {
                "type": "input",
                "block_id": "income_payment_type",
                "element": {
                    "type": "static_select",
                    "action_id": "payment_type_input",
                    "placeholder": {"type": "plain_text", "text": "Select payment type"},
                    "options": [
                        {"text": {"type": "plain_text", "text": "Cash"}, "value": "Cash"},
                        {"text": {"type": "plain_text", "text": "Bank Transfer"}, "value": "Bank Transfer"},
                        {"text": {"type": "plain_text", "text": "UPI"}, "value": "UPI"},
                        {"text": {"type": "plain_text", "text": "Credit Card"}, "value": "Credit Card"},
                        {"text": {"type": "plain_text", "text": "ICICI Bank POS"}, "value": "ICICI Bank POS"},
                        {"text": {"type": "plain_text", "text": "HDFC Bank POS"}, "value": "HDFC Bank POS"},
                        {"text": {"type": "plain_text", "text": "QR Code Standy (ICICI)"}, "value": "QR Code Standy (ICICI)"},
                    ]
                },
                "label": {"type": "plain_text", "text": "Payment Type"}
            },
            {
                "type": "input",
                "block_id": "income_room_amount",
                "element": {"type": "plain_text_input", "action_id": "room_amount_input", "placeholder": {"type": "plain_text", "text": "0"}},
                "label": {"type": "plain_text", "text": "Amount Collected for Room"}
            },
            {
                "type": "input",
                "block_id": "income_food_amount",
                "element": {"type": "plain_text_input", "action_id": "food_amount_input", "placeholder": {"type": "plain_text", "text": "0"}},
                "label": {"type": "plain_text", "text": "Amount Collected for Food"}
            },

            # ── SECTION 4: Receipt By ──
            {"type": "header", "text": {"type": "plain_text", "text": "Submission Details"}},
            {
                "type": "input",
                "block_id": "income_receipt_by",
                "element": {"type": "plain_text_input", "action_id": "receipt_by_input", "placeholder": {"type": "plain_text", "text": "Who received the payment?"}},
                "label": {"type": "plain_text", "text": "Receipt By"}
            }
        ]
    }

def get_expense_modal():
    return {
        "type": "modal",
        "callback_id": "expense_form",
        "title": {"type": "plain_text", "text": "Record Expense"},
        "submit": {"type": "plain_text", "text": "Submit"},
        "close": {"type": "plain_text", "text": "Cancel"},
        "blocks": [
            # ── SECTION 1: Expense Details ──
            {"type": "header", "text": {"type": "plain_text", "text": "Expense Details"}},
            {
                "type": "input",
                "block_id": "expense_name",
                "element": {"type": "plain_text_input", "action_id": "name_input", "placeholder": {"type": "plain_text", "text": "What was purchased?"}},
                "label": {"type": "plain_text", "text": "Expense Name"}
            },
            {
                "type": "input",
                "block_id": "expense_seller",
                "element": {"type": "plain_text_input", "action_id": "seller_input", "placeholder": {"type": "plain_text", "text": "Seller / Vendor name"}},
                "label": {"type": "plain_text", "text": "Seller Name"}
            },
            {
                "type": "input",
                "block_id": "expense_gst",
                "optional": True,
                "element": {"type": "plain_text_input", "action_id": "gst_input", "placeholder": {"type": "plain_text", "text": "0"}},
                "label": {"type": "plain_text", "text": "GST Amount (₹)"}
            },
            {
                "type": "input",
                "block_id": "expense_total",
                "element": {"type": "plain_text_input", "action_id": "total_input", "placeholder": {"type": "plain_text", "text": "Total amount"}},
                "label": {"type": "plain_text", "text": "Total Amount (₹)"}
            },
            {
                "type": "input",
                "block_id": "expense_date",
                "element": {"type": "datepicker", "action_id": "date_input"},
                "label": {"type": "plain_text", "text": "Purchase Date"}
            },
            # ── SECTION 2: Priority ──
            {"type": "header", "text": {"type": "plain_text", "text": "Priority"}},
            {
                "type": "input",
                "block_id": "expense_priority",
                "element": {
                    "type": "static_select",
                    "action_id": "priority_input",
                    "options": [
                        {"text": {"type": "plain_text", "text": "Critical"}, "value": "Critical"},
                        {"text": {"type": "plain_text", "text": "High"}, "value": "High"},
                        {"text": {"type": "plain_text", "text": "Medium"}, "value": "Medium"},
                        {"text": {"type": "plain_text", "text": "Low"}, "value": "Low"},
                    ]
                },
                "label": {"type": "plain_text", "text": "Priority"}
            },

            # ── SECTION 3: Payment Method ──
            {"type": "header", "text": {"type": "plain_text", "text": "Payment Method"}},
            {
                "type": "input",
                "block_id": "expense_paid_by",
                "element": {"type": "plain_text_input", "action_id": "paid_by_input", "placeholder": {"type": "plain_text", "text": "Who paid?"}},
                "label": {"type": "plain_text", "text": "Paid By"}
            },
            {
                "type": "input",
                "block_id": "expense_mode",
                "element": {
                    "type": "static_select",
                    "action_id": "mode_input",
                    "options": [
                        {"text": {"type": "plain_text", "text": "Cash"}, "value": "Cash"},
                        {"text": {"type": "plain_text", "text": "Bank"}, "value": "Bank"},
                        {"text": {"type": "plain_text", "text": "UPI"}, "value": "UPI"},
                        {"text": {"type": "plain_text", "text": "Credit Card"}, "value": "Credit Card"},
                        {"text": {"type": "plain_text", "text": "Petty Cash"}, "value": "Petty Cash"},
                        {"text": {"type": "plain_text", "text": "Company"}, "value": "Company"},
                    ]
                },
                "label": {"type": "plain_text", "text": "Mode of Payment"}
            },

            # ── SECTION 4: Property Selection ──
            {"type": "header", "text": {"type": "plain_text", "text": "Property Selection"}},
            {
                "type": "input",
                "block_id": "expense_property",
                "element": {
                    "type": "radio_buttons",
                    "action_id": "property_input",
                    "options": [
                        {"text": {"type": "plain_text", "text": "Clover Villa"}, "value": "Clover Villa"},
                        {"text": {"type": "plain_text", "text": "Kitchen"}, "value": "Kitchen"},
                        {"text": {"type": "plain_text", "text": "Clovera"}, "value": "Clovera"},
                        {"text": {"type": "plain_text", "text": "Clover Woods"}, "value": "Clover Woods"},
                        {"text": {"type": "plain_text", "text": "Central"}, "value": "Central"},
                        {"text": {"type": "plain_text", "text": "Default"}, "value": "Default"},
                        {"text": {"type": "plain_text", "text": "Clover Connect"}, "value": "Clover Connect"},
                    ]
                },
                "label": {"type": "plain_text", "text": "Property"}
            }
        ]
    }


# ==============================
# MODAL SUBMISSION HANDLERS
# ==============================
@slack_app.view("income_form")
def handle_income_submission(ack, body, client, view):
    try:
        ack()
        logger.info("ACK SENT")
        logger.info("INCOME SUBMISSION RECEIVED")
        user_id = body["user"]["id"]
        values = view["state"]["values"]

        # ── Extract all form fields ──
        property_val = values["income_property"]["property_input"]["selected_option"]["value"]
        name = values["income_name"]["name_input"]["value"]
        receipt_date_str = values["income_receipt_date"]["receipt_date_input"].get("selected_date")
        booking_number = values["income_booking"]["booking_input"]["value"]
        payment_type = values["income_payment_type"]["payment_type_input"]["selected_option"]["value"]
        room_amount = safe_float(values["income_room_amount"]["room_amount_input"]["value"])
        food_amount = safe_float(values["income_food_amount"]["food_amount_input"]["value"])
        receipt_by = values["income_receipt_by"]["receipt_by_input"]["value"]
        
        logger.info("FIELDS EXTRACTED")

        # ── Parse receipt date ──
        receipt_date = None
        if receipt_date_str:
            try:
                receipt_date = datetime.strptime(receipt_date_str, "%Y-%m-%d").date()
            except Exception:
                receipt_date = datetime.now().date()
        else:
            receipt_date = datetime.now().date()

        transaction_id = f"TXN-{uuid.uuid4().hex[:8]}"
        logger.info("TRANSACTION ID GENERATED")

        # ── Save PENDING record to DB ──
        db = SessionLocal()
        logger.info("DB SESSION CREATED")
        try:
            logger.info("INSERTING INCOME ROW")
            new_income = Income(
                transaction_id=transaction_id,
                user_id=user_id,
                status="PENDING",
                file_uploaded=False,
                submitted_at=datetime.utcnow(),
                name=name,
                booking_number=booking_number,
                contact_number="N/A",
                captured_date=receipt_date,
                receipt_date=receipt_date,
                room_amount=room_amount,
                food_amount=food_amount,
                payment_type=payment_type,
                receipt_by=receipt_by,
                for_property={"name": property_val},
                submitted_by=user_id
            )
            logger.info("ABOUT TO INSERT TO DB")
            db.add(new_income)
            logger.info("DB COMMIT STARTED")
            db.commit()
            logger.info("DB COMMIT SUCCESS")
            db.refresh(new_income)
            record_id = new_income.id
            logger.info("TRANSACTION SAVED:\n" + transaction_id)
            logger.info(f"TRANSACTION CREATED: {transaction_id}")
            logger.info(f"PENDING: {transaction_id}")
        except Exception as e:
            db.rollback()
            logger.error(f"Income modal submission DB error: {e}", exc_info=True)
            try:
                client.chat_postMessage(channel=user_id, text=f"❌ Error creating income record: {e}")
            except Exception as msg_e:
                logger.error(f"Failed to send DB error message: {msg_e}", exc_info=True)
            return
        finally:
            db.close()

        logger.info("ABOUT TO SEND MESSAGE")
        try:
            client.chat_postMessage(
                channel=user_id,
                text=f"✅ Income form received\n\nTransaction ID: {transaction_id}\n\n📎 Please upload the payment screenshot in this chat to complete the submission.\n\nStatus: PENDING"
            )
            logger.info("MESSAGE SENT SUCCESSFULLY")
        except Exception as msg_e:
            logger.error(f"Failed to send screenshot request message: {msg_e}", exc_info=True)

    except Exception as top_e:
        logger.error(f"handle_income_submission top-level exception: {top_e}", exc_info=True)

@slack_app.view("expense_form")
def handle_expense_submission(ack, body, client, view):
    ack()
    logger.info("EXPENSE SUBMISSION RECEIVED")
    user_id = body["user"]["id"]
    values = view["state"]["values"]
    
    expense_name = values["expense_name"]["name_input"]["value"]
    seller_name = values["expense_seller"]["seller_input"]["value"]
    total_amount = safe_float(values["expense_total"]["total_input"]["value"])
    gst_amount = safe_float((values.get("expense_gst", {}).get("gst_input", {}) or {}).get("value", "0"))
    purchase_date_str = values["expense_date"]["date_input"].get("selected_date")
    paid_by = values["expense_paid_by"]["paid_by_input"]["value"]
    mode_of_payment = values["expense_mode"]["mode_input"]["selected_option"]["value"]
    priority = values["expense_priority"]["priority_input"]["selected_option"]["value"]
    property_val = values["expense_property"]["property_input"]["selected_option"]["value"]
    
    purchase_date = None
    if purchase_date_str:
        try:
            purchase_date = datetime.strptime(purchase_date_str, "%Y-%m-%d").date()
        except Exception:
            purchase_date = datetime.now().date()
    else:
        purchase_date = datetime.now().date()
    
    transaction_id = f"TXN-{uuid.uuid4().hex[:8]}"
    logger.info("TRANSACTION ID GENERATED")
    
    db = SessionLocal()
    logger.info("DB SESSION CREATED")
    try:
        logger.info("INSERTING EXPENSE ROW")
        new_expense = Expense(
            transaction_id=transaction_id,
            user_id=user_id,
            status="PENDING",
            file_uploaded=False,
            submitted_at=datetime.utcnow(),
            expense_name=expense_name,
            seller_name=seller_name,
            total_amount=total_amount,
            gst_amount=gst_amount,
            purchase_date=purchase_date,
            paid_by=paid_by,
            mode_of_payment=mode_of_payment,
            priority=priority,
            for_property={"name": property_val},
            submitted_by=user_id
        )
        db.add(new_expense)
        logger.info("DB COMMIT STARTED")
        db.commit()
        logger.info("DB COMMIT SUCCESS")
        db.refresh(new_expense)
        record_id = new_expense.id
        logger.info("TRANSACTION SAVED:\n" + transaction_id)
        logger.info(f"TRANSACTION CREATED: {transaction_id}")
        logger.info(f"PENDING: {transaction_id}")
    except Exception as e:
        db.rollback()
        logger.error(f"Expense modal submission DB error: {e}", exc_info=True)
        client.chat_postMessage(channel=user_id, text=f"❌ Error creating transaction: {e}")
        return
    finally:
        db.close()
        
    client.chat_postMessage(
        channel=user_id,
        text=f"✅ Expense form received\n\nTransaction ID: {transaction_id}\n\n📎 Please upload the payment screenshot in this chat to complete the submission.\n\nStatus: PENDING"
    )

import collections
import time
import json

# Thread-safe bounded cache for deduplicating Slack events
PROCESSED_EVENTS = {}

def is_event_processed(event_id):
    return event_id in PROCESSED_EVENTS

def mark_event_processed(event_id):
    if not event_id: return
    PROCESSED_EVENTS[event_id] = True

def extract_txn_from_message(text: str, user_id: str) -> str | None:
    import re
    if text:
        match = re.search(r'\bTXN-[a-z0-9]+\b', text)
        if match:
            return match.group(0)

    db = SessionLocal()
    try:
        from models import Income, Expense
        
        income = db.query(Income).filter(
            Income.user_id == user_id,
            Income.status == "PENDING",
            Income.payment_screenshot == None
        ).order_by(Income.submitted_at.desc()).first()
        
        expense = db.query(Expense).filter(
            Expense.user_id == user_id,
            Expense.status == "PENDING",
            Expense.receipt_copy == None
        ).order_by(Expense.submitted_at.desc()).first()
        
        most_recent = None
        if income and expense:
            if income.submitted_at > expense.submitted_at:
                most_recent = income
            else:
                most_recent = expense
        else:
            most_recent = income or expense
            
        if most_recent:
            return most_recent.transaction_id
            
    finally:
        db.close()
        
    slack_app.client.chat_postMessage(
        channel=user_id,
        text="❌ No pending submission found. Please submit /income or /expense first."
    )
    return None

def process_slack_file_event(event):
    logger.info("PROCESS_SLACK_FILE_EVENT FUNCTION CALLED")
    logger.info("THREAD EXECUTION CONFIRMED")
    logger.info("PROCESS_SLACK_FILE_EVENT STARTED")
    user_id = event.get("user") or event.get("user_id")
    transaction_id = None
    msg_ts = None
    msg_channel = None
    try:
        file_info = None
        text = event.get("text", "")
        
        if "files" in event and len(event["files"]) > 0:
            file_info = event["files"][0]
        elif event.get("type") == "file_shared" or event.get("file_id"):
            file_id = event.get("file_id") or event.get("file", {}).get("id")
            if file_id:
                try:
                    res = slack_app.client.files_info(file=file_id)
                    file_info = res.get("file")
                except Exception as e:
                    logger.error(f"Failed to fetch file info: {e}")
                    
        if not file_info:
            return
            
        file_url = file_info.get("url_private_download") or file_info.get("url_private")
        
        if not file_url or not user_id:
            return
            
        if is_event_processed(f"url_{file_url}"):
            logger.info("FILE URL ALREADY PROCESSED - DUPLICATE THREAD PREVENTED")
            return
        mark_event_processed(f"url_{file_url}")
            
        transaction_id = extract_txn_from_message(text, user_id)
        if not transaction_id:
            logger.info("No PENDING transaction matched for the uploaded file.")
            return
            
        logger.info(f"TRANSACTION MATCHED:\n{transaction_id}")
            
        db = SessionLocal()
        try:
            from models import Income, Expense
            
            record_type = None
            ModelClass = None
            
            # Lock record before update
            record = db.query(Income).filter(Income.transaction_id == transaction_id).with_for_update().first()
            if record:
                record_type = "income"
                ModelClass = Income
            else:
                record = db.query(Expense).filter(Expense.transaction_id == transaction_id).with_for_update().first()
                if record:
                    record_type = "expense"
                    ModelClass = Expense
                else:
                    raise ValueError(f"Transaction {transaction_id} not found in database.")
                
            # Prevent duplicate file upload
            if getattr(record, 'file_uploaded', False):
                slack_app.client.chat_postMessage(
                    channel=user_id,
                    text=f"⚠️ Screenshot already uploaded for `{transaction_id}`. No action taken."
                )
                return
                
            record_id = record.id
            db.commit()
        except Exception as e:
            db.rollback()
            raise e
        finally:
            db.close()

        # ── Send Initial Processing Message ──
        processing_text = f"⏳ Screenshot detected\n\nTransaction ID: {transaction_id}\n\n🔄 Upload processing started...\n🔍 Verifying screenshot integrity...\n☁ Uploading to Google Drive...\n\nStatus: PROCESSING"
        msg_response = slack_app.client.chat_postMessage(channel=user_id, text=processing_text)
        msg_ts = msg_response["ts"]
        msg_channel = msg_response["channel"]
        logger.info("PROCESSING MESSAGE SENT")
        logger.info(f"PROCESSING STARTED for {transaction_id}")

        logger.info("UPLOAD PIPELINE STARTED")
        logger.info(f"DOWNLOAD STARTED for {transaction_id}")
        from services.slack_downloader import download_slack_file
        file_bytes, mime_type = download_slack_file(file_url)
        
        logger.info(f"VALIDATION PASSED / DRIVE UPLOAD STARTED for {transaction_id}")
        result = process_upload(
            record_id=record_id,
            transaction_id=transaction_id,
            file_bytes=file_bytes,
            mime_type=mime_type,
            file_index=1,
            record_type=record_type,
            submitted_by_id=user_id,
            submitted_by_name="Slack User"
        )
        
        logger.info(f"DRIVE VERIFIED / SUPABASE UPDATED for {transaction_id}")
        drive_link = result.get("drive_link")
        
        payment_summary = ""
        try:
            import google.generativeai as genai
            import base64
            import os
            
            gemini_key = os.getenv("GEMINI_API_KEY")
            logger.info(f"GEMINI KEY FOUND: {bool(gemini_key)}")
            
            genai.configure(api_key=gemini_key)
            model = genai.GenerativeModel("gemini-1.5-flash")
            
            logger.info(f"FILE BYTES LENGTH: {len(file_bytes)}")
            logger.info(f"MIME TYPE: {mime_type}")
            
            image_part = {
                "mime_type": mime_type if mime_type else "image/jpeg",
                "data": file_bytes
            }
            
            prompt = "This is a payment screenshot. Extract only these fields if visible: Amount, Paid To, Date, UPI Ref ID, Payment Method. Reply in this exact format (skip any field not found):\nAmount: ₹___\nPaid To: ___\nDate: ___\nUPI Ref ID: ___\nPayment Method: ___"
            
            response = model.generate_content([prompt, image_part])
            payment_summary = response.text.strip()
            logger.info(f"GEMINI RESPONSE: {payment_summary}")
            
        except Exception as vision_err:
            logger.error(f"GEMINI FAILED: {vision_err}", exc_info=True)
            payment_summary = "_Could not extract payment details._"
        
        success_text = f"✅ Screenshot uploaded successfully\n\nTransaction ID: {transaction_id}\n\n{payment_summary}\n\n📂 Google Drive Upload: COMPLETED\n🗄 Supabase Update: COMPLETED\n🔍 Binary Validation: PASSED\n📎 Screenshot Verification: PASSED\n\n🔗 Drive Link:\n<{drive_link}>\n\nStatus: COMPLETED"
        
        slack_app.client.chat_update(
            channel=msg_channel,
            ts=msg_ts,
            text=success_text
        )
        logger.info(f"TRANSACTION COMPLETED for {transaction_id}")

    except Exception as e:
        logger.error(f"PIPELINE FAILED: {str(e)}", exc_info=True)
        if user_id:
            fail_text = f"❌ Screenshot upload failed\n\nTransaction ID: {transaction_id or 'UNKNOWN'}\n\nReason:\n{str(e)}\n\nStatus: FAILED\n\nPlease upload a valid screenshot again."
            
            # If we successfully sent the processing message, edit it. Otherwise, send a new one.
            if msg_ts and msg_channel:
                slack_app.client.chat_update(channel=msg_channel, ts=msg_ts, text=fail_text)
            else:
                slack_app.client.chat_postMessage(channel=user_id, text=fail_text)
            
        if transaction_id:
            db = SessionLocal()
            try:
                from sqlalchemy.sql import text as sql_text
                for table_name in ["incomes", "expenses"]:
                    query = sql_text(f"UPDATE {table_name} SET status = 'FAILED', error_message = :err, updated_at = NOW() WHERE transaction_id = :txn")
                    db.execute(query, {"err": str(e), "txn": transaction_id})
                db.commit()
            except Exception:
                db.rollback()
            finally:
                db.close()

def handle_other_events(payload: dict):
    try:
        logger.info("HANDLE_OTHER_EVENTS CALLED")
        event = payload.get("event", {})
        event_type = event.get("type")
        
        if event_type == "message":
            logger.info("DISPATCHING TO HANDLE_MESSAGE_EVENTS")
            handle_message_events(payload, logger, slack_app.client)
        else:
            logger.info(f"UNHANDLED EVENT TYPE: {event_type}")
            
    except Exception as e:
        logger.error(f"Error handling other events: {e}", exc_info=True)

@app.post("/slack/commands")
async def slack_commands(request: Request):
    return await app_handler.handle(request)

@app.post("/slack/interactive")
async def slack_interactive(request: Request):
    body_bytes = await request.body()
    try:
        import urllib.parse
        import json
        import threading
        
        body_str = body_bytes.decode("utf-8")
        parsed = urllib.parse.parse_qs(body_str)
        payload_str = parsed.get("payload", [None])[0]
        
        if payload_str:
            payload = json.loads(payload_str)
            payload_type = payload.get("type")
            
            if payload_type == "view_submission":
                callback_id = payload.get("view", {}).get("callback_id")
                
                def run_handler():
                    if callback_id == "income_form":
                        handle_income_submission(ack=lambda **kw: None, body=payload, client=slack_app.client, view=payload["view"])
                    elif callback_id == "expense_form":
                        handle_expense_submission(ack=lambda **kw: None, body=payload, client=slack_app.client, view=payload["view"])
                        
                threading.Thread(target=run_handler, daemon=True).start()
                
            elif payload_type == "block_actions":
                logger.info("BLOCK ACTIONS RECEIVED - SKIPPING")
                
    except Exception as e:
        logger.error(f"Error handling slack interactive event: {e}", exc_info=True)
        
    return Response(status_code=200)

@app.post("/slack/events")
async def slack_events(request: Request):
    body_bytes = await request.body()
    try:
        payload = json.loads(body_bytes)
    except Exception:
        payload = {}

    if payload.get("type") == "url_verification":
        return Response(content=payload.get("challenge"), media_type="text/plain", status_code=200)

    event_id = payload.get("event_id")

    if is_event_processed(event_id):
        return Response(status_code=200)

    mark_event_processed(event_id)

    event = payload.get("event", {})
    event_type = event.get("type")
    event_subtype = event.get("subtype")
    
    logger.info("SLACK EVENT RECEIVED")
    logger.info(f"EVENT TYPE: {event_type}")
    logger.info(f"EVENT SUBTYPE: {event_subtype}")

    is_file_event = (
        event_subtype == "file_share" or 
        event_type == "file_shared" or 
        (event.get("files") and len(event.get("files", [])) > 0)
    )

    if is_file_event:
        logger.info("FILE SHARE EVENT DETECTED")
        files = event.get("files") or []
        file_id = files[0].get("id") if len(files) > 0 else event.get("file_id") or event.get("file", {}).get("id")
        if file_id and is_event_processed(f"file_{file_id}"):
            logger.info(f"FILE EVENT ALREADY PROCESSED: {file_id}")
            return Response(status_code=200)
        if file_id:
            mark_event_processed(f"file_{file_id}")
            
        logger.info("BACKGROUND THREAD STARTED")
        import threading
        threading.Thread(
            target=process_slack_file_event,
            args=(event,),
            daemon=True
        ).start()
        return Response(status_code=200)

    import threading
    threading.Thread(
        target=handle_other_events,
        args=(payload,),
        daemon=True
    ).start()
    return Response(status_code=200)


# ==============================
# CREATE EXPENSE (API)
# ==============================
@app.post("/expense", summary="Create a new expense entry")
def create_expense(expense: ExpenseCreate, db: Session = Depends(get_db)):
    try:
        prop_data = expense.for_property if isinstance(expense.for_property, dict) else {"name": expense.for_property}
        
        purchase_date = datetime.now().date()
        if expense.purchase_date:
            try:
                purchase_date = datetime.strptime(expense.purchase_date, "%Y-%m-%d").date()
            except Exception:
                pass
            
        new_expense = Expense(
            expense_name=expense.expense_name,
            seller_name=expense.seller_name,
            total_amount=expense.total_amount,
            gst_amount=expense.gst_amount,
            purchase_date=purchase_date,
            paid_by=expense.paid_by,
            mode_of_payment=expense.mode_of_payment,
            priority=expense.priority,
            for_property=prop_data,
            submitted_by=expense.submitted_by,
            submitted_at=datetime.now()
        )

        db.add(new_expense)
        db.commit()
        db.refresh(new_expense)

        return {"status": "success", "id": new_expense.id}

    except Exception as e:
        return {"error": str(e)}


# ==============================
# CREATE INCOME (API)
# ==============================
@app.post("/income", summary="Create a new income entry")
def create_income(income: IncomeCreate, db: Session = Depends(get_db)):
    try:
        prop_data = income.for_property if isinstance(income.for_property, dict) else {"name": income.for_property}
        
        if income.captured_date:
            try:
                captured_date_obj = datetime.strptime(income.captured_date, "%Y-%m-%d").date()
            except Exception:
                captured_date_obj = datetime.now().date()
        else:
            captured_date_obj = datetime.now().date()
        
        new_income = Income(
            name=income.name,
            booking_number=income.booking_number,
            contact_number=income.contact_number,
            captured_date=captured_date_obj,
            room_amount=income.room_amount,
            food_amount=income.food_amount,
            payment_type=income.payment_type,
            receipt_by=income.receipt_by,
            for_property=prop_data,
            submitted_by=income.submitted_by,
            submitted_at=datetime.now()
        )
        
        db.add(new_income)
        db.commit()
        db.refresh(new_income)

        return {"status": "success", "id": new_income.id}

    except Exception as e:
        return {"error": str(e)}


# ==============================
# FILE UPLOAD (EXPENSE)
# ==============================
@app.post("/expense/upload-receipt", summary="Upload receipt image for an expense")
async def upload_receipt(transaction_id: str, file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not transaction_id:
        raise HTTPException(status_code=400, detail="transaction_id is required")

    data = await file.read()
    expense = db.query(Expense).filter(Expense.transaction_id == transaction_id).first()
    if not expense:
        raise HTTPException(status_code=404, detail="Expense not found")
        
    print("FLOW: REST")
    print("transaction_id:", transaction_id)
    print("resolved record_id:", expense.id)
    
    existing_screenshots = expense.receipt_copies or []
    file_index = len(existing_screenshots) + 1
    
    try:
        result = process_upload(
            record_id=expense.id,
            transaction_id=transaction_id,
            file_bytes=data,
            mime_type=file.content_type,
            file_index=file_index,
            record_type="expense",
            submitted_by_id="API_USER",
            submitted_by_name="API Upload"
        )
        print("file_url:", result.get("file_url"))
        
        # Verify db update
        db.refresh(expense)
        if not expense.receipt_copy:
            raise Exception("Upload failed: receipt_copy is still NULL after update")
            
    except Exception as e:
        logger.error(f"process_upload failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
        
    return {"status": "success", "message": "Receipt uploaded", "data": result}


# ==============================
# GET RECEIPT
# ==============================
@app.get("/expense/{id}/receipt", summary="Get receipt image for an expense")
def get_receipt(id: int, db: Session = Depends(get_db)):
    expense = db.query(Expense).filter(Expense.id == id).first()
    if not expense or not expense.receipt_copy:
        raise HTTPException(status_code=404, detail="Receipt not found")

    return Response(
        content=expense.receipt_copy,
        media_type="image/jpeg"
    )


# ==============================
# UPLOAD INCOME SCREENSHOT
# ==============================
@app.post("/income/upload-screenshot", summary="Upload screenshot image for an income")
async def upload_income_screenshot(transaction_id: str, file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not transaction_id:
        raise HTTPException(status_code=400, detail="transaction_id is required")

    data = await file.read()
    income = db.query(Income).filter(Income.transaction_id == transaction_id).first()
    if not income:
        raise HTTPException(status_code=404, detail="Income not found")
        
    print("FLOW: REST")
    print("transaction_id:", transaction_id)
    print("resolved record_id:", income.id)
    
    existing_screenshots = income.payment_screenshots or []
    file_index = len(existing_screenshots) + 1
    
    try:
        result = process_upload(
            record_id=income.id,
            transaction_id=transaction_id,
            file_bytes=data,
            mime_type=file.content_type,
            file_index=file_index,
            record_type="income",
            submitted_by_id="API_USER",
            submitted_by_name="API Upload"
        )
        print("file_url:", result.get("file_url"))
        
        # Verify db update
        db.refresh(income)
        if not income.payment_screenshot:
            raise Exception("Upload failed: payment_screenshot is still NULL after update")

    except Exception as e:
        logger.error(f"process_upload failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
        
    return {"status": "success", "message": "Screenshot uploaded", "data": result}


# ==============================
# GET INCOME SCREENSHOT
# ==============================
@app.get("/income/{id}/screenshot", summary="Get screenshot image for an income")
def get_income_screenshot(id: int, db: Session = Depends(get_db)):
    income = db.query(Income).filter(Income.id == id).first()
    if not income or not income.payment_screenshot:
        raise HTTPException(status_code=404, detail="Screenshot not found")

    return Response(
        content=income.payment_screenshot,
        media_type="image/jpeg"
    )

# ==============================
# SUBMIT INCOME FORM
# ==============================
@app.post("/submit-income", summary="Submit an income form with screenshot")
async def submit_income_endpoint(
    name: str = Form(...),
    booking_number: int = Form(...),
    contact_number: str = Form(...),
    captured_date: str = Form(...),
    room_amount: int = Form(...),
    food_amount: int = Form(...),
    payment_type: str = Form(...),
    receipt_by: str = Form(...),
    payment_screenshot: UploadFile = File(...)
):
    # Log incoming request data safely
    logger.info("Received POST /submit-income request")
    logger.info(f"Data: name={name}, booking_number={booking_number}, contact_number={contact_number}, "
                f"captured_date={captured_date}, room_amount={room_amount}, food_amount={food_amount}, "
                f"payment_type={payment_type}, receipt_by={receipt_by}")
                
    try:
        # File parsing
        file_bytes = await process_income_file(payment_screenshot)
        
        # Date parsing
        try:
            captured_date_obj = datetime.strptime(captured_date, "%Y-%m-%d").date()
        except Exception:
            raise ValueError("captured_date must be in YYYY-MM-DD format")

        # Validation
        validate_income_data(
            name=name,
            booking_number=booking_number,
            contact_number=contact_number,
            room_amount=room_amount,
            food_amount=food_amount,
            payment_type=payment_type,
            receipt_by=receipt_by
        )
        
        # DB Logic
        data = {
            "name": name,
            "booking_number": booking_number,
            "contact_number": contact_number,
            "captured_date": captured_date_obj,
            "room_amount": room_amount,
            "food_amount": food_amount,
            "payment_type": payment_type,
            "receipt_by": receipt_by
        }
        
        record_id, transaction_id = insert_income_form_record(data)

        # File upload through the pipeline
        try:
            mime_type = payment_screenshot.content_type or "image/jpeg"
            process_upload(
                record_id=record_id,
                transaction_id=transaction_id,
                file_bytes=file_bytes,
                mime_type=mime_type,
                file_index=1,
                record_type="income",
                submitted_by_id="API_USER",
                submitted_by_name="API Upload"
            )
        except Exception as e:
            logger.error(f"process_upload failed: {e}")
            return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})

        return {
            "status": "success",
            "message": "Income recorded successfully",
            "id": record_id
        }
    except ValueError as ve:
        return JSONResponse(status_code=400, content={"status": "error", "message": str(ve)})
    except Exception as e:
        logger.error(f"Error in /submit-income: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "message": "System error occurred"})

# ==============================
# SUBMIT TRANSACTION
# ==============================
@app.post("/create-transaction", summary="Create a new transaction")
def create_transaction_endpoint(txn: TransactionCreate, db: Session = Depends(get_db)):
    try:
        new_transaction = Transaction(
            transaction_id=f"TXN-{uuid.uuid4().hex[:8]}",
            name=txn.name,
            email=txn.email,
            amount=txn.amount
        )
        db.add(new_transaction)
        db.commit()
        db.refresh(new_transaction)
        return {"status": "success", "transaction_id": new_transaction.transaction_id}
    except Exception as e:
        logger.error(f"Error in /create-transaction: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})

# ==============================
# UPLOAD TRANSACTION SCREENSHOT
# ==============================
from supabase import create_client, Client  # kept for type hints; client is managed in supabase_storage

@app.post("/upload-screenshot", summary="Upload screenshot and link to transaction")
async def upload_transaction_screenshot(
    transaction_id: str = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    txn = db.query(Transaction).filter(Transaction.transaction_id == transaction_id).first()
    if not txn:
        raise HTTPException(status_code=404, detail="Transaction not found")

    try:
        file_bytes = await file.read()
        mime_type = file.content_type or "image/jpeg"

        # Reuse the shared Supabase client from supabase_storage (uses SUPABASE_SERVICE_ROLE_KEY)
        public_url = upload_file_to_storage(
            transaction_id=transaction_id,
            file_bytes=file_bytes,
            mime_type=mime_type,
        )

        txn.screenshot_url = public_url
        db.commit()

        return {"status": "success", "transaction_id": transaction_id, "screenshot_url": public_url}
    except Exception as e:
        logger.error(f"Error uploading screenshot: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})