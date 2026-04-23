import os
import threading
import re
import uuid
from datetime import datetime
from typing import Any, Optional

from fastapi import FastAPI, Request, UploadFile, File, Depends, HTTPException, Form, BackgroundTasks
import requests
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel
from sqlalchemy.orm import Session
from dotenv import load_dotenv

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
    complete_income_with_screenshot,
    insert_expense_record,
    get_pending_expense,
    complete_expense_with_receipt,
)
from services.validation import validate_income_data
from services.file_handler import process_income_file
from services.supabase_storage import upload_file_to_storage
from services.slack_downloader import download_slack_file

# Load Environment Variables
load_dotenv()

# ==============================
# FILE VALIDATION CONSTANTS
# ==============================
MAX_FILE_SIZE = 5 * 1024 * 1024  # 5 MB
ALLOWED_TYPES = ["image/jpeg", "image/png", "application/pdf"]

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Finance API")

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
    token=os.getenv("SLACK_BOT_TOKEN"),
    signing_secret=os.getenv("SLACK_SIGNING_SECRET")
)

handler = SlackRequestHandler(slack_app)

@slack_app.event("message")
def handle_message_events(body, logger, client):
    event = body.get("event", {})
    text = event.get("text", "")
    channel = event.get("channel")
    subtype = event.get("subtype")
    user_id = event.get("user", "UNKNOWN")

    # ── Ignore all bot messages ────────────────────────────────────────────
    if subtype == "bot_message":
        return

    # ══════════════════════════════════════════════════════════════════════
    # FILE UPLOAD  →  attach file to latest PENDING income OR expense record
    # Priority: income (TXN-) checked first, then expense (EXP-).
    # All state lives in Supabase — zero in-memory dicts.
    # ══════════════════════════════════════════════════════════════════════
    if subtype == "file_share":
        if "files" not in event:
            return

        file = event["files"][0]
        mime_type = file.get("mimetype", "")

        # ── Step 1: check for pending INCOME first ──────────────────────
        pending_income = None
        pending_expense = None
        income_db = expense_db = None

        try:
            pending_income, income_db = get_pending_income(user_id)
        except Exception as e:
            logger.error(f"DB error fetching pending income for {user_id}: {e}")

        # ── Step 2: if no pending income, check for pending EXPENSE ────────
        if not pending_income:
            if income_db:
                income_db.close()
            try:
                pending_expense, expense_db = get_pending_expense(user_id)
            except Exception as e:
                logger.error(f"DB error fetching pending expense for {user_id}: {e}")

        if not pending_income and not pending_expense:
            if expense_db:
                expense_db.close()
            client.chat_postMessage(
                channel=user_id,
                text="❌ No pending income or expense submission found. Please submit a form first."
            )
            return

        # Capture transaction_id and close session before HTTP call
        if pending_income:
            transaction_id = pending_income.transaction_id
            record_type = "income"
            income_db.close()
        else:
            transaction_id = pending_expense.transaction_id
            record_type = "expense"
            expense_db.close()

        # ── Step 3: Download file from Slack ─────────────────────────────
        # download_slack_file() handles: auth header, retries, HTML-response
        # detection, integrity check, and descriptive RuntimeError messages.
        try:
            file_bytes = download_slack_file(file)
        except RuntimeError as e:
            logger.error(f"Slack file download failed | txn={transaction_id}: {e}")
            client.chat_postMessage(
                channel=user_id,
                text=f"❌ Failed to download the file from Slack.\n`{e}`"
            )
            return

        # ── Validate: file size (≤5 MB) ────────────────────────────────
        if len(file_bytes) > MAX_FILE_SIZE:
            client.chat_postMessage(
                channel=user_id,
                text="❌ File too large. Please upload a file under 5 MB."
            )
            return

        # ── Validate: MIME type ────────────────────────────────────────
        if mime_type not in ALLOWED_TYPES:
            client.chat_postMessage(
                channel=user_id,
                text="❌ Invalid file type. Only JPG, PNG, PDF allowed."
            )
            return

        # ── Step 4: Upload to Supabase Storage → get public URL ───────────
        try:
            file_url = upload_file_to_storage(
                transaction_id=transaction_id,
                file_bytes=file_bytes,
                mime_type=mime_type,
            )
        except Exception as e:
            logger.error(f"Supabase Storage upload failed | txn={transaction_id}: {e}")
            client.chat_postMessage(
                channel=user_id,
                text=f"❌ File upload to storage failed: {str(e)}"
            )
            return

        # ── Step 5: Save URL in DB & mark COMPLETED ──────────────────────
        try:
            if record_type == "income":
                success = complete_income_with_screenshot(transaction_id, file_url)
                entity = "Income"
            else:
                success = complete_expense_with_receipt(transaction_id, file_url)
                entity = "Expense"
        except Exception as e:
            logger.error(f"DB error completing {record_type} txn={transaction_id}: {e}")
            client.chat_postMessage(
                channel=user_id,
                text=f"❌ Database error while saving file reference: {str(e)}"
            )
            return

        if success:
            if record_type == "income":
                client.chat_postMessage(
                    channel=user_id,
                    text=(
                        f"✅ Payment screenshot received and linked to transaction `{transaction_id}`.\n"
                        f"Income record is now *COMPLETED*."
                    )
                )
            else:
                client.chat_postMessage(
                    channel=user_id,
                    text=(
                        f"✅ Receipt uploaded successfully. Expense completed.\n"
                        f"Transaction ID: `{transaction_id}`"
                    )
                )
            logger.info(f"{entity} file attached | txn={transaction_id} | user={user_id}")
        else:
            client.chat_postMessage(
                channel=user_id,
                text="❌ No matching PENDING record found. It may have already been completed."
            )
        return

    # ══════════════════════════════════════════════════════════════════════
    # AUTO RAZORPAY DETECTION
    # ══════════════════════════════════════════════════════════════════════
    if subtype is None and text and "You received a new payment" in text:
        logger.info("Razorpay Payment Message Detected")

        try:
            # 1. Parse message
            parsed_data = parse_razorpay_message(text)

            logger.info("--- EXTRACTED DATA ---")
            logger.info(f"Raw Message: {text}")
            logger.info(f"Amount: {parsed_data.get('amount')}")
            logger.info(f"Contact: {parsed_data.get('contact')}")
            logger.info(f"Date: {parsed_data.get('captured_date')}")
            logger.info("----------------------")

            # 2. Insert into database (auto-assigns transaction_id, status=COMPLETED)
            amount = insert_razorpay_income(parsed_data, user_id)

            # 3. Send confirmation
            if channel:
                client.chat_postMessage(
                    channel=channel,
                    text=f"✅ Auto-recorded Razorpay payment: ₹{amount}"
                )

            logger.info(f"Razorpay income processed | amount=₹{amount} | user={user_id}")

        except Exception as e:
            logger.error(f"Failed to process Razorpay message: {e}")

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
# SLACK COMMAND: /expense
# ==============================
@slack_app.command("/expense")
def open_expense(ack, body, client, logger):
    ack()
    try:
        client.views_open(
        trigger_id=body["trigger_id"],
        view={
            "type": "modal",
            "callback_id": "expense_modal",
            "title": {"type": "plain_text", "text": "Submit Expense"},
            "submit": {"type": "plain_text", "text": "Submit"},
            "blocks": [
                {
                    "type": "input",
                    "block_id": "expense_name_block",
                    "element": {"type": "plain_text_input", "action_id": "expense_name"},
                    "label": {"type": "plain_text", "text": "Expense Name"}
                },
                {
                    "type": "input",
                    "block_id": "seller_name_block",
                    "element": {"type": "plain_text_input", "action_id": "seller_name"},
                    "label": {"type": "plain_text", "text": "Seller Name"}
                },
                {
                    "type": "input",
                    "block_id": "gst_amount_block",
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "gst_amount",
                        "initial_value": "0",
                        "placeholder": {"type": "plain_text", "text": "e.g. 150"}
                    },
                    "label": {"type": "plain_text", "text": "GST Amount"}
                },
                {
                    "type": "input",
                    "block_id": "total_amount_block",
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "total_amount",
                        "placeholder": {"type": "plain_text", "text": "e.g. 1500"}
                    },
                    "label": {"type": "plain_text", "text": "Total Amount"}
                },
                {
                    "type": "input",
                    "block_id": "purchase_date_block",
                    "element": {
                        "type": "datepicker",
                        "action_id": "purchase_date_input",
                        "placeholder": {"type": "plain_text", "text": "Select purchase date"}
                    },
                    "label": {"type": "plain_text", "text": "Purchase Date"}
                },
                {
                    "type": "input",
                    "block_id": "priority_block",
                    "element": {
                        "type": "static_select",
                        "action_id": "priority",
                        "placeholder": {"type": "plain_text", "text": "Select priority"},
                        "options": [
                            {"text": {"type": "plain_text", "text": "Critical \u26a0\ufe0f"}, "value": "Critical"},
                            {"text": {"type": "plain_text", "text": "High"},            "value": "High"},
                            {"text": {"type": "plain_text", "text": "Medium"},          "value": "Medium"},
                            {"text": {"type": "plain_text", "text": "Low"},             "value": "Low"}
                        ]
                    },
                    "label": {"type": "plain_text", "text": "Priority"}
                },
                {
                    "type": "input",
                    "block_id": "paid_by_block",
                    "element": {"type": "plain_text_input", "action_id": "paid_by"},
                    "label": {"type": "plain_text", "text": "Paid By"}
                },
                {
                    "type": "input",
                    "block_id": "mode_of_payment_block",
                    "element": {
                        "type": "static_select",
                        "action_id": "mode_of_payment",
                        "placeholder": {"type": "plain_text", "text": "Select payment mode"},
                        "options": [
                            {"text": {"type": "plain_text", "text": "Cash"},        "value": "Cash"},
                            {"text": {"type": "plain_text", "text": "Bank"},        "value": "Bank"},
                            {"text": {"type": "plain_text", "text": "UPI"},         "value": "UPI"},
                            {"text": {"type": "plain_text", "text": "Credit Card"}, "value": "Credit Card"},
                            {"text": {"type": "plain_text", "text": "Petty Cash"},  "value": "Petty Cash"},
                            {"text": {"type": "plain_text", "text": "Company"},     "value": "Company"}
                        ]
                    },
                    "label": {"type": "plain_text", "text": "Mode Of Payment"}
                },
                {
                    "type": "input",
                    "block_id": "property_block",
                    "element": {
                        "type": "radio_buttons",
                        "action_id": "property",
                        "options": PROPERTIES
                    },
                    "label": {"type": "plain_text", "text": "Property"}
                }
            ]
        }
    )
    except Exception as e:
        logger.error(f"Failed to open /expense modal: {str(e)}")


# ==============================
# SLACK COMMAND: /income
# ==============================
@slack_app.command("/income")
def open_income(ack, body, client, logger):
    ack()
    try:
        client.views_open(
        trigger_id=body["trigger_id"],
        view={
            "type": "modal",
            "callback_id": "income_modal",
            "title": {"type": "plain_text", "text": "Submit Income"},
            "submit": {"type": "plain_text", "text": "Submit"},
            "blocks": [
                {
                    "type": "input",
                    "block_id": "for_property",
                    "element": {
                        "type": "radio_buttons",
                        "action_id": "value",
                        "options": PROPERTIES
                    },
                    "label": {"type": "plain_text", "text": "Select Property"}
                },
                {
                    "type": "input",
                    "block_id": "name_block",
                    "element": {"type": "plain_text_input", "action_id": "name"},
                    "label": {"type": "plain_text", "text": "Customer Name"}
                },
                {
                    "type": "input",
                    "block_id": "booking_block",
                    "element": {"type": "plain_text_input", "action_id": "booking"},
                    "label": {"type": "plain_text", "text": "Booking Number"}
                },
                {
                    "type": "input",
                    "block_id": "payment_type",
                    "element": {
                        "type": "static_select",
                        "action_id": "value",
                        "placeholder": {"type": "plain_text", "text": "Select payment method"},
                        "options": [
                            {"text": {"type": "plain_text", "text": "Cash"}, "value": "Cash"},
                            {"text": {"type": "plain_text", "text": "Bank Transfer"}, "value": "Bank Transfer"},
                            {"text": {"type": "plain_text", "text": "UPI"}, "value": "UPI"},
                            {"text": {"type": "plain_text", "text": "Credit Card"}, "value": "Credit Card"},
                            {"text": {"type": "plain_text", "text": "ICICI Bank POS"}, "value": "ICICI Bank POS"},
                            {"text": {"type": "plain_text", "text": "HDFC Bank POS"}, "value": "HDFC Bank POS"},
                            {"text": {"type": "plain_text", "text": "QR Code Standy (ICICI)"}, "value": "QR Code Standy (ICICI)"}
                        ]
                    },
                    "label": {"type": "plain_text", "text": "Payment Type"}
                },
                {
                    "type": "input",
                    "block_id": "room_block",
                    "element": {"type": "plain_text_input", "action_id": "room"},
                    "label": {"type": "plain_text", "text": "Room Amount"}
                },
                {
                    "type": "input",
                    "block_id": "food_block",
                    "element": {"type": "plain_text_input", "action_id": "food", "initial_value": "0"},
                    "label": {"type": "plain_text", "text": "Food Amount"}
                },
                {
                    "type": "input",
                    "block_id": "receipt_block",
                    "element": {"type": "plain_text_input", "action_id": "receipt"},
                    "label": {"type": "plain_text", "text": "Receipt By"}
                },
                {
                    "type": "input",
                    "block_id": "receipt_date",
                    "optional": True,
                    "element": {
                        "type": "datepicker",
                        "action_id": "receipt_date_input",
                        "placeholder": {"type": "plain_text", "text": "Select receipt date"}
                    },
                    "label": {"type": "plain_text", "text": "Receipt Date"}
                }
            ]
        }
    )
    except Exception as e:
        logger.error(f"Failed to open /income modal: {str(e)}")


# ==============================
# SUBMIT HANDLER: EXPENSE
# ==============================
@slack_app.view("expense_modal")
def handle_expense_modal(ack, body, client, logger):
    ack()

    def _process_expense():
        logger.info("THREAD STARTED")
        try:
            user_id      = body.get("user", {}).get("id")
            state_values = body.get("view", {}).get("state", {}).get("values", {})
            # Extract
            expense_name     = state_values["expense_name_block"]["expense_name"]["value"]
            seller_name      = state_values["seller_name_block"]["seller_name"]["value"]
            gst_amount_str   = state_values["gst_amount_block"]["gst_amount"]["value"]
            total_amount_str = state_values["total_amount_block"]["total_amount"]["value"]
            purchase_date_str = state_values["purchase_date_block"]["purchase_date_input"]["selected_date"]
            priority         = state_values["priority_block"]["priority"]["selected_option"]["value"]
            paid_by          = state_values["paid_by_block"]["paid_by"]["value"]
            mode_of_payment  = state_values["mode_of_payment_block"]["mode_of_payment"]["selected_option"]["value"]
            property_name    = state_values["property_block"]["property"]["selected_option"]["value"]

            # Validate
            num_regex = re.compile(r"^\d+(\.\d+)?$")
            errs = []
            if not expense_name or not expense_name.strip():
                errs.append("Expense name is required.")
            if not seller_name or not seller_name.strip():
                errs.append("Seller name is required.")
            if not gst_amount_str or not num_regex.match(gst_amount_str.strip()):
                errs.append("GST Amount must be a valid number (e.g. 0 or 150.50).")
            if not total_amount_str or not num_regex.match(total_amount_str.strip()):
                errs.append("Total Amount must be a valid number (e.g. 1500).")
            if not paid_by or not paid_by.strip():
                errs.append("Paid By is required.")
            if not purchase_date_str:
                errs.append("Purchase date is required.")
            if errs:
                client.chat_postMessage(
                    channel=user_id,
                    text="\u274c Expense submission failed:\n" + "\n".join(f"\u2022 {e}" for e in errs)
                )
                return

            # Parse date
            try:
                purchase_date_obj = datetime.strptime(purchase_date_str, "%Y-%m-%d").date()
            except Exception:
                purchase_date_obj = datetime.now().date()

            data = {
                "user_id":         user_id,
                "expense_name":    expense_name.strip(),
                "seller_name":     seller_name.strip(),
                "gst_amount":      float(gst_amount_str.strip()),
                "total_amount":    float(total_amount_str.strip()),
                "purchase_date":   purchase_date_obj,
                "priority":        priority,
                "paid_by":         paid_by.strip(),
                "mode_of_payment": mode_of_payment,
                "property_name":   property_name,
            }

            transaction_id = insert_expense_record(data)
            logger.info(f"PENDING expense created | txn={transaction_id} | user={user_id}")

            client.chat_postMessage(
                channel=user_id,
                text=(
                    f"\u2705 Expense submitted successfully.\n"
                    f"Transaction ID: `{transaction_id}`\n"
                    "\U0001f4ce Please upload the *receipt screenshot* to complete the submission."
                )
            )
            logger.info("THREAD COMPLETED")  # trace
        except Exception as e:
            logger.error(f"Thread crash: {str(e)}")
            try:
                user_id = body.get("user", {}).get("id")
                if user_id:
                    client.chat_postMessage(
                        channel=user_id,
                        text=f"\u274c An error occurred while saving your expense: {str(e)}"
                    )
            except Exception:
                pass

    threading.Thread(target=_process_expense, daemon=True).start()



# ==============================
# SUBMIT HANDLER: INCOME
# ==============================
@slack_app.view("income_modal")
def handle_income_modal(ack, body, client, logger):
    ack()

    def _process_income():
        logger.info("THREAD STARTED")
        try:
            user_id      = body.get("user", {}).get("id")
            state_values = body.get("view", {}).get("state", {}).get("values", {})
            # Extract
            name            = state_values["name_block"]["name"]["value"]
            booking_number  = state_values["booking_block"]["booking"]["value"]
            room_amount_str = state_values["room_block"]["room"]["value"]
            food_amount_str = state_values["food_block"]["food"]["value"]
            receipt_by      = state_values["receipt_block"]["receipt"]["value"]
            receipt_date    = (
                state_values
                .get("receipt_date", {})
                .get("receipt_date_input", {})
                .get("selected_date")
            )
            contact_number = (
                state_values.get("contact_number", {})
                .get("value", {}).get("value", "")
            )
            captured_date_str = (
                state_values.get("captured_date", {})
                .get("value", {}).get("selected_date", "")
            )
            payment_type = (
                state_values.get("payment_type", {})
                .get("value", {}).get("selected_option", {}).get("value", "")
            )
            property_name = (
                state_values.get("for_property", {})
                .get("value", {}).get("selected_option", {}).get("value", "")
            )

            # Validate
            alpha_regex = re.compile(r"^[A-Za-z\s]+$")
            errs = []
            if not name or not alpha_regex.match(name):
                errs.append("Customer Name: only alphabets allowed.")
            if not booking_number or not booking_number.isdigit():
                errs.append("Booking Number: only numbers allowed.")
            if not room_amount_str or not room_amount_str.isdigit() or int(room_amount_str) <= 0:
                errs.append("Room Amount: must be a positive number.")
            if not food_amount_str or not food_amount_str.isdigit() or int(food_amount_str) < 0:
                errs.append("Food Amount: must be a non-negative number.")
            if not receipt_by or not alpha_regex.match(receipt_by):
                errs.append("Receipt By: only alphabets allowed.")
            if errs:
                client.chat_postMessage(
                    channel=user_id,
                    text="\u274c Income submission failed:\n" + "\n".join(f"\u2022 {e}" for e in errs)
                )
                return

            # Generate transaction_id
            transaction_id = f"TXN-{uuid.uuid4().hex[:8]}"

            # Parse dates
            captured_dt = datetime.now().date()
            if captured_date_str:
                try:
                    captured_dt = datetime.strptime(captured_date_str, "%Y-%m-%d").date()
                except Exception:
                    pass

            receipt_dt = None
            if receipt_date:
                try:
                    receipt_dt = datetime.strptime(receipt_date, "%Y-%m-%d").date()
                except Exception:
                    pass

            db = SessionLocal()
            try:
                new_income = Income(
                    transaction_id     = transaction_id,
                    user_id            = user_id,
                    status             = "PENDING",
                    name               = name,
                    booking_number     = booking_number,
                    contact_number     = contact_number,
                    captured_date      = captured_dt,
                    room_amount        = float(room_amount_str),
                    food_amount        = float(food_amount_str),
                    receipt_by         = receipt_by,
                    payment_type       = payment_type,
                    for_property       = {"name": property_name},
                    receipt_date       = receipt_dt,
                    payment_screenshot = None,
                    submitted_by       = user_id,
                    submitted_at       = datetime.utcnow(),
                )
                db.add(new_income)
                db.commit()
                logger.info(
                    f"PENDING income created | txn={transaction_id} | "
                    f"user={user_id} | property={property_name}"
                )
                client.chat_postMessage(
                    channel=user_id,
                    text=(
                        f"\u2705 Income form received (Transaction ID: `{transaction_id}`)\n"
                        "\U0001f4ce Please upload the *payment screenshot* to complete the submission."
                    )
                )
            except Exception as e:
                db.rollback()
                logger.error(f"DB error creating PENDING income: {e}")
                try:
                    client.chat_postMessage(
                        channel=user_id,
                        text=f"\u274c Database error while saving your submission: {str(e)}"
                    )
                except Exception:
                    pass
            finally:
                db.close()

            logger.info("THREAD COMPLETED")  # trace

        except Exception as e:
            logger.error(f"Thread crash: {str(e)}")
            try:
                user_id = body.get("user", {}).get("id")
                if user_id:
                    client.chat_postMessage(
                        channel=user_id,
                        text=f"\u274c An error occurred while processing your income form: {str(e)}"
                    )
            except Exception:
                pass

    threading.Thread(target=_process_income, daemon=True).start()



# ==============================
# SLACK EVENTS ENDPOINT
# ==============================
@app.post("/slack/events", summary="Slack Events webhook endpoint")
async def slack_events(request: Request):
    logger.info("SLACK EVENT HIT")
    return await handler.handle(request)



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
@app.post("/expense/{id}/upload-receipt", summary="Upload receipt image for an expense")
async def upload_receipt(id: int, file: UploadFile = File(...), db: Session = Depends(get_db)):
    data = await file.read()
    expense = db.query(Expense).filter(Expense.id == id).first()
    if not expense:
        raise HTTPException(status_code=404, detail="Expense not found")
        
    expense.receipt_copy = data
    db.commit()
    return {"status": "success", "message": "Receipt uploaded"}


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
@app.post("/income/{id}/upload-screenshot", summary="Upload screenshot image for an income")
async def upload_income_screenshot(id: int, file: UploadFile = File(...), db: Session = Depends(get_db)):
    data = await file.read()
    income = db.query(Income).filter(Income.id == id).first()
    if not income:
        raise HTTPException(status_code=404, detail="Income not found")
        
    income.payment_screenshot = data
    db.commit()
    return {"status": "success", "message": "Screenshot uploaded"}


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
        
        record_id = insert_income_form_record(data, file_bytes)

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