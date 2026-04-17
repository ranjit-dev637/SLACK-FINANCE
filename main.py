import os
import threading
from datetime import datetime
from typing import Any, Optional

from fastapi import FastAPI, Request, UploadFile, File, Depends, HTTPException
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

# Load Environment Variables
load_dotenv()

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
def handle_message_events(body, logger):
    logger.info("Message Event Received")
    logger.info(body)

# ==============================
# PROPERTY LIST
# ==============================
PROPERTIES = [
    {"text": {"type": "plain_text", "text": "Clover Villa"}, "value": "Clover Villa"},
    {"text": {"type": "plain_text", "text": "Clover Woods"}, "value": "Clover Woods"},
    {"text": {"type": "plain_text", "text": "Clover Connect"}, "value": "Clover Connect"},
    {"text": {"type": "plain_text", "text": "Clovera"}, "value": "Clovera"},
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


# ==============================
# SLACK COMMAND: /expense
# ==============================
@slack_app.command("/expense")
def open_expense(ack, body, client):
    ack()
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
                    "block_id": "expense_name",
                    "element": {"type": "plain_text_input", "action_id": "value"},
                    "label": {"type": "plain_text", "text": "Expense Name"}
                },
                {
                    "type": "input",
                    "block_id": "seller_name",
                    "element": {"type": "plain_text_input", "action_id": "value"},
                    "label": {"type": "plain_text", "text": "Seller Name"}
                },
                {
                    "type": "input",
                    "block_id": "total_amount",
                    "element": {"type": "plain_text_input", "action_id": "value"},
                    "label": {"type": "plain_text", "text": "Total Amount"}
                },
                {
                    "type": "input",
                    "block_id": "gst_amount",
                    "element": {"type": "plain_text_input", "action_id": "value", "initial_value": "0"},
                    "label": {"type": "plain_text", "text": "GST Amount"}
                },
                {
                    "type": "input",
                    "block_id": "purchase_date",
                    "element": {"type": "datepicker", "action_id": "value"},
                    "label": {"type": "plain_text", "text": "Purchase Date"}
                },
                {
                    "type": "input",
                    "block_id": "paid_by",
                    "element": {"type": "plain_text_input", "action_id": "value"},
                    "label": {"type": "plain_text", "text": "Paid By"}
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
                            {"text": {"type": "plain_text", "text": "UPI"}, "value": "UPI"},
                            {"text": {"type": "plain_text", "text": "Card"}, "value": "Card"}
                        ]
                    },
                    "label": {"type": "plain_text", "text": "Payment Type"}
                },
                {
                    "type": "input",
                    "block_id": "priority",
                    "element": {
                        "type": "static_select",
                        "action_id": "value",
                        "options": [
                            {"text": {"type": "plain_text", "text": "High"}, "value": "High"},
                            {"text": {"type": "plain_text", "text": "Medium"}, "value": "Medium"},
                            {"text": {"type": "plain_text", "text": "Low"}, "value": "Low"}
                        ]
                    },
                    "label": {"type": "plain_text", "text": "Priority"}
                },
                {
                    "type": "input",
                    "block_id": "for_property",
                    "element": {
                        "type": "static_select",
                        "action_id": "value",
                        "options": PROPERTIES
                    },
                    "label": {"type": "plain_text", "text": "Select Property"}
                }
            ]
        }
    )


# ==============================
# SLACK COMMAND: /income
# ==============================
@slack_app.command("/income")
def open_income(ack, body, client):
    ack()
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
                    "block_id": "name",
                    "element": {"type": "plain_text_input", "action_id": "value"},
                    "label": {"type": "plain_text", "text": "Customer Name"}
                },
                {
                    "type": "input",
                    "block_id": "booking_number",
                    "element": {"type": "plain_text_input", "action_id": "value"},
                    "label": {"type": "plain_text", "text": "Booking Number"}
                },
                {
                    "type": "input",
                    "block_id": "contact_number",
                    "element": {"type": "plain_text_input", "action_id": "value"},
                    "label": {"type": "plain_text", "text": "Contact Number"}
                },
                {
                    "type": "input",
                    "block_id": "captured_date",
                    "optional": True,
                    "element": {"type": "datepicker", "action_id": "value"},
                    "label": {"type": "plain_text", "text": "Captured Date"}
                },
                {
                    "type": "input",
                    "block_id": "room_amount",
                    "element": {"type": "plain_text_input", "action_id": "value"},
                    "label": {"type": "plain_text", "text": "Room Amount"}
                },
                {
                    "type": "input",
                    "block_id": "food_amount",
                    "element": {"type": "plain_text_input", "action_id": "value", "initial_value": "0"},
                    "label": {"type": "plain_text", "text": "Food Amount"}
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
                            {"text": {"type": "plain_text", "text": "UPI"}, "value": "UPI"},
                            {"text": {"type": "plain_text", "text": "Card"}, "value": "Card"}
                        ]
                    },
                    "label": {"type": "plain_text", "text": "Payment Type"}
                },
                {
                    "type": "input",
                    "block_id": "receipt_by",
                    "element": {"type": "plain_text_input", "action_id": "value"},
                    "label": {"type": "plain_text", "text": "Receipt By"}
                },
                {
                    "type": "input",
                    "block_id": "for_property",
                    "element": {
                        "type": "static_select",
                        "action_id": "value",
                        "options": PROPERTIES
                    },
                    "label": {"type": "plain_text", "text": "Select Property"}
                }
            ]
        }
    )


# ==============================
# SUBMIT HANDLER: EXPENSE
# ==============================
@slack_app.view("expense_modal")
def handle_expense_modal(ack, body, client):
    # Immediate ACK to prevent Slack app timeout
    ack()
    
    def background_task():
        values = body["view"]["state"]["values"]
        user_id = body["user"]["id"]
        user_name = body["user"]["username"]
        
        try:
            expense_name = values["expense_name"]["value"]["value"]
            seller_name = values["seller_name"]["value"]["value"]
            total_amount = safe_float(values["total_amount"]["value"]["value"])
            gst_amount = safe_float(values["gst_amount"]["value"]["value"])
            
            purchase_date_str = values["purchase_date"]["value"]["selected_date"]
            purchase_date = datetime.strptime(purchase_date_str, "%Y-%m-%d").date()
            
            paid_by = values["paid_by"]["value"]["value"]
            mode_of_payment = values["payment_type"]["value"]["selected_option"]["value"]
            priority = values["priority"]["value"]["selected_option"]["value"]
            property_name = values["for_property"]["value"]["selected_option"]["value"]
            
            db = SessionLocal()
            try:
                new_expense = models.Expense(
                    expense_name=expense_name,
                    seller_name=seller_name,
                    total_amount=total_amount,
                    gst_amount=gst_amount,
                    purchase_date=purchase_date,
                    paid_by=paid_by,
                    mode_of_payment=mode_of_payment,
                    priority=priority,
                    for_property={"name": property_name},
                    submitted_by=user_name,
                    submitted_at=datetime.now()
                )
                db.add(new_expense)
                db.commit()
                
                try:
                    client.chat_postMessage(
                        channel=user_id,
                        text=f"✅ Expense recorded: ₹{total_amount} for {property_name}"
                    )
                except Exception:
                    pass
            except Exception as e:
                try:
                    client.chat_postMessage(
                        channel=user_id,
                        text=f"❌ Failed to save expense in database. Error: {str(e)}"
                    )
                except Exception:
                    pass
            finally:
                db.close()
                
        except Exception as e:
            try:
                client.chat_postMessage(channel=user_id, text=f"❌ Error processing modal fields: {str(e)}")
            except Exception:
                pass

    threading.Thread(target=background_task).start()


# ==============================
# SUBMIT HANDLER: INCOME
# ==============================
@slack_app.view("income_modal")
def handle_income_modal(ack, body, client):
    # Immediate ACK to prevent Slack app timeout
    ack()
    
    def background_task():
        values = body["view"]["state"]["values"]
        user_id = body["user"]["id"]
        user_name = body["user"]["username"]
        
        try:
            name = values["name"]["value"]["value"]
            booking_number = values["booking_number"]["value"]["value"]
            contact_number = values["contact_number"]["value"]["value"]
            
            captured_date = values["captured_date"]["value"]["selected_date"]
            if not captured_date:
                captured_date = datetime.now().strftime("%Y-%m-%d")
            
            room_amount = safe_float(values["room_amount"]["value"]["value"])
            food_amount = safe_float(values["food_amount"]["value"]["value"])
            payment_type = values["payment_type"]["value"]["selected_option"]["value"]
            receipt_by = values["receipt_by"]["value"]["value"]
            property_name = values["for_property"]["value"]["selected_option"]["value"]
            
            total_amount = room_amount + food_amount
            
            db = SessionLocal()
            try:
                new_income = models.Income(
                    name=name,
                    booking_number=booking_number,
                    contact_number=contact_number,
                    captured_date=captured_date,
                    room_amount=room_amount,
                    food_amount=food_amount,
                    payment_type=payment_type,
                    receipt_by=receipt_by,
                    for_property={"name": property_name},
                    submitted_by=user_name,
                    submitted_at=datetime.now()
                )
                db.add(new_income)
                db.commit()
                
                try:
                    client.chat_postMessage(
                        channel=user_id,
                        text=f"✅ Income recorded: ₹{total_amount} from {name} ({property_name})"
                    )
                except Exception:
                    pass
            except Exception as e:
                try:
                    client.chat_postMessage(
                        channel=user_id,
                        text=f"❌ Failed to save income in database. Error: {str(e)}"
                    )
                except Exception:
                    pass
            finally:
                db.close()
                
        except Exception as e:
            try:
                client.chat_postMessage(channel=user_id, text=f"❌ Error processing modal fields: {str(e)}")
            except Exception:
                pass

    threading.Thread(target=background_task).start()


# ==============================
# SLACK EVENTS ENDPOINT
# ==============================
@app.post("/slack/events", summary="Slack Events webhook endpoint")
async def slack_events(request: Request):
    try:
        body = await request.json()
        if body.get("type") == "url_verification":
            return {"challenge": body.get("challenge")}
    except Exception:
        pass

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
            
        new_expense = models.Expense(
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
        
        captured_date = income.captured_date
        if not captured_date:
            captured_date = datetime.now().strftime("%Y-%m-%d")
        
        new_income = models.Income(
            name=income.name,
            booking_number=income.booking_number,
            contact_number=income.contact_number,
            captured_date=captured_date,
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
    expense = db.query(models.Expense).filter(models.Expense.id == id).first()
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
    expense = db.query(models.Expense).filter(models.Expense.id == id).first()
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
    income = db.query(models.Income).filter(models.Income.id == id).first()
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
    income = db.query(models.Income).filter(models.Income.id == id).first()
    if not income or not income.payment_screenshot:
        raise HTTPException(status_code=404, detail="Screenshot not found")

    return Response(
        content=income.payment_screenshot,
        media_type="image/jpeg"
    )