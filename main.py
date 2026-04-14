import os
import sys
from dotenv import load_dotenv

load_dotenv()

from datetime import datetime
from fastapi import FastAPI, Depends, Request, UploadFile, File, Form, Response, HTTPException
from sqlalchemy.orm import Session
import database
from database import engine, get_db, Base
import models
from schemas import ExpenseSchema, IncomeSchema
from slack_bolt.async_app import AsyncApp
from slack_bolt.adapter.fastapi.async_handler import AsyncSlackRequestHandler

# Create the database tables
Base.metadata.create_all(bind=engine)

app = FastAPI(title="Finance Ingestion API")

# Initialize Slack App
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET")

if not SLACK_BOT_TOKEN or not SLACK_SIGNING_SECRET:
    print("ERROR: SLACK_BOT_TOKEN and SLACK_SIGNING_SECRET must be set in the .env file.")
    sys.exit(1)

slack_app = AsyncApp(
    token=SLACK_BOT_TOKEN,
    signing_secret=SLACK_SIGNING_SECRET
)
handler = AsyncSlackRequestHandler(slack_app)

@app.post("/expense")
def create_expense(expense: ExpenseSchema, db: Session = Depends(get_db)):
    db_data = expense.model_dump()
    if isinstance(db_data.get("receipt_copy"), str):
        db_data["receipt_copy"] = None
        
    db_expense = models.ExpenseDB(**db_data)
    db.add(db_expense)
    db.commit()
    db.refresh(db_expense)
    return {
        "status": "success",
        "message": "Expense record ingested successfully.",
        "data": {"id": db_expense.id}
    }

@app.post("/income")
def create_income(income: IncomeSchema, db: Session = Depends(get_db)):
    db_data = income.model_dump()
    if isinstance(db_data.get("payment_screenshot"), str):
        db_data["payment_screenshot"] = None
        
    db_income = models.IncomeDB(**db_data)
    db.add(db_income)
    db.commit()
    db.refresh(db_income)
    return {
        "status": "success",
        "message": "Income record ingested successfully.",
        "data": {"id": db_income.id}
    }

import logging

# Initialize logger for file endpoints
logger = logging.getLogger(__name__)

@app.post("/expense/{id}/upload-receipt")
async def upload_expense_receipt(id: int, file: UploadFile = File(...), db: Session = Depends(get_db)):
    try:
        db_expense = db.query(models.ExpenseDB).filter(models.ExpenseDB.id == id).first()
        if not db_expense:
            raise HTTPException(status_code=404, detail="Expense not found")
            
        # Safely read bytes from UploadFile spool
        file_bytes = await file.read()
        if not file_bytes:
            raise HTTPException(status_code=400, detail="Uploaded file is empty.")
            
        db_expense.receipt_copy = file_bytes
        db.commit()
        return {"status": "success", "message": f"Receipt '{file.filename}' uploaded successfully."}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to process receipt upload for expense ID {id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error during upload")

@app.post("/income/{id}/upload-screenshot")
async def upload_income_screenshot(id: int, file: UploadFile = File(...), db: Session = Depends(get_db)):
    try:
        db_income = db.query(models.IncomeDB).filter(models.IncomeDB.id == id).first()
        if not db_income:
            raise HTTPException(status_code=404, detail="Income not found")
            
        # Safely read bytes
        file_bytes = await file.read()
        if not file_bytes:
            raise HTTPException(status_code=400, detail="Uploaded file is empty.")
            
        db_income.payment_screenshot = file_bytes
        db.commit()
        return {"status": "success", "message": f"Screenshot '{file.filename}' uploaded successfully."}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to process screenshot upload for income ID {id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error during upload")

@app.get("/expense/{id}/receipt")
def get_expense_receipt(id: int, db: Session = Depends(get_db)):
    try:
        db_expense = db.query(models.ExpenseDB).filter(models.ExpenseDB.id == id).first()
        if not db_expense or not db_expense.receipt_copy:
            raise HTTPException(status_code=404, detail="Receipt not found")
        
        # Safely resolve content type mapping based on precise magic bytes
        content_type = "image/jpeg"
        if len(db_expense.receipt_copy) >= 8 and db_expense.receipt_copy.startswith(b'\x89PNG\r\n\x1a\n'):
            content_type = "image/png"
            
        return Response(content=db_expense.receipt_copy, media_type=content_type)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch receipt for expense ID {id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error retrieving file")

@app.get("/income/{id}/screenshot")
def get_income_screenshot(id: int, db: Session = Depends(get_db)):
    try:
        db_income = db.query(models.IncomeDB).filter(models.IncomeDB.id == id).first()
        if not db_income or not db_income.payment_screenshot:
            raise HTTPException(status_code=404, detail="Screenshot not found")
            
        content_type = "image/jpeg"
        if len(db_income.payment_screenshot) >= 8 and db_income.payment_screenshot.startswith(b'\x89PNG\r\n\x1a\n'):
            content_type = "image/png"
            
        return Response(content=db_income.payment_screenshot, media_type=content_type)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch screenshot for income ID {id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error retrieving file")

@slack_app.command("/expense")
async def handle_expense_command(ack, body, client, logger):
    await ack()
    try:
        await client.views_open(
            trigger_id=body["trigger_id"],
            view={
                "type": "modal",
                "callback_id": "expense_form",
                "title": { "type": "plain_text", "text": "Submit New Expense" },
                "submit": { "type": "plain_text", "text": "Submit Expense" },
                "close": { "type": "plain_text", "text": "Cancel" },
                "blocks": [
                    { "type": "input", "block_id": "expense_name", "label": { "type": "plain_text", "text": "Expense Name" }, "element": { "type": "plain_text_input", "action_id": "expense_name", "placeholder": { "type": "plain_text", "text": "Office Stationery Purchase" } } },
                    { "type": "input", "block_id": "seller_name", "label": { "type": "plain_text", "text": "Seller Name" }, "element": { "type": "plain_text_input", "action_id": "seller_name", "placeholder": { "type": "plain_text", "text": "Stationery World" } } },
                    { "type": "input", "block_id": "total_amount", "label": { "type": "plain_text", "text": "Total Amount (₹)" }, "element": { "type": "plain_text_input", "action_id": "total_amount", "placeholder": { "type": "plain_text", "text": "2850.00" } } },
                    { "type": "input", "block_id": "gst_amount", "label": { "type": "plain_text", "text": "GST Amount" }, "element": { "type": "plain_text_input", "action_id": "gst_amount", "placeholder": { "type": "plain_text", "text": "450.00" } } },
                    { "type": "input", "block_id": "purchase_date", "label": { "type": "plain_text", "text": "Purchase Date" }, "element": { "type": "datepicker", "action_id": "purchase_date" } },
                    { "type": "input", "block_id": "paid_by", "label": { "type": "plain_text", "text": "Paid By" }, "element": { "type": "plain_text_input", "action_id": "paid_by", "placeholder": { "type": "plain_text", "text": "Rahul Sharma" } } },
                    { "type": "input", "block_id": "mode_of_payment", "label": { "type": "plain_text", "text": "Mode of Payment" }, "element": { "type": "static_select", "action_id": "mode_of_payment", "options": [
                    { "text": { "type": "plain_text", "text": "UPI" }, "value": "UPI" },
                    { "text": { "type": "plain_text", "text": "Cash" }, "value": "Cash" },
                    { "text": { "type": "plain_text", "text": "Card" }, "value": "Card" }
                    ] } },
                    { "type": "input", "block_id": "for_property", "label": { "type": "plain_text", "text": "For Property" }, "element": { "type": "multi_static_select", "action_id": "for_property", "options": [
                    { "text": { "type": "plain_text", "text": "Clover Villa" }, "value": "Clover Villa" },
                    { "text": { "type": "plain_text", "text": "Kitchen" }, "value": "Kitchen" }
                    ] } }
                ]
            }
        )
    except Exception as e:
        logger.error(f"Error opening expense modal: {e}")

@slack_app.command("/income")
async def handle_income_command(ack, body, client, logger):
    await ack()
    try:
        await client.views_open(
            trigger_id=body["trigger_id"],
            view={
                "type": "modal",
                "callback_id": "income_form",
                "title": { "type": "plain_text", "text": "Submit New Income" },
                "submit": { "type": "plain_text", "text": "Submit Income" },
                "close": { "type": "plain_text", "text": "Cancel" },
                "blocks": [
                    { "type": "input", "block_id": "for_property", "label": { "type": "plain_text", "text": "For Property" }, "element": { "type": "multi_static_select", "action_id": "for_property", "options": [
                    { "text": { "type": "plain_text", "text": "Clover Villa" }, "value": "Clover Villa" },
                    { "text": { "type": "plain_text", "text": "Kitchen" }, "value": "Kitchen" }
                    ] } },
                    { "type": "input", "block_id": "name", "label": { "type": "plain_text", "text": "Customer Name" }, "element": { "type": "plain_text_input", "action_id": "name", "placeholder": { "type": "plain_text", "text": "John Doe" } } },
                    { "type": "input", "block_id": "receipt_date", "label": { "type": "plain_text", "text": "Receipt Date" }, "element": { "type": "datepicker", "action_id": "receipt_date" } },
                    { "type": "input", "block_id": "booking_number", "label": { "type": "plain_text", "text": "Booking Number" }, "element": { "type": "plain_text_input", "action_id": "booking_number", "placeholder": { "type": "plain_text", "text": "BKG-101" } } },
                    { "type": "input", "block_id": "room_amount", "label": { "type": "plain_text", "text": "Room Amount (₹)" }, "element": { "type": "plain_text_input", "action_id": "room_amount" } },
                    { "type": "input", "block_id": "food_amount", "label": { "type": "plain_text", "text": "Food Amount (₹)" }, "element": { "type": "plain_text_input", "action_id": "food_amount" } },
                    { "type": "input", "block_id": "payment_type", "label": { "type": "plain_text", "text": "Payment Type" }, "element": { "type": "static_select", "action_id": "payment_type", "options": [
                    { "text": { "type": "plain_text", "text": "UPI" }, "value": "UPI" },
                    { "text": { "type": "plain_text", "text": "Cash" }, "value": "Cash" },
                    { "text": { "type": "plain_text", "text": "Card" }, "value": "Card" }
                    ] } },
                    { "type": "input", "block_id": "receipt_by", "label": { "type": "plain_text", "text": "Receipt By" }, "element": { "type": "plain_text_input", "action_id": "receipt_by" } }
                ]
            }
        )
    except Exception as e:
        logger.error(f"Error opening income modal: {e}")

@slack_app.view("expense_form")
async def handle_expense_submission(ack, body, client, view, logger):
    await ack()
    user = body["user"]["id"]
    values = view["state"]["values"]
    
    try:
        # Extract inputs
        expense_name = values["expense_name"]["expense_name"]["value"]
        seller_name = values["seller_name"]["seller_name"]["value"]
        total_amount = float(values["total_amount"]["total_amount"]["value"])
        gst_amount = float(values["gst_amount"]["gst_amount"]["value"])
        purchase_date_str = values["purchase_date"]["purchase_date"]["selected_date"]
        purchase_date = datetime.strptime(purchase_date_str, "%Y-%m-%d").date()
        paid_by = values["paid_by"]["paid_by"]["value"]
        mode_of_payment = values["mode_of_payment"]["mode_of_payment"]["selected_option"]["value"]
        for_property = [option["value"] for option in values["for_property"]["for_property"]["selected_options"]]
        
        # Build Pydantic schema
        expense_data = ExpenseSchema(
            expense_name=expense_name, seller_name=seller_name,
            gst_amount=gst_amount, total_amount=total_amount, purchase_date=purchase_date,
            priority="Normal", paid_by=paid_by, mode_of_payment=mode_of_payment,
            for_property=for_property, submitted_by=f"<@{user}>", submitted_at=datetime.utcnow()
        )
        
        # Save to DB correctly
        db = database.SessionLocal()
        try:
            db_data = expense_data.model_dump()
            if isinstance(db_data.get("receipt_copy"), str):
                db_data["receipt_copy"] = None
            db_expense = models.ExpenseDB(**db_data)
            db.add(db_expense)
            db.commit()
            db.refresh(db_expense)
            logger.info(f"Successfully saved expense {db_expense.id} to DB.")
        except Exception as db_err:
            db.rollback()
            raise db_err
        finally:
            db.close()
        
        await client.chat_postMessage(channel=user, text=f"✅ Successfully registered expense: *{expense_name}* for ₹{total_amount}.")
    except Exception as e:
        logger.error(f"Error handling expense submission: {e}")
        await client.chat_postMessage(channel=user, text=f"❌ Failed to submit expense. Error: {e}")

@slack_app.view("income_form")
async def handle_income_submission(ack, body, client, view, logger):
    await ack()
    user = body["user"]["id"]
    values = view["state"]["values"]

    try:
        # Extract inputs
        name = values["name"]["name"]["value"]
        receipt_date = values["receipt_date"]["receipt_date"]["selected_date"]
        booking_number = values["booking_number"]["booking_number"]["value"]
        room_amount = float(values["room_amount"]["room_amount"]["value"])
        food_amount = float(values["food_amount"]["food_amount"]["value"])
        payment_type = values["payment_type"]["payment_type"]["selected_option"]["value"]
        receipt_by = values["receipt_by"]["receipt_by"]["value"]
        for_property = [option["value"] for option in values["for_property"]["for_property"]["selected_options"]]

        # Build schema ensuring receipt_date is passed as a valid string
        income_data = IncomeSchema(
            for_property=for_property, name=name, receipt_date=str(receipt_date),
            booking_number=booking_number, payment_type=payment_type, room_amount=room_amount,
            food_amount=food_amount, receipt_by=receipt_by, submitted_by=f"<@{user}>", submitted_at=datetime.utcnow()
        )

        # Save to DB correctly
        db = database.SessionLocal()
        try:
            db_data = income_data.model_dump()
            if isinstance(db_data.get("payment_screenshot"), str):
                db_data["payment_screenshot"] = None
            db_income = models.IncomeDB(**db_data)
            db.add(db_income)
            db.commit()
            db.refresh(db_income)
            logger.info(f"Successfully saved income {db_income.id} to DB.")
        except Exception as db_err:
            db.rollback()
            raise db_err
        finally:
            db.close()

        await client.chat_postMessage(channel=user, text=f"✅ Successfully registered income from *{name}* for BKG *{booking_number}*.")
    except Exception as e:
        logger.error(f"Error handling income submission: {e}")
        await client.chat_postMessage(channel=user, text=f"❌ Failed to submit income. Error: {e}")

@app.post("/slack/events")
async def endpoint(req: Request):
    return await handler.handle(req)
