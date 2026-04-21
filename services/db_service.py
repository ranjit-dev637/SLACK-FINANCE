import uuid
import logging
from datetime import datetime
from database import SessionLocal
from models import Income, Expense

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# INCOME — RAZORPAY AUTO-INSERT
# ══════════════════════════════════════════════════════════════════════════════
def insert_razorpay_income(parsed_data: dict, user_id: str) -> float:
    """
    Inserts a Razorpay auto-detected income record into the database.
    Assigns a unique transaction_id and marks status as COMPLETED
    (no screenshot required for auto-detected payments).
    Returns the amount saved. Raises an Exception on failure.
    """
    db = SessionLocal()
    try:
        captured_date_str = parsed_data.get("captured_date")
        captured_date_obj = (
            datetime.strptime(captured_date_str, "%Y-%m-%d").date()
            if captured_date_str
            else None
        )

        transaction_id = f"TXN-{uuid.uuid4().hex[:8]}"

        new_income = Income(
            transaction_id=transaction_id,
            user_id=user_id,
            status="COMPLETED",
            name="Razorpay",
            booking_number="AUTO",
            contact_number=parsed_data.get("contact"),
            captured_date=captured_date_obj,
            room_amount=parsed_data.get("amount", 0.0),
            food_amount=0.0,
            payment_type="Online",
            receipt_by="Razorpay",
            for_property={"name": "Auto Capture"},
            submitted_by=user_id,
            submitted_at=datetime.utcnow(),
            payment_screenshot=None,
        )

        db.add(new_income)
        db.commit()
        db.refresh(new_income)
        logger.info(
            f"Razorpay income saved | txn={transaction_id} | "
            f"amount=₹{new_income.room_amount}"
        )
        return new_income.room_amount
    except Exception as e:
        db.rollback()
        logger.error(f"DB error saving Razorpay income: {e}")
        raise
    finally:
        db.close()


# ══════════════════════════════════════════════════════════════════════════════
# INCOME — MANUAL FORM INSERT (REST API)
# ══════════════════════════════════════════════════════════════════════════════
def insert_income_form_record(data: dict, file_bytes: bytes) -> int:
    """
    Inserts an income form record submitted via the REST API.
    Screenshot bytes are attached immediately and status set to COMPLETED.
    Returns the inserted DB row ID. Raises Exception on failure.
    """
    db = SessionLocal()
    try:
        transaction_id = f"TXN-{uuid.uuid4().hex[:8]}"

        new_income = Income(
            transaction_id=transaction_id,
            user_id="API_USER",
            status="COMPLETED",
            name=data["name"],
            booking_number=str(data["booking_number"]),
            contact_number=data["contact_number"],
            captured_date=data["captured_date"],
            room_amount=float(data["room_amount"]),
            food_amount=float(data["food_amount"]),
            payment_type=data["payment_type"],
            receipt_by=data["receipt_by"],
            payment_screenshot=file_bytes,
            for_property={"name": "API Submission"},
            submitted_by="API User",
            submitted_at=datetime.utcnow(),
        )

        db.add(new_income)
        db.commit()
        db.refresh(new_income)
        logger.info(
            f"Manual income saved | txn={transaction_id} | db_id={new_income.id}"
        )
        return new_income.id
    except Exception as e:
        db.rollback()
        logger.error(f"DB error saving manual income: {e}")
        raise
    finally:
        db.close()


# ══════════════════════════════════════════════════════════════════════════════
# INCOME — FETCH LATEST PENDING RECORD FOR A USER
# ══════════════════════════════════════════════════════════════════════════════
def get_pending_income(user_id: str):
    """
    Fetches the most recent PENDING income record for the given Slack user_id.

    Equivalent SQL:
        SELECT * FROM incomes
        WHERE user_id = '<user_id>'
          AND status = 'PENDING'
        ORDER BY submitted_at DESC
        LIMIT 1;

    Returns (Income ORM object | None, db session).
    IMPORTANT: Caller is responsible for closing the session.
    """
    db = SessionLocal()
    try:
        record = (
            db.query(Income)
            .filter(Income.user_id == user_id, Income.status == "PENDING")
            .order_by(Income.submitted_at.desc())
            .first()
        )
        return record, db
    except Exception as e:
        db.close()
        logger.error(f"DB error fetching pending income for user {user_id}: {e}")
        raise


# ══════════════════════════════════════════════════════════════════════════════
# INCOME — ATTACH SCREENSHOT & MARK COMPLETED
# ══════════════════════════════════════════════════════════════════════════════
def complete_income_with_screenshot(transaction_id: str, file_bytes: bytes) -> bool:
    """
    Attaches screenshot bytes to an existing PENDING income record and marks
    it as COMPLETED — identified strictly by transaction_id.

    Equivalent SQL:
        UPDATE incomes
        SET payment_screenshot = <file_bytes>,
            status = 'COMPLETED'
        WHERE transaction_id = '<transaction_id>'
          AND status = 'PENDING';

    Returns True on success, False if no matching PENDING record found.
    Raises Exception on DB error.
    """
    db = SessionLocal()
    try:
        record = (
            db.query(Income)
            .filter(
                Income.transaction_id == transaction_id,
                Income.status == "PENDING",
            )
            .first()
        )

        if not record:
            logger.warning(
                f"No PENDING income found for transaction_id={transaction_id}"
            )
            return False

        record.payment_screenshot = file_bytes
        record.status = "COMPLETED"
        db.commit()
        logger.info(f"Income completed | txn={transaction_id} | db_id={record.id}")
        return True
    except Exception as e:
        db.rollback()
        logger.error(f"DB error completing income txn={transaction_id}: {e}")
        raise
    finally:
        db.close()


# ══════════════════════════════════════════════════════════════════════════════
# EXPENSE — INSERT PENDING RECORD (SLACK MODAL)
# ══════════════════════════════════════════════════════════════════════════════
def insert_expense_record(data: dict) -> str:
    """
    Inserts a new PENDING expense record submitted via the Slack modal.
    receipt_copy is NULL — the user must upload it separately.

    Returns the generated transaction_id. Raises Exception on failure.
    """
    db = SessionLocal()
    try:
        transaction_id = f"EXP-{uuid.uuid4().hex[:8]}"

        new_expense = Expense(
            transaction_id  = transaction_id,
            user_id         = data["user_id"],
            status          = "PENDING",
            expense_name    = data["expense_name"],
            seller_name     = data["seller_name"],
            gst_amount      = float(data["gst_amount"]),
            total_amount    = float(data["total_amount"]),
            purchase_date   = data["purchase_date"],
            priority        = data["priority"],
            paid_by         = data["paid_by"],
            mode_of_payment = data["mode_of_payment"],
            for_property    = {"name": data["property_name"]},
            submitted_by    = data["user_id"],
            submitted_at    = datetime.utcnow(),
            receipt_copy    = None,
        )

        db.add(new_expense)
        db.commit()
        db.refresh(new_expense)
        logger.info(
            f"PENDING expense created | txn={transaction_id} | "
            f"user={data['user_id']} | property={data['property_name']}"
        )
        return transaction_id
    except Exception as e:
        db.rollback()
        logger.error(f"DB error creating PENDING expense: {e}")
        raise
    finally:
        db.close()


# ══════════════════════════════════════════════════════════════════════════════
# EXPENSE — FETCH LATEST PENDING RECORD FOR A USER
# ══════════════════════════════════════════════════════════════════════════════
def get_pending_expense(user_id: str):
    """
    Fetches the most recent PENDING expense record for the given Slack user_id.

    Equivalent SQL:
        SELECT * FROM expenses
        WHERE user_id = '<user_id>'
          AND status = 'PENDING'
        ORDER BY submitted_at DESC
        LIMIT 1;

    Returns (Expense ORM object | None, db session).
    IMPORTANT: Caller is responsible for closing the session.
    """
    db = SessionLocal()
    try:
        record = (
            db.query(Expense)
            .filter(Expense.user_id == user_id, Expense.status == "PENDING")
            .order_by(Expense.submitted_at.desc())
            .first()
        )
        return record, db
    except Exception as e:
        db.close()
        logger.error(f"DB error fetching pending expense for user {user_id}: {e}")
        raise


# ══════════════════════════════════════════════════════════════════════════════
# EXPENSE — ATTACH RECEIPT & MARK COMPLETED
# ══════════════════════════════════════════════════════════════════════════════
def complete_expense_with_receipt(transaction_id: str, file_bytes: bytes) -> bool:
    """
    Attaches receipt bytes to an existing PENDING expense record and marks
    it as COMPLETED — identified strictly by transaction_id.

    Equivalent SQL:
        UPDATE expenses
        SET receipt_copy = <file_bytes>,
            status = 'COMPLETED'
        WHERE transaction_id = '<transaction_id>'
          AND status = 'PENDING';

    Returns True on success, False if no matching PENDING record found.
    Raises Exception on DB error. Will NOT overwrite COMPLETED records.
    """
    db = SessionLocal()
    try:
        record = (
            db.query(Expense)
            .filter(
                Expense.transaction_id == transaction_id,
                Expense.status == "PENDING",
            )
            .first()
        )

        if not record:
            logger.warning(
                f"No PENDING expense found for transaction_id={transaction_id}"
            )
            return False

        record.receipt_copy = file_bytes
        record.status = "COMPLETED"
        db.commit()
        logger.info(
            f"Expense completed | txn={transaction_id} | db_id={record.id}"
        )
        return True
    except Exception as e:
        db.rollback()
        logger.error(f"DB error completing expense txn={transaction_id}: {e}")
        raise
    finally:
        db.close()
