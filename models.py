from sqlalchemy import Column, Integer, String, Float, DateTime, Date, JSON, LargeBinary
from database import Base


class ExpenseDB(Base):
    __tablename__ = "expenses"

    id = Column(Integer, primary_key=True, index=True)

    # 🔹 Core Tracking
    transaction_id = Column(String, unique=True, index=True, nullable=True)
    user_id        = Column(String, index=True, nullable=True)
    status         = Column(String, index=True, default="PENDING")

    # 🔹 Expense Details
    expense_name    = Column(String, index=True)
    seller_name     = Column(String)
    total_amount    = Column(Float)
    gst_amount      = Column(Float)
    purchase_date   = Column(Date)
    paid_by         = Column(String)
    mode_of_payment = Column(String)
    priority        = Column(String)

    # 🔹 Metadata
    for_property = Column(JSON)
    submitted_by = Column(String)
    submitted_at = Column(DateTime(timezone=True))

    # 🔹 Receipt (BLOB)
    receipt_copy = Column(LargeBinary, nullable=True)


class IncomeDB(Base):
    __tablename__ = "incomes"

    id = Column(Integer, primary_key=True, index=True)

    # 🔹 Core Tracking
    transaction_id = Column(String, unique=True, index=True)
    user_id = Column(String, index=True)
    status = Column(String, index=True, default="PENDING")

    # 🔹 Customer Details
    name = Column(String, index=True)
    booking_number = Column(String, index=True)
    contact_number = Column(String)

    # 🔹 Date (IMPORTANT: using captured_date instead of receipt_date)
    captured_date = Column(Date, nullable=True)
    receipt_date = Column(Date, nullable=True)

    # 🔹 Amounts
    room_amount = Column(Float)
    food_amount = Column(Float)

    # 🔹 Payment Info
    payment_type = Column(String)
    receipt_by = Column(String)

    # 🔹 Metadata
    for_property = Column(JSON)
    submitted_by = Column(String)
    submitted_at = Column(DateTime(timezone=True))

    # 🔹 Screenshot (BLOB)
    payment_screenshot = Column(LargeBinary, nullable=True)


# Aliases
Expense = ExpenseDB
Income = IncomeDB

import uuid
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

class TransactionDB(Base):
    __tablename__ = "transactions"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()), index=True)
    transaction_id = Column(String, unique=True, index=True)
    name = Column(String)
    email = Column(String)
    amount = Column(Float, nullable=True)
    screenshot_url = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

Transaction = TransactionDB