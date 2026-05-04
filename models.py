from sqlalchemy import Column, Integer, String, Float, DateTime, Date, JSON, Boolean
from database import Base


class Expense(Base):
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
    submitted_by_id = Column(String)
    submitted_by_name = Column(String)
    submitted_by = Column(String)  # Deprecated, keep for safety
    submitted_at = Column(DateTime(timezone=True))

    # 🔹 Receipt URL (Supabase Storage)
    receipt_copy = Column(String, nullable=True)  # public URL (latest)

    receipt_copies = Column(JSON, default=list)
    file_uploaded  = Column(Boolean, default=False)

    # 🔹 Google Drive Links
    drive_links = Column(JSON, default=list)

    # 🔹 Pipeline metadata
    updated_at    = Column(DateTime(timezone=True), nullable=True)
    error_message = Column(String, nullable=True)


class Income(Base):
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
    submitted_by_id = Column(String)
    submitted_by_name = Column(String)
    submitted_by = Column(String)  # Deprecated, keep for safety
    submitted_at = Column(DateTime(timezone=True))

    # 🔹 Screenshot URL (Supabase Storage)
    payment_screenshot = Column(String, nullable=True)  # public URL (latest)

    payment_screenshots = Column(JSON, default=list)
    file_uploaded       = Column(Boolean, default=False)

    # 🔹 Google Drive Links
    drive_links = Column(JSON, default=list)

    # 🔹 Pipeline metadata
    updated_at    = Column(DateTime(timezone=True), nullable=True)
    error_message = Column(String, nullable=True)


# Aliases removed

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