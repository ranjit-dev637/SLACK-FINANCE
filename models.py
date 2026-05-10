from sqlalchemy import Column, Integer, String, Float, DateTime, Date, Boolean, JSON
from sqlalchemy.orm import declarative_base
from datetime import datetime

Base = declarative_base()

class BookingData(Base):
    __tablename__ = "bookings"

    id = Column(Integer, primary_key=True, index=True)
    booking_id = Column(String, unique=True, index=True)
    property_name = Column(String, index=True)
    booking_source = Column(String)
    room_type = Column(String)
    booking_status = Column(String)
    check_in = Column(DateTime)
    check_out = Column(DateTime)
    revenue = Column(Float)
    fetched_at = Column(DateTime, default=datetime.utcnow)

class InventoryData(Base):
    __tablename__ = "inventory_snapshots"

    id = Column(Integer, primary_key=True, index=True)
    property_name = Column(String, index=True)
    total_rooms = Column(Integer)
    available_rooms = Column(Integer)
    snapshot_time = Column(DateTime, default=datetime.utcnow)

class KPIReport(Base):
    __tablename__ = "kpi_reports"

    id = Column(Integer, primary_key=True, index=True)
    property_name = Column(String, index=True)
    report_time = Column(DateTime, default=datetime.utcnow)
    rooms_booked = Column(Integer)
    ota_bookings = Column(Integer)
    direct_bookings = Column(Integer)
    occupancy_percentage = Column(Float)
    available_rooms = Column(Integer)
    adr = Column(Float)
    revpar = Column(Float)
    cancellation_count = Column(Integer)
    no_show_count = Column(Integer)
    alerts = Column(String) # JSON string of alerts


# ==============================
# FINANCIAL MODELS
# ==============================

class Expense(Base):
    __tablename__ = "expenses"

    id = Column(Integer, primary_key=True, index=True)

    # Core Tracking
    transaction_id = Column(String, unique=True, index=True, nullable=True)
    user_id        = Column(String, index=True, nullable=True)
    status         = Column(String, index=True, default="PENDING")

    # Expense Details
    expense_name    = Column(String, index=True)
    seller_name     = Column(String)
    total_amount    = Column(Float)
    gst_amount      = Column(Float)
    purchase_date   = Column(Date)
    paid_by         = Column(String)
    mode_of_payment = Column(String)
    priority        = Column(String)

    # Metadata
    for_property = Column(JSON)
    submitted_by_id = Column(String)
    submitted_by_name = Column(String)
    submitted_by = Column(String)
    submitted_at = Column(DateTime(timezone=True))

    # Receipt URL (Supabase Storage)
    receipt_copy = Column(String, nullable=True)
    receipt_copies = Column(JSON, default=list)
    file_uploaded  = Column(Boolean, default=False)

    # Google Drive Links
    drive_links = Column(JSON, default=list)

    # Pipeline metadata
    updated_at    = Column(DateTime(timezone=True), nullable=True)
    error_message = Column(String, nullable=True)


class Income(Base):
    __tablename__ = "incomes"

    id = Column(Integer, primary_key=True, index=True)

    # Core Tracking
    transaction_id = Column(String, unique=True, index=True)
    user_id = Column(String, index=True)
    status = Column(String, index=True, default="PENDING")

    # Customer Details
    name = Column(String, index=True)
    booking_number = Column(String, index=True)
    contact_number = Column(String)

    # Date
    captured_date = Column(Date, nullable=True)
    receipt_date = Column(Date, nullable=True)

    # Amounts
    room_amount = Column(Float)
    food_amount = Column(Float)

    # Payment Info
    payment_type = Column(String)
    receipt_by = Column(String)

    # Metadata
    for_property = Column(JSON)
    submitted_by_id = Column(String)
    submitted_by_name = Column(String)
    submitted_by = Column(String)
    submitted_at = Column(DateTime(timezone=True))

    # Screenshot URL (Supabase Storage)
    payment_screenshot = Column(String, nullable=True)
    payment_screenshots = Column(JSON, default=list)
    file_uploaded       = Column(Boolean, default=False)

    # Google Drive Links
    drive_links = Column(JSON, default=list)

    # Pipeline metadata
    updated_at    = Column(DateTime(timezone=True), nullable=True)
    error_message = Column(String, nullable=True)


import uuid
from sqlalchemy.sql import func

class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()), index=True)
    transaction_id = Column(String, unique=True, index=True)
    name = Column(String)
    email = Column(String)
    amount = Column(Float, nullable=True)
    screenshot_url = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class UploadJob(Base):
    __tablename__ = "upload_jobs"

    id = Column(Integer, primary_key=True, index=True)
    transaction_id = Column(String, index=True, nullable=False)
    user_id = Column(String, nullable=True)
    channel_id = Column(String, nullable=True)
    file_url = Column(String, unique=True, index=True, nullable=False)
    mime_type = Column(String, nullable=True)
    status = Column(String, index=True, default="QUEUED", nullable=False)
    attempts = Column(Integer, default=0, nullable=False)
    max_attempts = Column(Integer, default=5, nullable=False)
    next_retry_at = Column(DateTime(timezone=True), server_default=func.now())
    drive_link = Column(String, nullable=True)
    error_message = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now())
