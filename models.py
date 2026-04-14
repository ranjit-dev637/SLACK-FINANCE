from sqlalchemy import Column, Integer, String, Float, DateTime, Date, JSON, LargeBinary
from database import Base

class ExpenseDB(Base):
    __tablename__ = "expenses"

    id = Column(Integer, primary_key=True, index=True)
    expense_name = Column(String, index=True)
    seller_name = Column(String)
    gst_amount = Column(Float)
    total_amount = Column(Float)
    purchase_date = Column(Date)
    receipt_copy = Column(LargeBinary, nullable=True)
    priority = Column(String)
    paid_by = Column(String)
    mode_of_payment = Column(String)
    for_property = Column(JSON)
    submitted_by = Column(String)
    submitted_at = Column(DateTime(timezone=True))

class IncomeDB(Base):
    __tablename__ = "incomes"

    id = Column(Integer, primary_key=True, index=True)
    for_property = Column(JSON)
    name = Column(String, index=True)
    receipt_date = Column(String)
    booking_number = Column(String, index=True)
    payment_type = Column(String)
    room_amount = Column(Float)
    food_amount = Column(Float)
    payment_screenshot = Column(LargeBinary, nullable=True)
    receipt_by = Column(String)
    submitted_by = Column(String)
    submitted_at = Column(DateTime(timezone=True))
