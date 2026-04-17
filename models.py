from sqlalchemy import Column, Integer, String, Float, DateTime, Date, JSON, LargeBinary
from database import Base

class ExpenseDB(Base):
    __tablename__ = "expenses"

    id = Column(Integer, primary_key=True, index=True)
    expense_name = Column(String, index=True)
    seller_name = Column(String)
    total_amount = Column(Float)
    gst_amount = Column(Float)
    purchase_date = Column(Date)
    paid_by = Column(String)
    mode_of_payment = Column(String)
    priority = Column(String)
    for_property = Column(JSON)
    submitted_by = Column(String)
    submitted_at = Column(DateTime(timezone=True))
    receipt_copy = Column(LargeBinary, nullable=True)

class IncomeDB(Base):
    __tablename__ = "incomes"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    booking_number = Column(String, index=True)
    contact_number = Column(String)
    captured_date = Column(String)
    room_amount = Column(Float)
    food_amount = Column(Float)
    payment_type = Column(String)
    receipt_by = Column(String)
    for_property = Column(JSON)
    submitted_by = Column(String)
    submitted_at = Column(DateTime(timezone=True))
    payment_screenshot = Column(LargeBinary, nullable=True)

Expense = ExpenseDB
Income = IncomeDB