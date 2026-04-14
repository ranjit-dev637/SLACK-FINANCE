from pydantic import BaseModel, ConfigDict
from typing import List, Optional
from datetime import date, datetime

class ExpenseSchema(BaseModel):
    expense_name: str
    seller_name: str
    gst_amount: float
    total_amount: float
    purchase_date: date
    receipt_copy: Optional[str] = None
    priority: str
    paid_by: str
    mode_of_payment: str
    for_property: List[str]
    submitted_by: str
    submitted_at: datetime

    model_config = ConfigDict(from_attributes=True)

class IncomeSchema(BaseModel):
    for_property: List[str]
    name: str
    receipt_date: str
    booking_number: str
    payment_type: str
    room_amount: float
    food_amount: float
    payment_screenshot: Optional[str] = None
    receipt_by: str
    submitted_by: str
    submitted_at: datetime

    model_config = ConfigDict(from_attributes=True)
