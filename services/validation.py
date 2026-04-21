import re
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

ALLOWED_PAYMENT_TYPES = [
    "Cash",
    "Bank Transfer",
    "UPI",
    "Credit Card",
    "ICICI Bank POS",
    "HDFC Bank POS",
    "QR Code Standy (ICICI)"
]

NAME_REGEX = re.compile(r"^[A-Za-z\s]+$")

def validate_income_data(
    name: str,
    booking_number: int,
    contact_number: str,
    room_amount: int,
    food_amount: int,
    payment_type: str,
    receipt_by: str
) -> None:
    """
    Validates income data rigorously.
    Raises ValueError on any strict rule violation.
    """
    # 1. Name validation
    if not name or not NAME_REGEX.match(name):
        logger.warning(f"Validation failed for name: {name}")
        raise ValueError("name must contain only alphabets and spaces.")

    # 2. Booking Number validation
    # If it's already an int from FastAPI, we just ensure it's not negative if we want,
    # but rule says "Only integers, no alphabets or special characters."
    # If passed as str, we check. If passed as int, it's valid.
    if isinstance(booking_number, str):
        if not booking_number.isdigit():
            logger.warning(f"Validation failed for booking_number: {booking_number}")
            raise ValueError("booking_number must contain only integers.")
        
    # 3. room_amount
    if not isinstance(room_amount, int) or room_amount <= 0:
        logger.warning(f"Validation failed for room_amount: {room_amount}")
        raise ValueError("room_amount must be an integer greater than 0.")
        
    # 4. food_amount
    if not isinstance(food_amount, int) or food_amount < 0:
        logger.warning(f"Validation failed for food_amount: {food_amount}")
        raise ValueError("food_amount must be an integer greater than or equal to 0.")

    # 5. receipt_by
    if not receipt_by or not NAME_REGEX.match(receipt_by):
        logger.warning(f"Validation failed for receipt_by: {receipt_by}")
        raise ValueError("receipt_by must contain only alphabets and spaces.")

    # 6. payment_type
    if payment_type not in ALLOWED_PAYMENT_TYPES:
        logger.warning(f"Validation failed for payment_type: {payment_type}")
        raise ValueError(f"payment_type must be one of: {', '.join(ALLOWED_PAYMENT_TYPES)}")
        
    logger.info("Income data validation passed successfully.")

def validate_form(data: dict) -> dict:
    """
    Validates form data specifically for Slack UI modals.
    Returns a dictionary of errors with exact block_id keys.
    """
    errors = {}
    
    # 1. Customer Name (name_block)
    name = data.get("name", "")
    if not name or not NAME_REGEX.match(str(name)):
        errors["name_block"] = "Name must contain only alphabets and spaces"
        
    # 2. Booking Number (booking_block)
    booking_number = str(data.get("booking_number", ""))
    if not booking_number.isdigit():
        errors["booking_block"] = "Booking number must be numeric"
        
    # 3. Room Amount (room_block)
    room_amount = str(data.get("room_amount", ""))
    if not room_amount.isdigit() or int(room_amount) <= 0:
        errors["room_block"] = "Room Amount must be an integer greater than 0"
        
    # 4. Food Amount (food_block)
    food_amount = str(data.get("food_amount", ""))
    if not food_amount.isdigit() or int(food_amount) < 0:
        errors["food_block"] = "Food Amount must be an integer greater than or equal to 0"
        
    # 5. Receipt By (receipt_block)
    receipt_by = str(data.get("receipt_by", ""))
    if not receipt_by or not NAME_REGEX.match(receipt_by):
        errors["receipt_block"] = "Receipt By must contain only alphabets and spaces"
        
    # 6. Payment Type (payment_type)
    payment_type = str(data.get("payment_type", ""))
    if payment_type not in ALLOWED_PAYMENT_TYPES:
        errors["payment_type"] = f"Payment Type must be one of: {', '.join(ALLOWED_PAYMENT_TYPES)}"
        
    return errors
