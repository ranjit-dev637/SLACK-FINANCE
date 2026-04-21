import re
from typing import Dict, Any

def parse_razorpay_message(text: str) -> Dict[str, Any]:
    """
    Parses Razorpay message text and extracts amount, contact, and captured_date.
    Returns a dictionary of parsed values.
    """
    parsed_data = {
        "amount": 0.0,
        "contact": None,
        "captured_date": None
    }
    
    # amount → Rs.(\d+,?\d*)
    amount_match = re.search(r'Rs\.(\d+,?\d*)', text)
    if amount_match:
        amount_str = amount_match.group(1).replace(',', '')
        parsed_data["amount"] = float(amount_str)
        
    # contact → Contact:\s*(?:tel:)?(\+?\d+)
    contact_match = re.search(r'Contact:\s*(?:tel:)?(\+?\d+)', text)
    if contact_match:
        parsed_data["contact"] = contact_match.group(1)
        
    # captured date → Captured at:\s*(\d{2}-\d{2}-\d{4})
    date_match = re.search(r'Captured at:\s*(\d{2}-\d{2}-\d{4})', text)
    if date_match:
        # Convert date → YYYY-MM-DD format
        date_parts = date_match.group(1).split('-')
        if len(date_parts) == 3:
            # DD-MM-YYYY -> YYYY-MM-DD
            parsed_data["captured_date"] = f"{date_parts[2]}-{date_parts[1]}-{date_parts[0]}"
            
    return parsed_data
