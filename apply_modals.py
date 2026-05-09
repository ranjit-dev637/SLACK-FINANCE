import re

def refactor():
    with open('main.py', 'r', encoding='utf-8') as f:
        content = f.read()

    # Find start and end markers
    start_marker = r'# ==============================\n# SLACK COMMAND: /income'
    end_marker = r'import collections\nimport time\nimport json\n\n# Thread-safe bounded cache for deduplicating Slack events\nPROCESSED_EVENTS = {}'

    match = re.search(f'({start_marker}.*?)({end_marker})', content, flags=re.DOTALL)
    
    if not match:
        print("Could not find block to replace.")
        return
        
    new_code = """# ==============================
# SLASH COMMANDS
# ==============================
@slack_app.command("/income")
def handle_income_command(ack, body, client):
    ack()
    client.views_open(
        trigger_id=body["trigger_id"],
        view=get_income_modal()
    )

@slack_app.command("/expense")
def handle_expense_command(ack, body, client):
    ack()
    client.views_open(
        trigger_id=body["trigger_id"],
        view=get_expense_modal()
    )


# ==============================
# MODAL DEFINITIONS
# ==============================
def get_income_modal():
    return {
        "type": "modal",
        "callback_id": "income_form",
        "title": {"type": "plain_text", "text": "Record Income"},
        "submit": {"type": "plain_text", "text": "Submit"},
        "close": {"type": "plain_text", "text": "Cancel"},
        "blocks": [
            {
                "type": "input",
                "block_id": "income_name",
                "element": {"type": "plain_text_input", "action_id": "name_input"},
                "label": {"type": "plain_text", "text": "Income Source"}
            },
            {
                "type": "input",
                "block_id": "income_amount",
                "element": {"type": "plain_text_input", "action_id": "amount_input"},
                "label": {"type": "plain_text", "text": "Amount"}
            },
            {
                "type": "input",
                "block_id": "income_type",
                "element": {"type": "plain_text_input", "action_id": "type_input"},
                "label": {"type": "plain_text", "text": "Payment Type"}
            },
            {
                "type": "input",
                "block_id": "income_notes",
                "optional": True,
                "element": {"type": "plain_text_input", "action_id": "notes_input", "multiline": True},
                "label": {"type": "plain_text", "text": "Notes"}
            }
        ]
    }

def get_expense_modal():
    return {
        "type": "modal",
        "callback_id": "expense_form",
        "title": {"type": "plain_text", "text": "Record Expense"},
        "submit": {"type": "plain_text", "text": "Submit"},
        "close": {"type": "plain_text", "text": "Cancel"},
        "blocks": [
            {
                "type": "input",
                "block_id": "expense_name",
                "element": {"type": "plain_text_input", "action_id": "name_input"},
                "label": {"type": "plain_text", "text": "Expense Name"}
            },
            {
                "type": "input",
                "block_id": "expense_amount",
                "element": {"type": "plain_text_input", "action_id": "amount_input"},
                "label": {"type": "plain_text", "text": "Amount"}
            },
            {
                "type": "input",
                "block_id": "expense_category",
                "element": {"type": "plain_text_input", "action_id": "category_input"},
                "label": {"type": "plain_text", "text": "Category"}
            },
            {
                "type": "input",
                "block_id": "expense_notes",
                "optional": True,
                "element": {"type": "plain_text_input", "action_id": "notes_input", "multiline": True},
                "label": {"type": "plain_text", "text": "Notes"}
            }
        ]
    }


# ==============================
# MODAL SUBMISSION HANDLERS
# ==============================
@slack_app.view("income_form")
def handle_income_submission(ack, body, client, view):
    ack()
    user_id = body["user"]["id"]
    values = view["state"]["values"]
    
    name = values["income_name"]["name_input"]["value"]
    amount = values["income_amount"]["amount_input"]["value"]
    payment_type = values["income_type"]["type_input"]["value"]
    notes = values.get("income_notes", {}).get("notes_input", {}).get("value", "")
    
    transaction_id = f"INC-{uuid.uuid4().hex[:8].upper()}"
    
    db = SessionLocal()
    try:
        from models import Income
        new_income = Income(
            transaction_id=transaction_id,
            user_id=user_id,
            received_from=name,
            amount=float(amount),
            payment_type=payment_type,
            notes=notes,
            status="PENDING",
            file_uploaded=False,
            submitted_at=datetime.utcnow(),
            # Mandatory fields fallback to avoid DB constraints failures:
            name=name,
            booking_number="N/A",
            contact_number="N/A",
            captured_date=datetime.now().date(),
            room_amount=0,
            food_amount=0,
            receipt_by=user_id,
            for_property={"name": "N/A"},
            submitted_by=user_id
        )
        db.add(new_income)
        db.commit()
    except Exception as e:
        db.rollback()
        client.chat_postMessage(channel=user_id, text=f"❌ Error creating transaction: {e}")
        return
    finally:
        db.close()
        
    client.chat_postMessage(
        channel=user_id,
        text=f"✅ Transaction created: {transaction_id}\\n📎 Please upload your payment screenshot in this chat."
    )

@slack_app.view("expense_form")
def handle_expense_submission(ack, body, client, view):
    ack()
    user_id = body["user"]["id"]
    values = view["state"]["values"]
    
    name = values["expense_name"]["name_input"]["value"]
    amount = values["expense_amount"]["amount_input"]["value"]
    category = values["expense_category"]["category_input"]["value"]
    notes = values.get("expense_notes", {}).get("notes_input", {}).get("value", "")
    
    transaction_id = f"EXP-{uuid.uuid4().hex[:8].upper()}"
    
    db = SessionLocal()
    try:
        from models import Expense
        new_expense = Expense(
            transaction_id=transaction_id,
            user_id=user_id,
            expense_name=name,
            amount=float(amount),
            category=category,
            notes=notes,
            status="PENDING",
            file_uploaded=False,
            submitted_at=datetime.utcnow(),
            # Mandatory fields fallback to avoid DB constraints failures:
            seller_name="N/A",
            gst_amount=0,
            total_amount=float(amount),
            purchase_date=datetime.now().date(),
            priority="Normal",
            paid_by=user_id,
            mode_of_payment="N/A",
            property_name="N/A"
        )
        db.add(new_expense)
        db.commit()
    except Exception as e:
        db.rollback()
        client.chat_postMessage(channel=user_id, text=f"❌ Error creating transaction: {e}")
        return
    finally:
        db.close()
        
    client.chat_postMessage(
        channel=user_id,
        text=f"✅ Transaction created: {transaction_id}\\n📎 Please upload your payment screenshot in this chat."
    )

"""
    new_content = content[:match.start(1)] + new_code + content[match.start(2):]
    with open('main.py', 'w', encoding='utf-8') as f:
        f.write(new_content)
    print("Modals and Submission Handlers replaced successfully.")

if __name__ == '__main__':
    refactor()
