import re
import os

def fix_main_py():
    with open('main.py', 'r', encoding='utf-8') as f:
        content = f.read()

    # 1. Idempotency & Slack events route
    start_marker = r'PROCESSED_EVENTS = '
    end_marker = r'# ==============================\n# CREATE EXPENSE \(API\)'

    match = re.search(f'({start_marker}.*?)({end_marker})', content, flags=re.DOTALL)
    if not match:
        print('Could not find chunk in main.py to replace')
        return

    new_code = '''PROCESSED_EVENTS = {}

def is_event_processed(event_id):
    return event_id in PROCESSED_EVENTS

def mark_event_processed(event_id):
    if not event_id: return
    PROCESSED_EVENTS[event_id] = True

def extract_txn_from_message(text: str, user_id: str) -> str | None:
    import re
    if text:
        match = re.search(r'\\b(EXP-|INC-|TXN-)[A-Za-z0-9\\-]+\\b', text)
        if match:
            return match.group(0)

    db = SessionLocal()
    try:
        from models import Income, Expense
        
        incomes = db.query(Income).filter(
            Income.user_id == user_id,
            Income.status == "PENDING",
            Income.payment_screenshot == None
        ).order_by(Income.submitted_at.desc()).limit(2).all()
        
        expenses = db.query(Expense).filter(
            Expense.user_id == user_id,
            Expense.status == "PENDING",
            Expense.receipt_copy == None
        ).order_by(Expense.submitted_at.desc()).limit(2).all()
        
        total_pending = len(incomes) + len(expenses)
        
        if total_pending > 1:
            slack_app.client.chat_postMessage(
                channel=user_id,
                text="❌ Multiple pending submissions found. Please upload the screenshot again and include the exact TXN ID in the message text."
            )
            return None
            
        if total_pending == 1:
            if incomes:
                return incomes[0].transaction_id
            return expenses[0].transaction_id
            
    finally:
        db.close()
        
    slack_app.client.chat_postMessage(
        channel=user_id,
        text="❌ No pending submission found. Please submit /income or /expense first."
    )
    return None

def process_slack_file_event(event):
    try:
        if "files" not in event or len(event["files"]) == 0:
            return
            
        file_info = event["files"][0]
        file_url = file_info.get("url_private_download") or file_info.get("url_private")
        user_id = event.get("user")
        text = event.get("text", "")
        
        if not file_url or not user_id:
            return
            
        transaction_id = extract_txn_from_message(text, user_id)
        if not transaction_id:
            return
            
        db = SessionLocal()
        try:
            record_type = "expense" if "EXP" in transaction_id else "income"
            from models import Income, Expense
            ModelClass = Expense if record_type == "expense" else Income
            
            # Lock record before update
            record = db.query(ModelClass).filter(ModelClass.transaction_id == transaction_id).with_for_update().first()
            if not record:
                raise ValueError(f"Transaction {transaction_id} not found in database.")
                
            # Prevent duplicate file upload
            if getattr(record, 'file_uploaded', False):
                return
                
            record_id = record.id
            db.commit()
        except Exception as e:
            db.rollback()
            raise e
        finally:
            db.close()

        from services.slack_downloader import download_slack_file
        file_bytes, mime_type = download_slack_file(file_url)
        
        result = process_upload(
            record_id=record_id,
            transaction_id=transaction_id,
            file_bytes=file_bytes,
            mime_type=mime_type,
            file_index=1,
            record_type=record_type,
            submitted_by_id=user_id,
            submitted_by_name="Slack User"
        )
        
        drive_link = result.get("drive_link")
        
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"✅ File successfully processed for *{transaction_id}*.\\n<{drive_link}|View in Google Drive>"
                }
            }
        ]
        
        slack_app.client.chat_postMessage(
            channel=user_id,
            blocks=blocks,
            text=f"File processed for {transaction_id}"
        )

    except Exception as e:
        logger.error(f"PIPELINE FAILED: {str(e)}")
        if user_id:
            slack_app.client.chat_postMessage(channel=user_id, text=f"❌ Upload failed: {str(e)}")
            
        if transaction_id:
            db = SessionLocal()
            try:
                record_type = "expense" if "EXP" in transaction_id else "income"
                from models import Income, Expense
                ModelClass = Expense if record_type == "expense" else Income
                
                from sqlalchemy.sql import text as sql_text
                table_name = "expenses" if record_type == "expense" else "incomes"
                query = sql_text(f"UPDATE {table_name} SET status = 'FAILED', error_message = :err, updated_at = NOW() WHERE transaction_id = :txn")
                db.execute(query, {"err": str(e), "txn": transaction_id})
                db.commit()
            except Exception:
                db.rollback()
            finally:
                db.close()

def handle_other_events(body_bytes: bytes):
    try:
        import asyncio
        from fastapi import Request
        
        async def mock_receive():
            return {"type": "http.request", "body": body_bytes}
            
        mock_scope = {
            "type": "http",
            "method": "POST",
            "headers": [(b"content-type", b"application/json")],
        }
        mock_req = Request(mock_scope, mock_receive)
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(handler.handle(mock_req))
        loop.close()
    except Exception as e:
        logger.error(f"Error handling other events: {e}")

@app.post("/slack/events")
async def slack_events(request: Request):
    body_bytes = await request.body()
    try:
        payload = json.loads(body_bytes)
    except Exception:
        payload = {}

    if payload.get("type") == "url_verification":
        return {"challenge": payload.get("challenge")}

    event_id = payload.get("event_id")

    if is_event_processed(event_id):
        return {"status": "duplicate ignored"}

    mark_event_processed(event_id)

    event = payload.get("event", {})

    if event.get("subtype") == "file_share":
        import threading
        threading.Thread(
            target=process_slack_file_event,
            args=(event,),
            daemon=True
        ).start()
        return {"status": "ok"}
        
    if event.get("files") and len(event.get("files", [])) > 0:
        import threading
        threading.Thread(
            target=process_slack_file_event,
            args=(event,),
            daemon=True
        ).start()
        return {"status": "ok"}

    import threading
    threading.Thread(
        target=handle_other_events,
        args=(body_bytes,),
        daemon=True
    ).start()
    return {"status": "ok"}

'''

    new_content = content[:match.start(1)] + new_code + '\n' + content[match.start(2):]
    with open('main.py', 'w', encoding='utf-8') as f:
        f.write(new_content)
    print("main.py updated successfully!")


def fix_upload_pipeline():
    with open('services/upload_pipeline.py', 'r', encoding='utf-8') as f:
        content = f.read()

    # Modify process_upload to use RETURNING for ATOMIC updates
    
    # Actually, the user asked for Atomic DB Update using RETURNING. Let's do that cleanly.
    pass

if __name__ == "__main__":
    fix_main_py()
