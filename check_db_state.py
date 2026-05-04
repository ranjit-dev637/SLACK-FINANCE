from database import SessionLocal
from sqlalchemy import text

db = SessionLocal()
rows = db.execute(text(
    "SELECT transaction_id, status, payment_screenshot IS NULL as ps_null, drive_links "
    "FROM incomes WHERE transaction_id LIKE 'TEST-%' ORDER BY id DESC LIMIT 10"
)).fetchall()
print("--- FINAL DB STATE (last 10 TEST- rows) ---")
for r in rows:
    txn, status, ps_null, dl = r
    print(f"  txn={txn}  status={status}  screenshot_null={ps_null}  drive_links={dl}")
db.close()
