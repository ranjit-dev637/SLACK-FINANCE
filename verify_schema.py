"""
verify_schema.py  —  Post-migration schema validation
Confirms constraints, indexes, and upload_logs table exist.
"""
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(), override=True)
import os, psycopg2, sys

conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur  = conn.cursor()
failures = 0

def check(label, query, *args):
    global failures
    cur.execute(query, args)
    row = cur.fetchone()
    ok  = bool(row and row[0])
    tag = "PASS" if ok else "FAIL"
    print(f"  [{tag}] {label}")
    if not ok:
        failures += 1
    return ok

print("\n=== SCHEMA VERIFICATION ===\n")

# Constraints
check("UNIQUE uq_incomes_transaction_id",
      "SELECT 1 FROM pg_constraint WHERE conname=%s", "uq_incomes_transaction_id")
check("UNIQUE uq_expenses_transaction_id",
      "SELECT 1 FROM pg_constraint WHERE conname=%s", "uq_expenses_transaction_id")
check("CHECK  chk_incomes_valid_status",
      "SELECT 1 FROM pg_constraint WHERE conname=%s", "chk_incomes_valid_status")
check("CHECK  chk_expenses_valid_status",
      "SELECT 1 FROM pg_constraint WHERE conname=%s", "chk_expenses_valid_status")
check("CHECK  chk_incomes_completed_has_screenshot",
      "SELECT 1 FROM pg_constraint WHERE conname=%s", "chk_incomes_completed_has_screenshot")
check("CHECK  chk_expenses_completed_has_receipt",
      "SELECT 1 FROM pg_constraint WHERE conname=%s", "chk_expenses_completed_has_receipt")

# Indexes
for idx in [
    "idx_incomes_transaction_id", "idx_incomes_status",
    "idx_expenses_transaction_id", "idx_expenses_status",
    "idx_upload_logs_transaction_id", "idx_upload_logs_created_at",
]:
    check(f"INDEX  {idx}",
          "SELECT 1 FROM pg_indexes WHERE indexname=%s", idx)

# Table
check("TABLE  upload_logs exists",
      "SELECT 1 FROM information_schema.tables WHERE table_name=%s", "upload_logs")

# upload_logs columns
for col in ["id","transaction_id","status","message","record_type","created_at"]:
    check(f"COLUMN upload_logs.{col}",
          "SELECT 1 FROM information_schema.columns WHERE table_name='upload_logs' AND column_name=%s", col)

cur.close(); conn.close()

print(f"\n{'All checks passed!' if failures == 0 else f'{failures} check(s) FAILED'}")
sys.exit(failures)
