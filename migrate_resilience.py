"""
migrate_resilience.py
────────────────────────────────────────────────────────────────────────────
Idempotent schema hardening for the upload pipeline.

Applies:
  1.  UNIQUE constraint on transaction_id (both tables)
  2.  CHECK: status must be PENDING | COMPLETED | FAILED
  3.  CHECK: COMPLETED rows must have a non-NULL payment_screenshot / receipt_copy
  4.  Indexes on transaction_id and status for fast lookups
  5.  upload_logs audit table

Safe to run multiple times — every statement is guarded with IF NOT EXISTS
or DO $$ ... IF NOT EXISTS ... $$ blocks.

Run:
    python migrate_resilience.py
"""

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(), override=True)

import os
import sys
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    sys.exit("ERROR: DATABASE_URL is not set in .env")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _constraint_block(constraint_name: str, table: str, definition: str) -> str:
    """Return a DO $$ block that adds a constraint only if it doesn't already exist."""
    return f"""
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = '{constraint_name}'
    ) THEN
        ALTER TABLE {table} ADD CONSTRAINT {constraint_name} {definition};
        RAISE NOTICE 'Constraint {constraint_name} created.';
    ELSE
        RAISE NOTICE 'Constraint {constraint_name} already exists — skipped.';
    END IF;
END $$;
"""


def _index_block(index_name: str, table: str, column: str) -> str:
    return f"CREATE INDEX IF NOT EXISTS {index_name} ON {table}({column});"


# ─────────────────────────────────────────────────────────────────────────────
# Data cleanup — fix any rows that would violate the new constraints
# ─────────────────────────────────────────────────────────────────────────────

CLEANUP = [
    # Ensure status values are valid before adding the CHECK constraint
    "UPDATE incomes  SET status = 'PENDING' WHERE status NOT IN ('PENDING','COMPLETED','FAILED');",
    "UPDATE expenses SET status = 'PENDING' WHERE status NOT IN ('PENDING','COMPLETED','FAILED');",

    # Ensure COMPLETED rows that are missing a screenshot are demoted to PENDING
    # (only rows that have no screenshot at all — Razorpay rows have no screenshot by design,
    #  but they are also COMPLETED; we do NOT touch them here — the constraint below
    #  uses payment_screenshot IS NOT NULL only for rows where file_uploaded = TRUE)
    """
    UPDATE incomes
    SET    status = 'PENDING'
    WHERE  status = 'COMPLETED'
      AND  file_uploaded = TRUE
      AND  payment_screenshot IS NULL;
    """,
    """
    UPDATE expenses
    SET    status = 'PENDING'
    WHERE  status = 'COMPLETED'
      AND  file_uploaded = TRUE
      AND  receipt_copy IS NULL;
    """,
]


# ─────────────────────────────────────────────────────────────────────────────
# Migration steps
# ─────────────────────────────────────────────────────────────────────────────

MIGRATIONS = [

    # ── Step 1: UNIQUE(transaction_id) ────────────────────────────────────────
    # The ORM already declares unique=True, but enforce it at DB level too.
    _constraint_block(
        "uq_incomes_transaction_id",
        "incomes",
        "UNIQUE (transaction_id)",
    ),
    _constraint_block(
        "uq_expenses_transaction_id",
        "expenses",
        "UNIQUE (transaction_id)",
    ),

    # ── Step 2: Valid status values ───────────────────────────────────────────
    _constraint_block(
        "chk_incomes_valid_status",
        "incomes",
        "CHECK (status IN ('PENDING', 'COMPLETED', 'FAILED'))",
    ),
    _constraint_block(
        "chk_expenses_valid_status",
        "expenses",
        "CHECK (status IN ('PENDING', 'COMPLETED', 'FAILED'))",
    ),

    # ── Step 3: COMPLETED + file_uploaded rows must have a screenshot ─────────
    # Scoped to file_uploaded=TRUE rows so Razorpay auto-rows (no file) are exempt.
    _constraint_block(
        "chk_incomes_completed_has_screenshot",
        "incomes",
        "CHECK (NOT (status = 'COMPLETED' AND file_uploaded = TRUE) OR payment_screenshot IS NOT NULL)",
    ),
    _constraint_block(
        "chk_expenses_completed_has_receipt",
        "expenses",
        "CHECK (NOT (status = 'COMPLETED' AND file_uploaded = TRUE) OR receipt_copy IS NOT NULL)",
    ),

    # ── Step 4: Indexes ───────────────────────────────────────────────────────
    _index_block("idx_incomes_transaction_id",  "incomes",  "transaction_id"),
    _index_block("idx_incomes_status",          "incomes",  "status"),
    _index_block("idx_expenses_transaction_id", "expenses", "transaction_id"),
    _index_block("idx_expenses_status",         "expenses", "status"),

    # ── Step 5: Audit log table ───────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS upload_logs (
        id             SERIAL PRIMARY KEY,
        transaction_id TEXT        NOT NULL,
        status         TEXT        NOT NULL,
        message        TEXT,
        record_type    TEXT,
        created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """,
    _index_block("idx_upload_logs_transaction_id", "upload_logs", "transaction_id"),
    _index_block("idx_upload_logs_created_at",     "upload_logs", "created_at"),
]


# ─────────────────────────────────────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────────────────────────────────────

def run() -> None:
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    cur = conn.cursor()

    print("\n=== DATA CLEANUP (pre-migration) ===")
    for sql in CLEANUP:
        label = sql.strip().split("\n")[0][:80]
        print(f"  {label}")
        cur.execute(sql)
        print("  [OK]")

    print("\n=== SCHEMA MIGRATIONS ===")
    for i, sql in enumerate(MIGRATIONS, 1):
        label = sql.strip().split("\n")[0][:80]
        print(f"  [{i:02d}] {label}")
        cur.execute(sql)
        print("       [OK]")

    cur.close()
    conn.close()
    print("\n[OK] migrate_resilience.py complete -- all steps applied.\n")


if __name__ == "__main__":
    run()
