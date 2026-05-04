"""
migrate_hardening.py
────────────────────────────────────────────────────────────────────────────
Applies schema hardening for the file upload pipeline:
  - Add submitted_by_id and submitted_by_name columns.
  - Add CHECK constraint: status != 'COMPLETED' OR file_uploaded = TRUE.
  - Add CHECK constraint: file_uploaded = FALSE OR jsonb arrays are not empty.
"""
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(), override=True)

import os
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise EnvironmentError("DATABASE_URL is not set in .env")

MIGRATIONS = [
    # 1. Add columns to incomes
    "ALTER TABLE incomes ADD COLUMN IF NOT EXISTS submitted_by_id TEXT;",
    "ALTER TABLE incomes ADD COLUMN IF NOT EXISTS submitted_by_name TEXT;",
    
    # 2. Add columns to expenses
    "ALTER TABLE expenses ADD COLUMN IF NOT EXISTS submitted_by_id TEXT;",
    "ALTER TABLE expenses ADD COLUMN IF NOT EXISTS submitted_by_name TEXT;",

    # 3. Cleanup existing inconsistent data before applying constraints
    "UPDATE incomes SET file_uploaded = TRUE WHERE status = 'COMPLETED' AND jsonb_array_length(COALESCE(payment_screenshots, '[]'::jsonb)) > 0;",
    "UPDATE incomes SET status = 'PENDING' WHERE status = 'COMPLETED' AND jsonb_array_length(COALESCE(payment_screenshots, '[]'::jsonb)) = 0;",
    "UPDATE expenses SET file_uploaded = TRUE WHERE status = 'COMPLETED' AND jsonb_array_length(COALESCE(receipt_copies, '[]'::jsonb)) > 0;",
    "UPDATE expenses SET status = 'PENDING' WHERE status = 'COMPLETED' AND jsonb_array_length(COALESCE(receipt_copies, '[]'::jsonb)) = 0;",
    "UPDATE incomes SET file_uploaded = FALSE WHERE file_uploaded = TRUE AND jsonb_array_length(COALESCE(payment_screenshots, '[]'::jsonb)) = 0;",
    "UPDATE expenses SET file_uploaded = FALSE WHERE file_uploaded = TRUE AND jsonb_array_length(COALESCE(receipt_copies, '[]'::jsonb)) = 0;",

    # 4. Add CHECK constraints to incomes (with IF NOT EXISTS via PL/pgSQL)
    """
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_incomes_completed_file') THEN
            ALTER TABLE incomes ADD CONSTRAINT chk_incomes_completed_file
            CHECK (status != 'COMPLETED' OR file_uploaded = TRUE);
        END IF;
    END $$;
    """,
    """
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_incomes_file_uploaded_array') THEN
            ALTER TABLE incomes ADD CONSTRAINT chk_incomes_file_uploaded_array
            CHECK (file_uploaded = FALSE OR jsonb_array_length(COALESCE(payment_screenshots, '[]'::jsonb)) > 0);
        END IF;
    END $$;
    """,

    # 4. Add CHECK constraints to expenses
    """
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_expenses_completed_file') THEN
            ALTER TABLE expenses ADD CONSTRAINT chk_expenses_completed_file
            CHECK (status != 'COMPLETED' OR file_uploaded = TRUE);
        END IF;
    END $$;
    """,
    """
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_expenses_file_uploaded_array') THEN
            ALTER TABLE expenses ADD CONSTRAINT chk_expenses_file_uploaded_array
            CHECK (file_uploaded = FALSE OR jsonb_array_length(COALESCE(receipt_copies, '[]'::jsonb)) > 0);
        END IF;
    END $$;
    """
]

def run():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    cur = conn.cursor()
    for sql in MIGRATIONS:
        print(f"Running migration block...")
        cur.execute(sql)
        print("  [OK]")
    cur.close()
    conn.close()
    print("\nMigration complete.")

if __name__ == "__main__":
    run()
