"""
migrate_pipeline_columns.py
────────────────────────────────────────────────────────────────────────────
Adds the two new pipeline-tracking columns to incomes and expenses tables:
  - updated_at    TIMESTAMPTZ  (tracks last DB mutation by the pipeline)
  - error_message TEXT         (stores failure reason when status = 'FAILED')

Safe to run multiple times — uses IF NOT EXISTS guards.
"""
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(), override=True)

import os
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise EnvironmentError("DATABASE_URL is not set in .env")

MIGRATIONS = [
    # incomes table
    "ALTER TABLE incomes ADD COLUMN IF NOT EXISTS updated_at    TIMESTAMPTZ;",
    "ALTER TABLE incomes ADD COLUMN IF NOT EXISTS error_message TEXT;",
    # expenses table
    "ALTER TABLE expenses ADD COLUMN IF NOT EXISTS updated_at    TIMESTAMPTZ;",
    "ALTER TABLE expenses ADD COLUMN IF NOT EXISTS error_message TEXT;",
]

def run():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    cur = conn.cursor()
    for sql in MIGRATIONS:
        print(f"Running: {sql.strip()}")
        cur.execute(sql)
        print("  [OK]")
    cur.close()
    conn.close()
    print("\nMigration complete.")

if __name__ == "__main__":
    run()
