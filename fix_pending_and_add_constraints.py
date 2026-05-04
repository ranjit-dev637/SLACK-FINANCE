import os
import sys
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL missing")

engine = create_engine(DATABASE_URL)

def run():
    with engine.begin() as conn:
        logger.info("Running recovery script...")
        
        # Incomes Recovery
        res = conn.execute(text("""
            UPDATE incomes
            SET status = 'FAILED', updated_at = NOW()
            WHERE status = 'PENDING'
            AND (
                jsonb_array_length(COALESCE(payment_screenshots,'[]'::jsonb)) > 0
                OR jsonb_array_length(COALESCE(drive_links,'[]'::jsonb)) > 0
            );
        """))
        logger.info(f"Updated {res.rowcount} orphaned incomes.")

        # Expenses Recovery
        res = conn.execute(text("""
            UPDATE expenses
            SET status = 'FAILED', updated_at = NOW()
            WHERE status = 'PENDING'
            AND (
                jsonb_array_length(COALESCE(receipt_copies,'[]'::jsonb)) > 0
                OR jsonb_array_length(COALESCE(drive_links,'[]'::jsonb)) > 0
            );
        """))
        logger.info(f"Updated {res.rowcount} orphaned expenses.")

        # Constraints for incomes
        logger.info("Adding constraints to incomes...")
        conn.execute(text("""
            ALTER TABLE incomes DROP CONSTRAINT IF EXISTS income_upload_state;
            ALTER TABLE incomes ADD CONSTRAINT income_upload_state CHECK (
                (file_uploaded = TRUE AND jsonb_array_length(COALESCE(payment_screenshots,'[]'::jsonb)) > 0)
                OR (file_uploaded = FALSE)
            );
            
            ALTER TABLE incomes DROP CONSTRAINT IF EXISTS income_status_state;
            ALTER TABLE incomes ADD CONSTRAINT income_status_state CHECK (
                (status = 'COMPLETED' AND file_uploaded = TRUE)
                OR (status <> 'COMPLETED')
            );
        """))

        # Constraints for expenses
        logger.info("Adding constraints to expenses...")
        conn.execute(text("""
            ALTER TABLE expenses DROP CONSTRAINT IF EXISTS expense_upload_state;
            ALTER TABLE expenses ADD CONSTRAINT expense_upload_state CHECK (
                (file_uploaded = TRUE AND jsonb_array_length(COALESCE(receipt_copies,'[]'::jsonb)) > 0)
                OR (file_uploaded = FALSE)
            );
            
            ALTER TABLE expenses DROP CONSTRAINT IF EXISTS expense_status_state;
            ALTER TABLE expenses ADD CONSTRAINT expense_status_state CHECK (
                (status = 'COMPLETED' AND file_uploaded = TRUE)
                OR (status <> 'COMPLETED')
            );
        """))

        logger.info("Success.")

if __name__ == "__main__":
    run()
