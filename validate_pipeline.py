import os
import uuid
from datetime import datetime, timezone
import unittest.mock as mock

from database import SessionLocal
from models import Income
from services.upload_pipeline import process_upload
from services.supabase_storage import _supabase

def check_file_exists_in_supabase(file_path: str) -> bool:
    try:
        res = _supabase.storage.from_("receipts").list()
        for item in res:
            if item["name"] == file_path:
                return True
        return False
    except Exception:
        return False

def create_dummy_record():
    """Creates a test income row and returns (transaction_id, integer_pk)."""
    db = SessionLocal()
    txn_id = f"TEST-{uuid.uuid4().hex[:8]}"
    inc = Income(
        transaction_id=txn_id,
        user_id="U_TEST_VAL",
        status="PENDING",
        name="Validation Test",
        booking_number="0",
        contact_number="0000000000",
        room_amount=0.0,
        food_amount=0.0,
        payment_type="Cash",
        receipt_by="Test",
        submitted_by="U_TEST_VAL",
        submitted_by_id="U_TEST_VAL",
        submitted_by_name="Test Bot",
        submitted_at=datetime.now(timezone.utc),
    )
    db.add(inc)
    db.commit()
    db.refresh(inc)
    pk = inc.id   # capture integer PK BEFORE closing session
    db.close()
    return txn_id, pk

def run_validation():
    print("=== STARTING PIPELINE VALIDATION ===")
    
    # ── 1. Test Successful Upload ─────────────────────────────────────────────
    txn_id, record_pk = create_dummy_record()
    print(f"\n[Test 1] Successful Upload (txn: {txn_id} | pk: {record_pk})")
    
    try:
        res1 = process_upload(
            record_id=record_pk,
            transaction_id=txn_id,
            file_bytes=b"test file content 1",
            mime_type="image/jpeg",
            file_index=1,
            record_type="income",
            submitted_by_id="U_VALID1",
            submitted_by_name="Validator One"
        )
        print("Upload completed without exceptions.")
    except Exception as e:
        print(f"Upload failed unexpectedly: {e}")
        return

    db = SessionLocal()
    inc = db.query(Income).filter_by(id=record_pk).first()
    
    print("  - payment_screenshot NOT NULL:", inc.payment_screenshot is not None)
    print(f"  - payment_screenshots array length: {len(inc.payment_screenshots) if inc.payment_screenshots else 0} (expected 1)")
    print(f"  - drive_links array length: {len(inc.drive_links) if inc.drive_links else 0} (expected 1)")
    print("  - file_uploaded:", inc.file_uploaded, "(expected True)")
    print("  - status:", inc.status, "(expected COMPLETED)")
    print("  - submitted_by_id:", inc.submitted_by_id, "(expected U_VALID1)")
    print("  - submitted_by_name:", inc.submitted_by_name, "(expected Validator One)")
    db.close()

    # ── 2. Test Multiple Uploads & Latest File ────────────────────────────────
    print(f"\n[Test 2] Multiple Uploads (same txn: {txn_id})")
    try:
        res2 = process_upload(
            record_id=record_pk,
            transaction_id=txn_id,
            file_bytes=b"test file content 2",
            mime_type="image/jpeg",
            file_index=2,
            record_type="income",
            submitted_by_id="U_VALID1",
            submitted_by_name="Validator One"
        )
    except Exception as e:
        print(f"Upload 2 failed unexpectedly: {e}")

    db = SessionLocal()
    inc = db.query(Income).filter_by(id=record_pk).first()
    print(f"  - payment_screenshots array length: {len(inc.payment_screenshots) if inc.payment_screenshots else 0} (expected 2)")
    print(f"  - payment_screenshot equals latest URL:", inc.payment_screenshot == res2["file_url"])
    db.close()

    # ── 3. Test Retry / Idempotency (Same URL) ────────────────────────────────
    print(f"\n[Test 3] Idempotency (retry same file index 2)")
    try:
        res3 = process_upload(
            record_id=record_pk,
            transaction_id=txn_id,
            file_bytes=b"test file content 2",  # same content, same index -> same path
            mime_type="image/jpeg",
            file_index=2,  # Duplicate index simulates retry
            record_type="income",
            submitted_by_id="U_VALID1",
            submitted_by_name="Validator One"
        )
    except Exception as e:
        print(f"Retry failed unexpectedly: {e}")

    db = SessionLocal()
    inc = db.query(Income).filter_by(id=record_pk).first()
    print(f"  - payment_screenshots array length: {len(inc.payment_screenshots) if inc.payment_screenshots else 0} (expected 2, NO DUPLICATES)")
    db.close()

    # ── 4. Test Forced Failure & Rollback ─────────────────────────────────────
    txn_id_fail, fail_pk = create_dummy_record()
    print(f"\n[Test 4] Forced Failure & Rollback (txn: {txn_id_fail} | pk: {fail_pk})")
    
    def mock_drive_upload(*args, **kwargs):
        raise RuntimeError("Simulated Google Drive API Failure")

    # Mock only the drive upload to trigger failure AFTER supabase upload
    with mock.patch('services.upload_pipeline.upload_to_drive', side_effect=mock_drive_upload):
        try:
            process_upload(
                record_id=fail_pk,
                transaction_id=txn_id_fail,
                file_bytes=b"fail content",
                mime_type="image/jpeg",
                file_index=1,
                record_type="income",
                submitted_by_id="U_VALID2",
                submitted_by_name="Validator Two"
            )
            print("  ERROR: Process did not raise exception on failure!")
        except Exception as e:
            print("  - Pipeline successfully aborted with exception:", repr(e))

    db = SessionLocal()
    inc_fail = db.query(Income).filter_by(id=fail_pk).first()
    print("  - status:", inc_fail.status, "(expected FAILED)")
    print("  - file_uploaded:", inc_fail.file_uploaded, "(expected False)")
    print("  - error_message contains 'Drive':", "Drive" in str(inc_fail.error_message))
    db.close()

    # Verify Supabase storage deletion
    file_path = f"{txn_id_fail}_1.jpg"
    is_orphan = check_file_exists_in_supabase(file_path)
    print("  - Storage file deleted (no orphan):", not is_orphan)
    
    print("\n=== VALIDATION COMPLETE ===")

if __name__ == "__main__":
    run_validation()
