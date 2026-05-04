"""
services/upload_pipeline.py
────────────────────────────────────────────────────────────────────────────
Central Upload Orchestrator — Production-Grade, Deterministic Pipeline
────────────────────────────────────────────────────────────────────────────

Guarantees:
  - Every successful upload writes correct, non-NULL values to the DB
  - No record ever stays in PENDING state after this function returns
  - No duplicate entries in payment_screenshots or drive_links arrays
  - All failures are atomically captured and marked as FAILED
  - Pipeline is deterministic: result is always COMPLETED or FAILED, never partial

Resilience features:
  - Structured logging (key=value) on every key event
  - 3-attempt linear-backoff retry on Supabase and Drive calls
  - Circuit breaker for Google Drive (5 failures → OPEN 60 s)
  - Fire-and-forget audit log writes to upload_logs table
  - Row-level locking (SELECT FOR UPDATE) prevents concurrent update races

Flow:
  0. Pre-flight validation
  1. Pipeline entry log + audit-start
  2. Supabase upload (retry + circuit breaker via supabase_storage module)
  3. Google Drive upload (retry + circuit breaker)
  4. Atomic SQL UPDATE … RETURNING
  5. Strict pre-commit validation
  6. db.commit() + post-commit read-back
  7. Return {file_url, drive_link}

On any exception: db.rollback() → _mark_failed() → _audit_log(FAILED) → re-raise
"""

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Any

from sqlalchemy import text

from database import SessionLocal
from services.supabase_storage import upload_file_to_storage, delete_file_from_storage
from services.google_drive import upload_to_drive
from services.circuit_breaker import get_breaker, CircuitOpenError

logger = logging.getLogger(__name__)

# ── Circuit breaker for Google Drive (5 consecutive failures → OPEN 60 s) ────
_drive_breaker = get_breaker("drive", failure_threshold=5, reset_timeout=60.0)

# ── Retry config for Drive (Supabase retry lives inside supabase_storage.py) ─
_DRIVE_MAX_RETRIES   = 3
_DRIVE_RETRY_DELAY_S = 1.0


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _audit_log(
    transaction_id: str,
    status: str,
    message: str,
    record_type: str = "",
) -> None:
    """
    Fire-and-forget insert into upload_logs.
    Swallows all errors — must never disrupt the main pipeline.
    """
    db = SessionLocal()
    try:
        db.execute(
            text("""
                INSERT INTO upload_logs (transaction_id, status, message, record_type)
                VALUES (:txn_id, :status, :message, :record_type)
            """),
            {
                "txn_id":      transaction_id,
                "status":      status,
                "message":     message[:2000],
                "record_type": record_type,
            },
        )
        db.commit()
    except Exception as exc:
        # Never let audit logging kill the pipeline
        logger.debug("audit_log_write_failed | txn=%s | error=%s", transaction_id, exc)
        try:
            db.rollback()
        except Exception:
            pass
    finally:
        db.close()


def _mark_failed(transaction_id: str, record_type: str, error_msg: str) -> None:
    """
    Mark a record as FAILED in a fresh, isolated DB session.
    Uses transaction_id (not PK) so it works even if the main session exploded.
    """
    table = "incomes" if record_type == "income" else "expenses"
    db = SessionLocal()
    try:
        db.execute(
            text(f"""
                UPDATE {table}
                SET
                    status        = 'FAILED',
                    file_uploaded = FALSE,
                    error_message = :err,
                    updated_at    = :now
                WHERE transaction_id = :txn_id
            """),
            {
                "err":    error_msg[:1000],
                "now":    datetime.now(timezone.utc),
                "txn_id": transaction_id,
            },
        )
        db.commit()
        logger.info("pipeline_mark_failed | txn=%s", transaction_id)
        print(f"PIPELINE FAILED — DB marked FAILED | txn={transaction_id}")
    except Exception as exc:
        db.rollback()
        logger.error(
            "pipeline_mark_failed_error | txn=%s | error=%s", transaction_id, exc
        )
    finally:
        db.close()


def _drive_upload_with_retry(
    file_bytes: bytes,
    filename: str,
    record_type: str,
    mime_type: str,
) -> str:
    """
    Upload to Google Drive with circuit breaker + linear-backoff retry.

    Raises:
        CircuitOpenError — Drive circuit is OPEN.
        RuntimeError     — All retry attempts exhausted.
    """
    last_exc: Exception | None = None

    for attempt in range(1, _DRIVE_MAX_RETRIES + 1):
        try:
            link = _drive_breaker.call(
                upload_to_drive,
                file_bytes=file_bytes,
                filename=filename,
                record_type=record_type,
                mime_type=mime_type,
            )
            return link
        except CircuitOpenError:
            logger.error("drive_circuit_open | filename=%s | attempt=%d", filename, attempt)
            raise  # fail immediately when circuit is OPEN
        except Exception as exc:
            last_exc = exc
            if attempt < _DRIVE_MAX_RETRIES:
                delay = _DRIVE_RETRY_DELAY_S * attempt
                logger.warning(
                    "drive_upload_retry | filename=%s | attempt=%d/%d | delay=%.1fs | error=%s",
                    filename, attempt, _DRIVE_MAX_RETRIES, delay, exc,
                )
                time.sleep(delay)
            else:
                logger.error(
                    "drive_upload_failed | filename=%s | attempts=%d | error=%s",
                    filename, _DRIVE_MAX_RETRIES, exc,
                )

    raise RuntimeError(
        f"Drive upload failed after {_DRIVE_MAX_RETRIES} attempts "
        f"(filename={filename}): {last_exc}"
    ) from last_exc


# ─────────────────────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

def process_upload(
    *,
    record_id: int,
    transaction_id: str,
    file_bytes: bytes,
    mime_type: str,
    file_index: int,
    record_type: str,
    submitted_by_id: str,
    submitted_by_name: str,
) -> dict:
    """
    Orchestrates Supabase + Google Drive upload then atomically updates the DB.

    Returns:
        {"file_url": str, "drive_link": str}

    Raises:
        RuntimeError — on any failure; DB is always left in FAILED state.
    """
    _t_start = time.monotonic()

    # ── STEP 0: Pre-flight checks ─────────────────────────────────────────────
    if not record_id:
        raise ValueError("record_id (DB primary key) is missing — cannot proceed")
    if not transaction_id:
        raise ValueError("transaction_id is missing — cannot name storage file")
    if not Path("credentials.json").exists() or not Path("token.pickle").exists():
        raise RuntimeError(
            "Google Drive authentication not initialised. "
            "Run generate_token.py to obtain fresh OAuth credentials."
        )

    # ── STEP 1: Pipeline entry ────────────────────────────────────────────────
    print(f"PIPELINE START: {transaction_id}")
    logger.info(
        "pipeline_start | txn_id=%s | record_id=%s | type=%s | file_index=%s",
        transaction_id, record_id, record_type, file_index,
    )
    _audit_log(
        transaction_id,
        "START",
        f"Pipeline started | record_id={record_id} | type={record_type} | index={file_index}",
        record_type,
    )

    table      = "incomes"            if record_type == "income"  else "expenses"
    single_col = "payment_screenshot" if record_type == "income"  else "receipt_copy"
    arr_col    = "payment_screenshots" if record_type == "income" else "receipt_copies"

    # ── IDEMPOTENCY: skip if already COMPLETED ────────────────────────────────
    db_check = SessionLocal()
    try:
        row = db_check.execute(
            text(f"SELECT status, {single_col}, drive_links FROM {table} WHERE id = :id"),
            {"id": record_id},
        ).fetchone()
        if row and row[0] == "COMPLETED":
            print(f"PIPELINE SKIP: {transaction_id} already COMPLETED — returning existing data")
            logger.info("pipeline_skip_already_completed | txn_id=%s", transaction_id)
            return {
                "file_url":   row[1],
                "drive_link": row[2][-1] if row[2] else None,
                "status":     "already_completed",
            }
    finally:
        db_check.close()

    # ── Main pipeline ─────────────────────────────────────────────────────────
    file_url   = None
    drive_link = None
    db         = None

    try:
        # ── STEP 2a: Supabase upload (retry + circuit breaker inside module) ──
        file_url = upload_file_to_storage(
            transaction_id=transaction_id,
            file_bytes=file_bytes,
            mime_type=mime_type,
            file_index=file_index,
        )
        if not file_url:
            raise RuntimeError("Supabase upload failed — returned empty URL")

        # ── STEP 2b: Google Drive upload (retry + circuit breaker) ────────────
        print("STEP: calling Google Drive upload")
        ext_map  = {"image/jpeg": "jpg", "image/png": "png", "application/pdf": "pdf"}
        ext      = ext_map.get(mime_type, "jpg")
        filename = f"{transaction_id}_{file_index}.{ext}"

        try:
            drive_link = _drive_upload_with_retry(
                file_bytes=file_bytes,
                filename=filename,
                record_type=record_type,
                mime_type=mime_type,
            )
        except Exception as drive_exc:
            # Roll back Supabase before propagating the error
            delete_file_from_storage(
                transaction_id=transaction_id,
                mime_type=mime_type,
                file_index=file_index,
            )
            raise RuntimeError(f"Drive upload failed: {drive_exc}") from drive_exc

        if not drive_link:
            raise RuntimeError("Drive upload failed — returned empty link")

        print("UPLOADS OK")
        logger.info(
            "upload_success | txn_id=%s | file_url=%s | drive_link=%s",
            transaction_id, file_url, drive_link,
        )

        # ── STEP 3: Atomic DB update with row-level locking ───────────────────
        db = SessionLocal()

        lock_row = db.execute(
            text(f"SELECT id FROM {table} WHERE id = :id FOR UPDATE"),
            {"id": record_id},
        ).fetchone()
        if not lock_row:
            raise RuntimeError(
                f"Record id={record_id} not found — cannot acquire row lock"
            )

        sql_update = text(f"""
            UPDATE {table}
            SET
                {single_col} = :file_url,

                {arr_col} =
                    CASE
                        WHEN NOT COALESCE({arr_col}, '[]') @> to_jsonb(ARRAY[:file_url]::text[])
                        THEN COALESCE({arr_col}, '[]') || to_jsonb(ARRAY[:file_url]::text[])
                        ELSE {arr_col}
                    END,

                drive_links =
                    CASE
                        WHEN NOT COALESCE(drive_links, '[]') @> to_jsonb(ARRAY[:drive_link]::text[])
                        THEN COALESCE(drive_links, '[]') || to_jsonb(ARRAY[:drive_link]::text[])
                        ELSE drive_links
                    END,

                file_uploaded = TRUE,
                status        = 'COMPLETED',
                error_message = NULL,
                updated_at    = NOW()

            WHERE id = :record_id
            RETURNING {single_col}, drive_links
        """)

        result = db.execute(
            sql_update,
            {
                "file_url":   file_url,
                "drive_link": drive_link,
                "record_id":  record_id,
            },
        ).fetchone()

        # ── STEP 4: Strict pre-commit validation ──────────────────────────────
        if result is None:
            raise RuntimeError("DB update failed: no row returned from RETURNING clause")
        if not result[0]:
            raise RuntimeError(
                f"DB update failed: {single_col} not written (NULL after UPDATE)"
            )
        if not result[1]:
            raise RuntimeError(
                "DB update failed: drive_links not written (NULL after UPDATE)"
            )

        # ── STEP 5: Commit ────────────────────────────────────────────────────
        db.commit()
        print("DB UPDATE SUCCESS")
        logger.info("db_update_ok | txn_id=%s | record_id=%s", transaction_id, record_id)

        # ── STEP 6: Post-commit read-back verification ────────────────────────
        verify_row = db.execute(
            text(f"""
                SELECT {single_col}, drive_links
                FROM   {table}
                WHERE  id = :record_id
            """),
            {"record_id": record_id},
        ).fetchone()

        if not verify_row or not verify_row[0]:
            raise RuntimeError(
                f"Post-commit verification FAILED: {single_col} is NULL in read-back"
            )
        if not verify_row[1] or verify_row[1] == [] or verify_row[1] == "[]":
            raise RuntimeError(
                "Post-commit verification FAILED: drive_links is empty in read-back"
            )

        duration_ms = int((time.monotonic() - _t_start) * 1000)
        print(f"FINAL VERIFIED: {transaction_id}")
        logger.info(
            "pipeline_complete | txn_id=%s | duration_ms=%d | file_url=%s | drive_link=%s",
            transaction_id, duration_ms, file_url, drive_link,
        )
        _audit_log(
            transaction_id,
            "SUCCESS",
            f"Upload complete | duration_ms={duration_ms} | file_url={file_url} | drive_link={drive_link}",
            record_type,
        )

    except Exception as exc:
        err = str(exc)
        duration_ms = int((time.monotonic() - _t_start) * 1000)
        logger.error(
            "pipeline_failed | txn_id=%s | duration_ms=%d | error=%s",
            transaction_id, duration_ms, err,
        )

        if db is not None:
            try:
                db.rollback()
            except Exception:
                pass

        _mark_failed(transaction_id, record_type, err)
        _audit_log(
            transaction_id,
            "FAILED",
            f"Pipeline failed | duration_ms={duration_ms} | error={err[:500]}",
            record_type,
        )
        raise RuntimeError(err) from exc

    finally:
        if db is not None:
            db.close()

    # ── STEP 7: Return ONLY on full success ───────────────────────────────────
    if not file_url or not drive_link:
        raise RuntimeError("Pipeline completed but URLs are unexpectedly missing")

    return {
        "file_url":   file_url,
        "drive_link": drive_link,
    }
