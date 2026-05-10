"""
worker.py — Background worker that polls the upload_jobs table
and processes queued upload jobs in dedicated threads.

Start with:
    from worker import start_worker
    start_worker()

The worker loop runs forever in a daemon thread:
  - Every 10 s: claim the next QUEUED job and process it
  - Every 60 s: requeue stuck PROCESSING jobs
"""

import threading
import time
import logging

from job_queue import claim_job, complete_job, fail_job, requeue_stuck_jobs
from services.slack_downloader import download_slack_file
from services.upload_pipeline import process_upload
from database import SessionLocal
from models import Income, Expense

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. Process a single job
# ---------------------------------------------------------------------------

def process_job(job: dict) -> None:
    """
    Download the Slack file, determine record type, run the upload
    pipeline, and mark the job as COMPLETED or FAILED.
    """
    transaction_id = job["transaction_id"]
    job_id = job["id"]

    # Lazy import to avoid circular dependency at module load time.
    # slack_app is created in main.py; by the time a job runs the app
    # is fully initialised.
    from main import slack_app

    try:
        logger.info("WORKER processing job=%s txn=%s", job_id, transaction_id)

        # ── Step 1: Download file from Slack ─────────────────────────────
        file_bytes, mime_type = download_slack_file(job["file_url"])

        # ── Step 2: Determine record type & fetch record_id ──────────────
        db = SessionLocal()
        try:
            record = db.query(Income).filter(
                Income.transaction_id == transaction_id
            ).first()
            if record:
                record_type = "income"
                record_id = record.id
                submitted_by_id = record.submitted_by_id or ""
                submitted_by_name = record.submitted_by_name or ""
            else:
                record = db.query(Expense).filter(
                    Expense.transaction_id == transaction_id
                ).first()
                if record:
                    record_type = "expense"
                    record_id = record.id
                    submitted_by_id = record.submitted_by_id or ""
                    submitted_by_name = record.submitted_by_name or ""
                else:
                    raise RuntimeError(
                        f"No Income or Expense record found for txn={transaction_id}"
                    )
        finally:
            db.close()

        # ── Step 3: Run the upload pipeline ──────────────────────────────
        result = process_upload(
            record_id=record_id,
            transaction_id=transaction_id,
            file_bytes=file_bytes,
            mime_type=mime_type,
            file_index=0,
            record_type=record_type,
            submitted_by_id=submitted_by_id,
            submitted_by_name=submitted_by_name,
        )

        drive_link = result.get("drive_link", "")

        # ── Step 4: Mark job as COMPLETED ────────────────────────────────
        complete_job(job_id, drive_link)

        # ── Step 5: Notify user on Slack ─────────────────────────────────
        success_text = (
            "✅ Submission Complete\n"
            f"Transaction ID: `{transaction_id}`\n"
            "📂 Google Drive: UPLOADED\n"
            "🗄 Supabase: UPDATED\n"
            "Status: COMPLETED ✅"
        )
        try:
            slack_app.client.chat_postMessage(
                channel=job["channel_id"],
                text=success_text,
            )
        except Exception as slack_err:
            logger.error(
                "WORKER slack_notify_failed | job=%s | error=%s",
                job_id, slack_err,
            )

        logger.info("WORKER job COMPLETED | job=%s txn=%s", job_id, transaction_id)

    except Exception as exc:
        error_msg = str(exc)
        logger.error(
            "WORKER job FAILED | job=%s txn=%s | error=%s",
            job_id, transaction_id, error_msg,
        )

        # Record the failure (may transition to DEAD if max attempts reached)
        fail_job(job_id, error_msg)

        # Check if the job is now DEAD
        _check_dead(job_id, transaction_id, job["channel_id"], slack_app)


def _check_dead(job_id: int, transaction_id: str, channel_id: str, slack_app) -> None:
    """If the job status is DEAD, send a final failure notice to the user."""
    db = SessionLocal()
    try:
        from models import UploadJob
        job_row = db.query(UploadJob).filter(UploadJob.id == job_id).first()
        if job_row and job_row.status == "DEAD":
            dead_text = (
                "❌ Upload failed after multiple attempts.\n"
                f"Transaction ID: `{transaction_id}`\n"
                "Please contact support."
            )
            try:
                slack_app.client.chat_postMessage(
                    channel=channel_id,
                    text=dead_text,
                )
            except Exception as slack_err:
                logger.error(
                    "WORKER dead_notify_failed | job=%s | error=%s",
                    job_id, slack_err,
                )
    except Exception as exc:
        logger.error("WORKER _check_dead error | job=%s | error=%s", job_id, exc)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# 2. Worker loop
# ---------------------------------------------------------------------------

def worker_loop() -> None:
    """
    Poll the job queue every 10 seconds.  Requeue stuck jobs every 60 seconds.
    Never crashes — all exceptions are caught and logged.
    """
    logger.info("WORKER LOOP RUNNING")
    requeue_counter = 0  # counts 10-second ticks; requeue every 6 ticks (60 s)

    while True:
        try:
            # ── Claim the next available job ─────────────────────────────
            job = claim_job()
            if job:
                logger.info("WORKER dispatching job=%s txn=%s",
                            job["id"], job["transaction_id"])
                threading.Thread(
                    target=process_job,
                    args=(job,),
                    daemon=True,
                ).start()

            # ── Periodically requeue stuck jobs (every ~60 s) ────────────
            requeue_counter += 1
            if requeue_counter >= 6:
                requeue_counter = 0
                try:
                    requeue_stuck_jobs()
                except Exception as rq_err:
                    logger.error("WORKER requeue_stuck error: %s", rq_err)

        except Exception as loop_err:
            logger.error("WORKER loop error (will retry): %s", loop_err)

        time.sleep(10)


# ---------------------------------------------------------------------------
# 3. Start the worker
# ---------------------------------------------------------------------------

def start_worker() -> None:
    """Launch the background worker loop in a daemon thread."""
    t = threading.Thread(target=worker_loop, daemon=True)
    t.start()
    logger.info("BACKGROUND WORKER STARTED")
