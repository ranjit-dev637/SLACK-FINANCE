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
from datetime import datetime, date

import schedule
import pytz

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

        # ── Step 6: Admin notification to #transactions-log ──────────────
        ADMIN_CHANNEL = "C0B2QQGRQTF"
        try:
            db = SessionLocal()
            try:
                income = db.query(Income).filter(Income.transaction_id == transaction_id).first()
                expense = db.query(Expense).filter(Expense.transaction_id == transaction_id).first()
                record = income or expense
                record_type_label = "Income" if income else "Expense"
            finally:
                db.close()

            admin_message = (
                f"🔔 *New Transaction Completed*\n\n"
                f"*Type:* {record_type_label}\n"
                f"*Transaction ID:* `{transaction_id}`\n"
                f"*Submitted by:* <@{job['user_id']}>\n"
                f"*Status:* COMPLETED ✅\n"
                f"*Drive Link:* {result.get('drive_link', 'N/A')}\n"
                f"*Time:* {datetime.now().strftime('%d %b %Y, %I:%M %p')}"
            )

            slack_app.client.chat_postMessage(
                channel=ADMIN_CHANNEL,
                text=admin_message,
            )
            logger.info(f"ADMIN NOTIFICATION SENT for {transaction_id}")
        except Exception as admin_err:
            logger.error(f"ADMIN NOTIFICATION FAILED: {admin_err}", exc_info=True)

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
                    count = requeue_stuck_jobs()
                    if count > 0:
                        logger.info("WORKER requeued %d stuck jobs", count)
                except Exception as rq_err:
                    logger.error("WORKER requeue_stuck error: %s", rq_err)
        except Exception as loop_err:
            logger.error("WORKER loop error (will retry): %s", loop_err)

        time.sleep(10)


# ---------------------------------------------------------------------------
# 3. Daily summary
# ---------------------------------------------------------------------------

ADMIN_CHANNEL = "C0B2QQGRQTF"
IST = pytz.timezone("Asia/Kolkata")


def send_daily_summary() -> None:
    """
    Build and send a daily finance summary to the admin channel.
    Covers all Income and Expense records submitted today (IST).
    """
    from main import slack_app
    from sqlalchemy import func, cast, Date as SADate

    today = datetime.now(IST).date()
    logger.info("DAILY SUMMARY generating for %s", today)

    db = SessionLocal()
    try:
        # ── Completed counts ─────────────────────────────────────────────
        completed_incomes = (
            db.query(Income)
            .filter(
                cast(Income.submitted_at, SADate) == today,
                Income.status == "COMPLETED",
            )
            .all()
        )
        completed_expenses = (
            db.query(Expense)
            .filter(
                cast(Expense.submitted_at, SADate) == today,
                Expense.status == "COMPLETED",
            )
            .all()
        )
        completed_count = len(completed_incomes) + len(completed_expenses)

        # ── Pending counts ───────────────────────────────────────────────
        pending_income_count = (
            db.query(func.count(Income.id))
            .filter(
                cast(Income.submitted_at, SADate) == today,
                Income.status == "PENDING",
            )
            .scalar() or 0
        )
        pending_expense_count = (
            db.query(func.count(Expense.id))
            .filter(
                cast(Expense.submitted_at, SADate) == today,
                Expense.status == "PENDING",
            )
            .scalar() or 0
        )
        pending_count = pending_income_count + pending_expense_count

        # ── Failed counts ────────────────────────────────────────────────
        failed_income_count = (
            db.query(func.count(Income.id))
            .filter(
                cast(Income.submitted_at, SADate) == today,
                Income.status.in_(["FAILED", "DEAD"]),
            )
            .scalar() or 0
        )
        failed_expense_count = (
            db.query(func.count(Expense.id))
            .filter(
                cast(Expense.submitted_at, SADate) == today,
                Expense.status.in_(["FAILED", "DEAD"]),
            )
            .scalar() or 0
        )
        failed_count = failed_income_count + failed_expense_count

        # ── Totals ───────────────────────────────────────────────────────
        total_income = sum(
            (r.room_amount or 0) + (r.food_amount or 0)
            for r in completed_incomes
        )
        total_expense = sum(
            (r.total_amount or 0)
            for r in completed_expenses
        )

        # ── Transaction detail lines ─────────────────────────────────────
        detail_lines = []
        for r in completed_incomes:
            amt = (r.room_amount or 0) + (r.food_amount or 0)
            detail_lines.append(f"  💰 `{r.transaction_id}` — Income ₹{amt:,.2f}")
        for r in completed_expenses:
            detail_lines.append(f"  💸 `{r.transaction_id}` — Expense ₹{(r.total_amount or 0):,.2f}")

        details_block = "\n".join(detail_lines) if detail_lines else "  _No completed transactions today._"

        # ── Build message ────────────────────────────────────────────────
        summary = (
            f"📊 *Daily Finance Summary — {today.strftime('%d %b %Y')}*\n\n"
            f"*Transactions Today:*\n"
            f"✅ Completed: {completed_count}\n"
            f"⏳ Pending: {pending_count}\n"
            f"❌ Failed: {failed_count}\n\n"
            f"💰 *Total Income:* ₹{total_income:,.2f}\n"
            f"💸 *Total Expense:* ₹{total_expense:,.2f}\n\n"
            f"*Transaction Details:*\n{details_block}\n\n"
            f"_Summary generated at 10:00 PM IST_"
        )

        slack_app.client.chat_postMessage(
            channel=ADMIN_CHANNEL,
            text=summary,
        )
        logger.info("DAILY SUMMARY SENT for %s", today)

    except Exception as e:
        logger.error("DAILY SUMMARY FAILED: %s", e, exc_info=True)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# 4. Scheduler loop
# ---------------------------------------------------------------------------

def run_scheduler() -> None:
    """
    Run the schedule library in a loop, checking every 60 seconds.
    The daily summary is scheduled at 22:00 IST (16:30 UTC).
    """
    # schedule library works in the process-local time, so convert
    # 22:00 IST to UTC for a reliable trigger.
    ist_10pm = datetime.now(IST).replace(hour=22, minute=0, second=0)
    utc_time = ist_10pm.astimezone(pytz.utc).strftime("%H:%M")

    schedule.every().day.at(utc_time).do(send_daily_summary)
    logger.info("SCHEDULER: daily summary set for %s UTC (22:00 IST)", utc_time)

    while True:
        try:
            schedule.run_pending()
        except Exception as e:
            logger.error("SCHEDULER error: %s", e, exc_info=True)
        time.sleep(60)


# ---------------------------------------------------------------------------
# 5. Start the worker
# ---------------------------------------------------------------------------

def start_worker() -> None:
    """Launch the background worker loop and daily summary scheduler."""
    t = threading.Thread(target=worker_loop, daemon=True)
    t.start()
    logger.info("BACKGROUND WORKER STARTED")

    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    logger.info("DAILY SUMMARY SCHEDULER STARTED")
