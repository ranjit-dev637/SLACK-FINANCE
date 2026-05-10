"""
job_queue.py — Persistent job queue backed by PostgreSQL via SQLAlchemy.

Manages the lifecycle of upload jobs: enqueue → claim → complete/fail,
with exponential backoff retries and stuck-job recovery.
"""

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from database import SessionLocal
from models import UploadJob

logger = logging.getLogger(__name__)

# Backoff schedule: attempt number → delay before next retry
RETRY_BACKOFF = {
    1: timedelta(seconds=30),
    2: timedelta(minutes=2),
    3: timedelta(minutes=10),
    4: timedelta(minutes=30),
}

STUCK_THRESHOLD = timedelta(minutes=5)


# ---------------------------------------------------------------------------
# 1. Enqueue
# ---------------------------------------------------------------------------

def enqueue_job(transaction_id: str, user_id: str, channel_id: str,
                file_url: str, mime_type: str) -> int | None:
    """
    Insert a new upload job or return the existing job id if file_url
    already exists (idempotent).
    """
    session = SessionLocal()
    try:
        # Check for existing job with the same file_url
        existing = (
            session.query(UploadJob)
            .filter(UploadJob.file_url == file_url)
            .first()
        )
        if existing:
            logger.info(f"JOB ALREADY EXISTS for file_url={file_url}, "
                        f"returning job id={existing.id}")
            return existing.id

        job = UploadJob(
            transaction_id=transaction_id,
            user_id=user_id,
            channel_id=channel_id,
            file_url=file_url,
            mime_type=mime_type,
            status="QUEUED",
            attempts=0,
        )
        session.add(job)
        session.commit()
        session.refresh(job)

        logger.info(f"JOB ENQUEUED: {transaction_id}")
        return job.id

    except SQLAlchemyError:
        session.rollback()
        logger.error("Failed to enqueue job", exc_info=True)
        return None
    finally:
        session.close()


# ---------------------------------------------------------------------------
# 2. Claim
# ---------------------------------------------------------------------------

def claim_job() -> dict | None:
    """
    Atomically claim the oldest QUEUED job whose next_retry_at <= NOW().
    Uses SELECT … FOR UPDATE SKIP LOCKED for safe concurrent access.
    Returns the job as a dict, or None if nothing is available.
    """
    session = SessionLocal()
    try:
        now = datetime.now(timezone.utc)

        job = (
            session.query(UploadJob)
            .filter(
                UploadJob.status == "QUEUED",
                UploadJob.next_retry_at <= now,
            )
            .order_by(UploadJob.created_at.asc())
            .with_for_update(skip_locked=True)
            .first()
        )

        if job is None:
            return None

        job.status = "PROCESSING"
        job.attempts += 1
        job.updated_at = now
        session.commit()

        result = {
            "id": job.id,
            "transaction_id": job.transaction_id,
            "user_id": job.user_id,
            "channel_id": job.channel_id,
            "file_url": job.file_url,
            "mime_type": job.mime_type,
            "status": job.status,
            "attempts": job.attempts,
            "max_attempts": job.max_attempts,
        }
        logger.info(f"JOB CLAIMED: id={job.id}, attempt={job.attempts}")
        return result

    except SQLAlchemyError:
        session.rollback()
        logger.error("Failed to claim job", exc_info=True)
        return None
    finally:
        session.close()


# ---------------------------------------------------------------------------
# 3. Complete
# ---------------------------------------------------------------------------

def complete_job(job_id: int, drive_link: str) -> bool:
    """Mark a job as COMPLETED and store the drive_link."""
    session = SessionLocal()
    try:
        job = session.query(UploadJob).filter(UploadJob.id == job_id).first()
        if job is None:
            logger.warning(f"complete_job: job_id={job_id} not found")
            return False

        job.status = "COMPLETED"
        job.drive_link = drive_link
        job.updated_at = datetime.now(timezone.utc)
        session.commit()

        logger.info(f"JOB COMPLETED: {job_id}")
        return True

    except SQLAlchemyError:
        session.rollback()
        logger.error(f"Failed to complete job {job_id}", exc_info=True)
        return False
    finally:
        session.close()


# ---------------------------------------------------------------------------
# 4. Fail
# ---------------------------------------------------------------------------

def fail_job(job_id: int, error_message: str) -> bool:
    """
    Record a failure. If max attempts reached → DEAD.
    Otherwise → back to QUEUED with exponential backoff.
    """
    session = SessionLocal()
    try:
        job = session.query(UploadJob).filter(UploadJob.id == job_id).first()
        if job is None:
            logger.warning(f"fail_job: job_id={job_id} not found")
            return False

        now = datetime.now(timezone.utc)

        if job.attempts >= job.max_attempts:
            job.status = "DEAD"
            logger.warning(f"JOB DEAD (max attempts reached): {job_id}")
        else:
            job.status = "QUEUED"
            backoff = RETRY_BACKOFF.get(job.attempts, timedelta(minutes=30))
            job.next_retry_at = now + backoff

        job.error_message = error_message
        job.updated_at = now
        session.commit()

        logger.info(f"JOB FAILED (attempt {job.attempts}): {error_message}")
        return True

    except SQLAlchemyError:
        session.rollback()
        logger.error(f"Failed to record failure for job {job_id}",
                      exc_info=True)
        return False
    finally:
        session.close()


# ---------------------------------------------------------------------------
# 5. Requeue stuck jobs
# ---------------------------------------------------------------------------

def requeue_stuck_jobs() -> int:
    """
    Find all PROCESSING jobs that haven't been updated in over 5 minutes
    and reset them to QUEUED for immediate retry.
    Returns the number of requeued jobs.
    """
    session = SessionLocal()
    try:
        now = datetime.now(timezone.utc)
        cutoff = now - STUCK_THRESHOLD

        stuck_jobs = (
            session.query(UploadJob)
            .filter(
                UploadJob.status == "PROCESSING",
                UploadJob.updated_at < cutoff,
            )
            .all()
        )

        count = len(stuck_jobs)
        for job in stuck_jobs:
            job.status = "QUEUED"
            job.next_retry_at = now
            job.updated_at = now

        session.commit()

        logger.info(f"REQUEUED {count} STUCK JOBS")
        return count

    except SQLAlchemyError:
        session.rollback()
        logger.error("Failed to requeue stuck jobs", exc_info=True)
        return 0
    finally:
        session.close()
