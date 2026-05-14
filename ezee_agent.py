"""
ezee_agent.py — Autonomous Hotel Operations Agent
===================================================
Fetches real-time booking and inventory data from eZee Absolute
for all managed properties, processes KPIs, persists snapshots
to PostgreSQL, and generates management-level operational reports.

Runs on a configurable schedule (default: every 2 hours).
Designed for production: graceful shutdown, health tracking,
per-property error isolation, and structured logging.

Usage:
    python ezee_agent.py
"""

import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone

import schedule
from loguru import logger

from config import settings
from database import init_db, SessionLocal
from ezee_client import EZeeClient
from models import KPIReport
from processor import KPIProcessor
from report import ReportGenerator

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PROPERTIES = [
    "Clover Villa",
    "Clovera",
    "Clover Woods",
    "Clover Connect",
]

SCHEDULE_INTERVAL_HOURS = int(os.getenv("AGENT_INTERVAL_HOURS", "2"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("AGENT_LOG_FILE", "ezee_agent.log")
LOG_ROTATION = os.getenv("AGENT_LOG_ROTATION", "10 MB")
LOG_RETENTION = os.getenv("AGENT_LOG_RETENTION", "30 days")

# ---------------------------------------------------------------------------
# Health state (queryable by external monitors / health endpoints)
# ---------------------------------------------------------------------------

_health_lock = threading.Lock()
_health = {
    "started_at": None,
    "last_run_at": None,
    "last_run_status": None,      # "success" | "partial" | "failed"
    "total_runs": 0,
    "total_failures": 0,
    "properties_last_ok": [],
    "properties_last_failed": [],
}


def get_health() -> dict:
    """Return a snapshot of the agent's health state (thread-safe)."""
    with _health_lock:
        return dict(_health)


def _update_health(*, status: str, ok: list, failed: list) -> None:
    with _health_lock:
        _health["last_run_at"] = datetime.now(timezone.utc).isoformat()
        _health["last_run_status"] = status
        _health["total_runs"] += 1
        if status != "success":
            _health["total_failures"] += 1
        _health["properties_last_ok"] = ok
        _health["properties_last_failed"] = failed


# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

_shutdown_event = threading.Event()


def _handle_signal(signum, _frame):
    sig_name = signal.Signals(signum).name
    logger.warning(f"Received {sig_name} — initiating graceful shutdown…")
    _shutdown_event.set()


# Register handlers for clean termination
signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ---------------------------------------------------------------------------
# Core agent job
# ---------------------------------------------------------------------------

def run_agent_job() -> None:
    """
    Single execution cycle:
      1. Fetch real-time data from eZee Absolute
      2. Process KPIs per property (isolated — one failure won't block others)
      3. Persist snapshots and generate reports
    """
    cycle_start = time.monotonic()
    logger.info("=" * 60)
    logger.info("AGENT CYCLE START — fetching data from eZee Absolute")
    logger.info("=" * 60)

    client = EZeeClient()
    raw_data = client.get_real_time_data()

    if not raw_data:
        logger.error("Failed to fetch data from eZee Absolute — skipping cycle.")
        _update_health(status="failed", ok=[], failed=PROPERTIES[:])
        return

    ok_properties: list[str] = []
    failed_properties: list[str] = []

    for property_name in PROPERTIES:
        try:
            logger.info(f"Processing: {property_name}")

            # Compute KPIs
            kpis = KPIProcessor.process(raw_data, property_name)

            # Persist snapshot and emit report
            ReportGenerator.save_and_generate(property_name, kpis)

            ok_properties.append(property_name)
            logger.success(f"✓ {property_name} — OK")

        except Exception as exc:
            failed_properties.append(property_name)
            logger.error(
                f"✗ {property_name} — FAILED: {exc}",
                exc_info=True,
            )

    # Determine overall status
    if not failed_properties:
        status = "success"
    elif not ok_properties:
        status = "failed"
    else:
        status = "partial"

    _update_health(status=status, ok=ok_properties, failed=failed_properties)

    elapsed = time.monotonic() - cycle_start
    logger.info(
        f"AGENT CYCLE END — {len(ok_properties)}/{len(PROPERTIES)} properties OK "
        f"| elapsed {elapsed:.1f}s | status={status}"
    )
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Bootstrap the agent:
      - Configure structured logging
      - Initialise database tables
      - Run an immediate cycle
      - Enter the scheduler loop (exits cleanly on SIGINT / SIGTERM)
    """
    # ── Logging ──────────────────────────────────────────────────────────
    logger.remove()  # Remove default stderr handler
    logger.add(
        sys.stderr,
        level=LOG_LEVEL,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> — "
            "<level>{message}</level>"
        ),
        colorize=True,
    )
    logger.add(
        LOG_FILE,
        rotation=LOG_ROTATION,
        retention=LOG_RETENTION,
        compression="zip",
        level=LOG_LEVEL,
        format=(
            "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | "
            "{name}:{function}:{line} — {message}"
        ),
    )

    logger.info("Antigravity Hotel Operations Agent v1.0 starting up…")
    logger.info(f"Properties monitored: {', '.join(PROPERTIES)}")
    logger.info(f"Schedule interval: every {SCHEDULE_INTERVAL_HOURS} hour(s)")

    with _health_lock:
        _health["started_at"] = datetime.now(timezone.utc).isoformat()

    # ── Database ─────────────────────────────────────────────────────────
    try:
        init_db()
    except Exception as exc:
        logger.critical(f"Database initialisation failed — aborting: {exc}")
        sys.exit(1)

    # ── Immediate first run ──────────────────────────────────────────────
    run_agent_job()

    # ── Scheduler ────────────────────────────────────────────────────────
    schedule.every(SCHEDULE_INTERVAL_HOURS).hours.do(run_agent_job)
    logger.info(
        f"Scheduler armed — next run in {SCHEDULE_INTERVAL_HOURS}h. "
        "Press Ctrl+C to stop."
    )

    # ── Main loop (sleeps in 30-second increments for fast shutdown) ─────
    try:
        while not _shutdown_event.is_set():
            schedule.run_pending()
            # Sleep in small increments so we react quickly to signals
            _shutdown_event.wait(timeout=30)
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Agent shutting down — goodbye.")


if __name__ == "__main__":
    main()
