"""
load_test_pipeline.py
────────────────────────────────────────────────────────────────────────────
Concurrent load test for the upload pipeline.

Spawns 15 threads, each with a unique PENDING income record, and calls
process_upload() simultaneously.  After all workers finish it queries
the DB and asserts:

  ✅  Zero PENDING rows among the test set
  ✅  Zero duplicate transaction_id values
  ✅  Every COMPLETED row has a non-NULL payment_screenshot
  ✅  Every FAILED row has a non-NULL error_message
  ✅  No row is in any other state

Prints a colour-free summary table compatible with standard terminals.

Usage:
    python load_test_pipeline.py
"""

import uuid
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Optional

from database import SessionLocal
from models import Income
from services.upload_pipeline import process_upload
from sqlalchemy import text

# ── Config ────────────────────────────────────────────────────────────────────
NUM_WORKERS    = 15
FILE_BYTES     = b"LOAD_TEST_DUMMY_CONTENT_" * 50   # ~1.2 KB — tiny but real
MIME_TYPE      = "image/jpeg"
TAG            = "LT"                               # prefix so rows are easy to find


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

_print_lock = threading.Lock()

def _log(msg: str) -> None:
    with _print_lock:
        print(msg)


def _create_pending_record() -> tuple[str, int]:
    """Insert a PENDING income row; return (transaction_id, pk)."""
    db = SessionLocal()
    try:
        txn_id = f"{TAG}-{uuid.uuid4().hex[:10]}"
        inc = Income(
            transaction_id = txn_id,
            user_id        = "U_LOAD_TEST",
            status         = "PENDING",
            name           = "LoadTest User",
            booking_number = "0",
            contact_number = "0000000000",
            room_amount    = 0.0,
            food_amount    = 0.0,
            payment_type   = "Cash",
            receipt_by     = "LoadTest",
            submitted_by   = "U_LOAD_TEST",
            submitted_at   = datetime.now(timezone.utc),
        )
        db.add(inc)
        db.commit()
        db.refresh(inc)
        return txn_id, inc.id
    finally:
        db.close()


def _run_worker(worker_id: int, txn_id: str, record_id: int) -> dict:
    """Execute the pipeline; return a result dict (never raises)."""
    _log(f"  [W{worker_id:02d}] START  txn={txn_id}")
    try:
        result = process_upload(
            record_id       = record_id,
            transaction_id  = txn_id,
            file_bytes      = FILE_BYTES,
            mime_type       = MIME_TYPE,
            file_index      = 1,
            record_type     = "income",
            submitted_by_id   = "U_LOAD_TEST",
            submitted_by_name = f"Worker-{worker_id}",
        )
        _log(f"  [W{worker_id:02d}] DONE   txn={txn_id} → COMPLETED")
        return {"worker": worker_id, "txn_id": txn_id, "ok": True, "error": None}
    except Exception as exc:
        _log(f"  [W{worker_id:02d}] ERROR  txn={txn_id} → {exc}")
        return {"worker": worker_id, "txn_id": txn_id, "ok": False, "error": str(exc)}


# ─────────────────────────────────────────────────────────────────────────────
# Assertions
# ─────────────────────────────────────────────────────────────────────────────

def _assert(label: str, passed: bool, detail: str = "") -> bool:
    icon = "PASS" if passed else "FAIL"
    line = f"  [{icon}] {label}"
    if detail:
        line += f"  ({detail})"
    print(line)
    return passed


def _run_assertions(txn_ids: list[str]) -> int:
    """Run DB-level assertions.  Returns number of failures."""
    db = SessionLocal()
    failures = 0
    try:
        placeholders = ", ".join(f"'{t}'" for t in txn_ids)
        rows = db.execute(text(f"""
            SELECT transaction_id, status, payment_screenshot, error_message
            FROM   incomes
            WHERE  transaction_id IN ({placeholders})
        """)).fetchall()

        txn_map = {r[0]: r for r in rows}

        # ── Assertion 1: all records returned (no missing rows) ───────────────
        ok = len(rows) == len(txn_ids)
        if not _assert("All records exist in DB", ok, f"{len(rows)}/{len(txn_ids)}"):
            failures += 1

        # ── Assertion 2: zero PENDING ─────────────────────────────────────────
        pending = [r for r in rows if r[1] == "PENDING"]
        if not _assert("Zero PENDING rows", len(pending) == 0, f"{len(pending)} found"):
            failures += 1

        # ── Assertion 3: no duplicate transaction_id ──────────────────────────
        dup_check = db.execute(text(f"""
            SELECT transaction_id, COUNT(*) AS cnt
            FROM   incomes
            WHERE  transaction_id IN ({placeholders})
            GROUP  BY transaction_id
            HAVING COUNT(*) > 1
        """)).fetchall()
        if not _assert("Zero duplicate transaction_ids", len(dup_check) == 0,
                        f"{len(dup_check)} duplicates"):
            failures += 1

        # ── Assertion 4: COMPLETED rows have non-NULL screenshot ──────────────
        completed = [r for r in rows if r[1] == "COMPLETED"]
        bad_completed = [r for r in completed if not r[2]]
        if not _assert(
            "All COMPLETED rows have payment_screenshot",
            len(bad_completed) == 0,
            f"{len(completed)} completed, {len(bad_completed)} missing screenshot",
        ):
            failures += 1

        # ── Assertion 5: FAILED rows have non-NULL error_message ──────────────
        failed = [r for r in rows if r[1] == "FAILED"]
        bad_failed = [r for r in failed if not r[3]]
        if not _assert(
            "All FAILED rows have error_message",
            len(bad_failed) == 0,
            f"{len(failed)} failed, {len(bad_failed)} missing error",
        ):
            failures += 1

        # ── Assertion 6: no invalid status values ─────────────────────────────
        invalid_status = [r for r in rows if r[1] not in ("COMPLETED", "FAILED")]
        if not _assert(
            "No invalid status values",
            len(invalid_status) == 0,
            f"{[r[1] for r in invalid_status]}",
        ):
            failures += 1

        # ── Summary counts ────────────────────────────────────────────────────
        print(f"\n  Breakdown: {len(completed)} COMPLETED | {len(failed)} FAILED | "
              f"{len(pending)} PENDING | {len(invalid_status)} INVALID")

    finally:
        db.close()
    return failures


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def run_load_test() -> None:
    print("=" * 60)
    print(f"  LOAD TEST START — {NUM_WORKERS} concurrent workers")
    print("=" * 60)

    # ── Phase 1: Create all PENDING records (sequential, fast) ───────────────
    print("\n[Phase 1] Creating PENDING records …")
    records: list[tuple[int, str, int]] = []   # (worker_id, txn_id, record_pk)
    for i in range(1, NUM_WORKERS + 1):
        txn_id, pk = _create_pending_record()
        records.append((i, txn_id, pk))
        print(f"  Created [{i:02d}] txn={txn_id} pk={pk}")

    # ── Phase 2: Fire all uploads concurrently ────────────────────────────────
    print(f"\n[Phase 2] Firing {NUM_WORKERS} concurrent uploads …")
    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as pool:
        futures = {
            pool.submit(_run_worker, worker_id, txn_id, pk): worker_id
            for worker_id, txn_id, pk in records
        }
        for future in as_completed(futures):
            results.append(future.result())

    ok_count  = sum(1 for r in results if r["ok"])
    err_count = sum(1 for r in results if not r["ok"])
    print(f"\n  Workers finished: {ok_count} succeeded, {err_count} failed\n")

    # ── Phase 3: DB assertions ────────────────────────────────────────────────
    print("[Phase 3] Running DB assertions …\n")
    txn_ids   = [r[1] for r in records]
    failures  = _run_assertions(txn_ids)

    # ── Phase 4: Audit log spot-check ────────────────────────────────────────
    print("\n[Phase 4] Audit log spot-check …")
    db = SessionLocal()
    try:
        log_count = db.execute(text(f"""
            SELECT COUNT(*) FROM upload_logs
            WHERE  transaction_id LIKE '{TAG}-%'
        """)).scalar()
        _assert("upload_logs has entries for load-test txns", log_count > 0,
                f"{log_count} rows found")
    finally:
        db.close()

    # ── Final result ──────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    if failures == 0:
        print("  ✅  LOAD TEST PASSED — all assertions green")
    else:
        print(f"  ❌  LOAD TEST FAILED — {failures} assertion(s) failed")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    run_load_test()
