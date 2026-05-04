"""
services/supabase_storage.py
────────────────────────────────────────────────────────────────────────────
Supabase Storage service with:
  - Retry logic (3 attempts, linear backoff)
  - Circuit breaker protection
  - Structured logging
"""

import os
import time
import logging
from dotenv import load_dotenv
from supabase import create_client, Client

from services.circuit_breaker import get_breaker, CircuitOpenError

logger = logging.getLogger(__name__)

# Load .env before reading any variable (safe to call multiple times)
load_dotenv()

# ── Validate required env vars at startup ─────────────────────────────────────
_SUPABASE_URL = os.getenv("SUPABASE_URL")
_SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")  # service-role key for backend ops

if not _SUPABASE_URL:
    raise ValueError("SUPABASE_URL environment variable is not set or empty.")
if not _SUPABASE_KEY:
    raise ValueError("SUPABASE_SERVICE_ROLE_KEY environment variable is not set or empty.")

# ── Supabase client (initialised once, reused across all requests) ────────────
_supabase: Client = create_client(_SUPABASE_URL, _SUPABASE_KEY)

BUCKET      = "receipts"
SUPABASE_URL = _SUPABASE_URL

# ── Circuit breaker for Supabase (5 failures → OPEN for 60 s) ────────────────
_supabase_breaker = get_breaker("supabase", failure_threshold=5, reset_timeout=60.0)

# ── Retry config ──────────────────────────────────────────────────────────────
_MAX_RETRIES   = 3
_RETRY_DELAY_S = 1.0   # seconds; multiplied by attempt number (linear backoff)


def _upload_once(file_path: str, file_bytes: bytes, mime_type: str) -> None:
    """Single upload attempt — raises on any error."""
    # Remove existing file (allows idempotent re-upload)
    try:
        _supabase.storage.from_(BUCKET).remove([file_path])
    except Exception:
        pass  # File may not exist yet — that is fine

    upload_resp = _supabase.storage.from_(BUCKET).upload(
        path=file_path,
        file=file_bytes,
        file_options={"content-type": mime_type},
    )
    if hasattr(upload_resp, "error") and upload_resp.error:
        raise RuntimeError(f"Supabase upload error: {upload_resp.error}")


def upload_file_to_storage(
    transaction_id: str,
    file_bytes: bytes,
    mime_type: str = "image/jpeg",
    file_index: int = 1,
) -> str:
    """
    Upload *file_bytes* to the Supabase "receipts" bucket.

    Wraps the upload with:
      - Circuit breaker check (raises CircuitOpenError if Supabase is known-bad)
      - 3-attempt retry with linear backoff (1s, 2s, 3s)

    Returns:
        Public URL of the uploaded file.

    Raises:
        CircuitOpenError — Supabase circuit is OPEN.
        RuntimeError     — All retry attempts exhausted.
    """
    ext_map   = {"image/jpeg": "jpg", "image/png": "png", "application/pdf": "pdf"}
    ext       = ext_map.get(mime_type, "jpg")
    file_path = f"{transaction_id}_{file_index}.{ext}"

    logger.info(
        "supabase_upload_start | bucket=%s | path=%s | size=%d",
        BUCKET, file_path, len(file_bytes),
    )

    last_exc: Exception | None = None

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            _supabase_breaker.call(_upload_once, file_path, file_bytes, mime_type)
            break  # success
        except CircuitOpenError:
            logger.error(
                "supabase_circuit_open | path=%s | attempt=%d", file_path, attempt
            )
            raise  # never retry when circuit is OPEN
        except Exception as exc:
            last_exc = exc
            if attempt < _MAX_RETRIES:
                delay = _RETRY_DELAY_S * attempt
                logger.warning(
                    "supabase_upload_retry | path=%s | attempt=%d/%d | delay=%.1fs | error=%s",
                    file_path, attempt, _MAX_RETRIES, delay, exc,
                )
                time.sleep(delay)
            else:
                logger.error(
                    "supabase_upload_failed | path=%s | attempts=%d | error=%s",
                    file_path, _MAX_RETRIES, exc,
                )
                raise RuntimeError(
                    f"Supabase upload failed after {_MAX_RETRIES} attempts "
                    f"(path={file_path}): {exc}"
                ) from exc

    public_url = f"{SUPABASE_URL}/storage/v1/object/public/{BUCKET}/{file_path}"
    if not public_url:
        raise RuntimeError("Upload returned empty URL — aborting")

    logger.info("supabase_upload_ok | url=%s", public_url)
    print(f"[storage] file_url={public_url}")
    return public_url


def delete_file_from_storage(
    transaction_id: str,
    mime_type: str = "image/jpeg",
    file_index: int = 1,
) -> bool:
    """
    Delete a file from Supabase Storage (used for rollback on Drive failure).
    Returns True on success, False otherwise — never raises.
    """
    ext_map   = {"image/jpeg": "jpg", "image/png": "png", "application/pdf": "pdf"}
    ext       = ext_map.get(mime_type, "jpg")
    file_path = f"{transaction_id}_{file_index}.{ext}"

    try:
        _supabase.storage.from_(BUCKET).remove([file_path])
        logger.info("supabase_delete_ok | path=%s", file_path)
        return True
    except Exception as exc:
        logger.error("supabase_delete_failed | path=%s | error=%s", file_path, exc)
        return False
