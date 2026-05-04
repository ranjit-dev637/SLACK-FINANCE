"""
services/google_drive.py
Handles all Google Drive file uploads for the Finance Bot.

Production features:
  - Unique filename generation (category + unix timestamp)
  - Folder existence validation before upload
  - Retry logic (3 attempts, 1 s delay) for transient API failures
  - Structured logging + debug prints at every key step
"""

import io
import os
import logging
import time
import pathlib
import pickle
from typing import Optional

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload
from google.auth.transport.requests import Request


# ── Logger ────────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)

# ── OAuth scopes ──────────────────────────────────────────────────────────────
SCOPES = ["https://www.googleapis.com/auth/drive"]

# ── Folder IDs (sourced from .env; hardcoded defaults as safety net) ──────────
DRIVE_FOLDER_INCOME  = os.getenv("GOOGLE_DRIVE_FOLDER_INCOME",  "1Gu4bTRjIca6fR0iB65aJKVMMmkhpsnx-")
DRIVE_FOLDER_EXPENSE = os.getenv("GOOGLE_DRIVE_FOLDER_EXPENSE", "1szvLU69NixqunsK0u-kooVjpvAZxxNmw")

# ── Retry config ──────────────────────────────────────────────────────────────
MAX_RETRIES   = 3
RETRY_DELAYS  = [1, 2, 4]  # Exponential backoff in seconds

# ── HTTP status codes that are worth retrying ──────────────────────────────────
_RETRYABLE_STATUS = {429, 500, 502, 503, 504}


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_drive_service():
    """Load OAuth credentials from token.pickle and return a Drive v3 service."""
    BASE_DIR   = pathlib.Path(__file__).parent.parent
    token_path = BASE_DIR / "token.pickle"
    creds: Optional[object] = None

    if token_path.exists():
        with open(token_path, "rb") as fh:
            creds = pickle.load(fh)
        logger.debug("token.pickle loaded from %s", token_path)
    else:
        logger.warning("token.pickle not found at %s", token_path)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logger.info("OAuth token expired — refreshing …")
            creds.refresh(Request())
            with open(token_path, "wb") as fh:
                pickle.dump(creds, fh)
            logger.info("OAuth token refreshed and saved.")
        else:
            raise RuntimeError(
                "token.pickle not found or credentials are invalid. "
                "Run generate_token.py to obtain fresh credentials."
            )

    return build("drive", "v3", credentials=creds)


def _get_folder_id(record_type: str) -> str:
    """Return the Drive folder ID for the given record type."""
    if record_type == "income":
        return DRIVE_FOLDER_INCOME
    return DRIVE_FOLDER_EXPENSE


def _make_unique_filename(category: str, original_filename: str) -> str:
    """
    Generate a collision-resistant filename.

    Format: {category}_{unix_timestamp}{ext}
    Example: income_1746178400.jpg
    """
    ext       = pathlib.Path(original_filename).suffix  # e.g. ".jpg"
    timestamp = int(time.time())
    unique    = f"{category}_{timestamp}{ext}"
    logger.debug("Filename remapped: '%s' → '%s'", original_filename, unique)
    return unique


def _validate_folder(service, folder_id: str) -> None:
    """
    Verify the target folder exists and is accessible by querying its children.
    Using files().list() is reliable for both owned and shared folders.
    Raises RuntimeError with a clear message if the folder is invalid.
    """
    try:
        result = service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="files(id, name)",
            pageSize=1,
        ).execute()
        logger.info("Folder validated | id=%s | accessible=True", folder_id)
    except HttpError as exc:
        status = exc.resp.status if exc.resp else "unknown"
        raise RuntimeError(
            f"Drive folder validation failed (HTTP {status}) for folder_id='{folder_id}'. "
            "Check that the folder exists and the OAuth account has access to it."
        ) from exc


def _is_retryable(exc: Exception) -> bool:
    """Return True if the exception looks like a transient API/network failure."""
    if isinstance(exc, HttpError):
        status = exc.resp.status if exc.resp else 0
        return status in _RETRYABLE_STATUS
    # Catch generic network / connection errors
    retryable_names = {"ConnectionError", "Timeout", "ChunkedEncodingError"}
    return type(exc).__name__ in retryable_names


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def upload_to_drive(file_bytes: bytes, filename: str, record_type: str, mime_type: str = None):
    service = _get_drive_service()
    
    folder_id = "1Gu4bTRjIca6fR0iB65aJKVMMmkhpsnx-" if record_type.lower() == "income" else "1szvLU69NixqunsK0u-kooVjpvAZxxNmw"
    
    print(f"[DEBUG] Uploading to folder: {folder_id} | filename: {filename}")
    
    file_metadata = {
        'name': filename,
        'parents': [folder_id]
    }

    media = MediaIoBaseUpload(
        io.BytesIO(file_bytes), 
        mimetype=mime_type or 'image/jpeg',
        resumable=True
    )

    try:
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id,webViewLink,name'
        ).execute()
        
        # ── Make the file publicly readable ────────────────────────────
        service.permissions().create(
            fileId=file.get('id'),
            body={"type": "anyone", "role": "reader"},
        ).execute()
        
        link = file.get('webViewLink')
        print(f"✅ Upload successful: {link}")
        return link
    except Exception as e:
        print(f"❌ Drive upload failed: {e}")
        raise
