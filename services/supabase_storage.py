import os
import logging
from dotenv import load_dotenv
from supabase import create_client, Client

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

BUCKET = "receipts"
SUPABASE_URL = _SUPABASE_URL


def upload_file_to_storage(
    transaction_id: str,
    file_bytes: bytes,
    mime_type: str = "image/jpeg",
) -> str:
    """
    Uploads file_bytes to Supabase Storage under the "receipts" bucket.

    File path inside the bucket:
        <transaction_id>.<ext>   e.g. EXP-a1b2c3d4.jpg

    Returns:
        Public URL of the uploaded file (string).

    Raises:
        Exception – if the upload fails, so callers can handle / log the error.
    """
    # Determine extension from MIME type
    ext_map = {
        "image/jpeg": "jpg",
        "image/png":  "png",
        "application/pdf": "pdf",
    }
    ext = ext_map.get(mime_type, "jpg")
    file_path = f"{transaction_id}.{ext}"

    logger.info(f"Uploading to Supabase Storage | bucket={BUCKET} | path={file_path}")

    # Remove any existing file with the same path to allow re-upload
    try:
        _supabase.storage.from_(BUCKET).remove([file_path])
    except Exception:
        pass  # Ignore — file may not exist yet

    # Upload
    _supabase.storage.from_(BUCKET).upload(
        path=file_path,
        file=file_bytes,
        file_options={"content-type": mime_type},
    )

    # Build public URL  (bucket must be public in Supabase dashboard)
    public_url = f"{SUPABASE_URL}/storage/v1/object/public/{BUCKET}/{file_path}"
    logger.info(f"Upload successful | url={public_url}")
    return public_url
