import os
import logging
from supabase import create_client, Client

logger = logging.getLogger(__name__)

# ── Supabase client (initialised once at import time) ─────────────────────────
_supabase: Client = create_client(
    os.getenv("SUPABASE_URL", ""),
    os.getenv("SUPABASE_KEY", ""),
)

BUCKET = "receipts"
SUPABASE_URL = os.getenv("SUPABASE_URL", "")


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
