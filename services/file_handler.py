import logging
from fastapi import UploadFile

logger = logging.getLogger(__name__)

ALLOWED_MIME_TYPES = ["image/png", "image/jpeg", "image/jpg"]
MAX_FILE_SIZE_BYTES = 2 * 1024 * 1024  # 2MB

async def process_income_file(file: UploadFile) -> bytes:
    """
    Validates file type and size.
    Returns the file bytes if valid.
    Raises ValueError on validation failure.
    """
    if file.content_type not in ALLOWED_MIME_TYPES:
        logger.warning(f"Invalid file type uploaded: {file.content_type}")
        raise ValueError("Invalid file type. Only PNG and JPEG images are allowed.")

    file_bytes = await file.read()
    file_size = len(file_bytes)

    logger.info(f"Uploaded file '{file.filename}', size: {file_size} bytes, type: {file.content_type}")

    if file_size > MAX_FILE_SIZE_BYTES:
        logger.warning(f"File size exceeded limit: {file_size} bytes")
        raise ValueError("File size exceeds the 2MB limit.")

    return file_bytes
