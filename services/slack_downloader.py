"""
services/slack_downloader.py
-----------------------------
Production-ready helper for downloading private Slack files.

Key features:
  - Token validated once at module import (fails loudly at startup, not mid-request)
  - Retries with exponential back-off (3 attempts by default)
  - HTML-response detection (catches invalid auth / expired URLs)
  - Integrity check (rejects empty / truncated downloads)
  - Fully synchronous (safe to call from a daemon thread inside a Slack handler)
"""

import os
import time
import logging
from dotenv import load_dotenv

import requests

logger = logging.getLogger(__name__)

# ── Load .env and validate token at import time ───────────────────────────────
load_dotenv()

_SLACK_BOT_TOKEN: str = os.getenv("SLACK_BOT_TOKEN", "")
if not _SLACK_BOT_TOKEN:
    raise ValueError(
        "SLACK_BOT_TOKEN is not set or empty. "
        "Check your .env file and ensure the variable exists."
    )

logger.info(
    f"[slack_downloader] Token loaded | starts_with={_SLACK_BOT_TOKEN[:10]}... | "
    f"length={len(_SLACK_BOT_TOKEN)}"
)

# ── Constants ─────────────────────────────────────────────────────────────────
_MAX_RETRIES   = 3        # total attempts
_BACKOFF_BASE  = 1.5      # seconds — doubles each retry (1.5 → 3 → 6)
_TIMEOUT       = 20       # seconds per request
_MIN_FILE_SIZE = 100      # bytes — files smaller than this are considered corrupt


def get_file_url(slack_file: dict) -> str:
    """
    Extract the best download URL from a Slack file object.

    Prefers url_private_download (direct binary stream) over url_private.
    Both require Bearer-token authentication.

    Args:
        slack_file: The file dict from a Slack event payload
                    (event["files"][0] or event["file"]).

    Returns:
        The download URL string.

    Raises:
        ValueError: If neither URL key is present in the file object.
    """
    url = slack_file.get("url_private_download") or slack_file.get("url_private")
    if not url:
        raise ValueError(
            "Slack file object contains no downloadable URL "
            "(url_private_download / url_private). "
            f"Keys present: {list(slack_file.keys())}"
        )
    return url


def download_slack_file(
    slack_file_url: str,
    max_retries: int = _MAX_RETRIES,
    timeout: int = _TIMEOUT,
) -> tuple[bytes, str]:
    """
    Download a private Slack file, returning its raw bytes and resolved MIME type.
    """
    token = _SLACK_BOT_TOKEN
    headers = {"Authorization": f"Bearer {token}"}

    last_error: Exception = RuntimeError("Download did not start.")

    for attempt in range(1, max_retries + 1):
        logger.info(
            f"[slack_downloader] Attempt {attempt}/{max_retries} | url={slack_file_url}"
        )
        try:
            response = requests.get(
                slack_file_url,
                headers=headers,
                stream=True,
                timeout=timeout,
            )

            if response.status_code != 200:
                raise RuntimeError(
                    f"Slack returned HTTP {response.status_code} for file download."
                )

            file_bytes = response.content

            if len(file_bytes) < 1000:
                raise RuntimeError(
                    f"Invalid file: too small ({len(file_bytes)} bytes)."
                )

            if file_bytes.startswith(b"<"):
                raise RuntimeError(
                    "Slack returned HTML instead of the file. This usually means the bot token is invalid or the URL has expired."
                )

            resolved_mime = None
            if file_bytes.startswith(b'\xff\xd8'):
                resolved_mime = "image/jpeg"
            elif file_bytes.startswith(b'\x89PNG\x0d\x0a\x1a\x0a') or file_bytes.startswith(b'\x89PNG'):
                resolved_mime = "image/png"
            elif file_bytes.startswith(b'%PDF'):
                resolved_mime = "application/pdf"
            else:
                raise RuntimeError("Invalid file format: Magic bytes do not match JPEG, PNG, or PDF.")

            logger.info(
                f"[slack_downloader] Download successful | "
                f"size={len(file_bytes)} bytes | mime={resolved_mime} | attempt={attempt}"
            )
            return file_bytes, resolved_mime

        except RuntimeError:
            raise

        except Exception as e:
            last_error = e
            wait = _BACKOFF_BASE * (2 ** (attempt - 1))
            logger.warning(
                f"[slack_downloader] Attempt {attempt} failed: {e} | "
                f"retrying in {wait:.1f}s..."
            )
            if attempt < max_retries:
                time.sleep(wait)

    raise RuntimeError(
        f"All {max_retries} download attempts failed. "
        f"Last error: {last_error}"
    )
