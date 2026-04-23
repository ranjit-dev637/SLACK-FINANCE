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
    slack_file: dict,
    max_retries: int = _MAX_RETRIES,
    timeout: int = _TIMEOUT,
) -> bytes:
    """
    Download a private Slack file, returning its raw bytes.

    Args:
        slack_file:   The file dict from a Slack event payload.
        max_retries:  Number of download attempts before raising.
        timeout:      Per-request timeout in seconds.

    Returns:
        Raw file bytes.

    Raises:
        RuntimeError: On repeated download failures, HTTP errors, or corrupt data.
    """
    url = slack_file.get("url_private_download") or slack_file.get("url_private")

    token = _SLACK_BOT_TOKEN
    headers = {"Authorization": f"Bearer {token}"}

    last_error: Exception = RuntimeError("Download did not start.")

    for attempt in range(1, max_retries + 1):
        logger.info(
            f"[slack_downloader] Attempt {attempt}/{max_retries} | url={url}"
        )
        try:
            response = requests.get(
                url,
                headers=headers,
                stream=True,    # binary-safe — prevents any automatic decoding
                timeout=timeout,
            )

            # ── HTTP status check ──────────────────────────────────────────
            if response.status_code == 401:
                raise RuntimeError(
                    "Slack returned 401 Unauthorized. "
                    "Check that SLACK_BOT_TOKEN is correct and the app has "
                    "'files:read' scope. Reinstall the app if the scope was "
                    "recently added."
                )
            if response.status_code == 403:
                raise RuntimeError(
                    "Slack returned 403 Forbidden. "
                    "The bot may not be a member of the channel where the file "
                    "was shared. Use /invite @your-bot in that channel."
                )
            if response.status_code != 200:
                raise RuntimeError(
                    f"Slack returned HTTP {response.status_code} for file download."
                )

            # ── Content-Type guard (HTML = bad auth or wrong URL) ──────────
            content_type = response.headers.get("Content-Type", "")
            if "text/html" in content_type:
                raise RuntimeError(
                    "Slack returned HTML instead of the file. "
                    "This usually means the bot token is invalid or the URL has expired."
                )

            # ── Read raw bytes ─────────────────────────────────────────────
            file_bytes = response.content   # complete binary payload

            # ── Integrity check ────────────────────────────────────────────
            if not file_bytes or len(file_bytes) < _MIN_FILE_SIZE:
                raise RuntimeError(
                    f"Downloaded file is empty or too small "
                    f"({len(file_bytes) if file_bytes else 0} bytes). "
                    "The file may be corrupt or the download was truncated."
                )

            logger.info(
                f"[slack_downloader] Download successful | "
                f"size={len(file_bytes)} bytes | attempt={attempt}"
            )
            return file_bytes

        except RuntimeError:
            # RuntimeErrors are definitive failures — do not retry
            raise

        except Exception as e:
            last_error = e
            wait = _BACKOFF_BASE * (2 ** (attempt - 1))  # 1.5 → 3 → 6 seconds
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
