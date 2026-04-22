"""Shared utilities for download scripts: logging, retries, validation."""

from __future__ import annotations

import logging
import time
import zipfile
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("download_sources")


class DownloadError(Exception):
    """Raised when a download fails after all retries."""


def safe_download(
    session: requests.Session,
    url: str,
    filepath: Path,
    max_retries: int = 3,
    timeout: int = 30,
    validator=None,
) -> bool:
    """Download ``url`` to ``filepath`` with exponential-backoff retries.

    Writes to a ``.tmp`` sibling first and renames on success. If ``validator``
    is supplied it must accept the temp Path and return a bool; on False the
    temp file is deleted and the attempt counts as failed.
    """
    filepath.parent.mkdir(parents=True, exist_ok=True)
    temp_path = filepath.with_suffix(filepath.suffix + ".tmp")

    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(url, timeout=timeout, stream=True)
            response.raise_for_status()

            with open(temp_path, "wb") as fh:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        fh.write(chunk)

            if validator is not None and not validator(temp_path):
                logger.warning("Validation failed for %s", filepath.name)
                temp_path.unlink(missing_ok=True)
            else:
                temp_path.replace(filepath)
                logger.info("Downloaded %s", filepath)
                return True

        except requests.RequestException as exc:
            logger.warning(
                "Attempt %d/%d failed for %s: %s", attempt, max_retries, url, exc
            )
            temp_path.unlink(missing_ok=True)

        if attempt < max_retries:
            time.sleep(2 ** attempt)

    logger.error("Failed to download %s after %d retries", url, max_retries)
    return False


def validate_xlsx(filepath: Path) -> bool:
    """Return True if ``filepath`` is a readable XLSX (zip with workbook.xml)."""
    try:
        with zipfile.ZipFile(filepath) as zf:
            return "xl/workbook.xml" in zf.namelist()
    except Exception:
        return False


def validate_pdf(filepath: Path) -> bool:
    """Return True if ``filepath`` starts with the %PDF magic bytes."""
    try:
        with open(filepath, "rb") as fh:
            return fh.read(4) == b"%PDF"
    except Exception:
        return False


def validate_by_extension(filepath: Path) -> bool:
    """Dispatch validator based on file extension."""
    suffix = filepath.suffix.lower()
    if suffix == ".xlsx":
        return validate_xlsx(filepath)
    if suffix == ".pdf":
        return validate_pdf(filepath)
    try:
        return filepath.stat().st_size > 0
    except OSError:
        return False
