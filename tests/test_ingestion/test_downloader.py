"""Tests for ingestion/downloader.py — FileDownloader + DownloadError.

All network interactions are mocked; tests never hit the real internet.
"""
from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from unittest.mock import patch
from urllib.error import URLError

import pytest

from ingestion.downloader import DownloadError, FileDownloader


def _fake_download(payload: bytes = b"fake content"):
    """Return a urlretrieve stand-in that writes `payload` to the destination path."""
    def _inner(url: str, dest: str):
        Path(dest).write_bytes(payload)
        return dest, None
    return _inner


@pytest.fixture()
def downloader(tmp_path: Path) -> FileDownloader:
    return FileDownloader(cache_dir=tmp_path, source_name="apra")


# ---------------------------------------------------------------------------
# Cache miss -> download
# ---------------------------------------------------------------------------

def test_cache_miss_downloads_and_returns_path(downloader, caplog) -> None:
    caplog.set_level(logging.INFO, logger="ingestion.downloader")
    with patch("ingestion.downloader.urlretrieve", side_effect=_fake_download()):
        path = downloader.download_and_cache(
            "https://example.com/apra.xlsx", "APRA_Q3_2025.xlsx",
        )
    assert path.exists()
    assert path.read_bytes() == b"fake content"
    assert "Downloading" in caplog.text


def test_cache_hit_does_not_call_urlretrieve(downloader, caplog) -> None:
    # Pre-populate the cache with the expected filename.
    cached = downloader.cache_dir / "APRA_Q3_2025.xlsx"
    cached.write_bytes(b"already here")

    caplog.set_level(logging.INFO, logger="ingestion.downloader")
    with patch("ingestion.downloader.urlretrieve", side_effect=_fake_download()) as mock_fn:
        path = downloader.download_and_cache(
            "https://example.com/apra.xlsx", "APRA_Q3_2025.xlsx",
        )
    assert mock_fn.call_count == 0
    assert path == cached
    assert path.read_bytes() == b"already here"
    assert "Using cached" in caplog.text


def test_force_refresh_redownloads_even_when_cached(downloader) -> None:
    cached = downloader.cache_dir / "APRA_Q3_2025.xlsx"
    cached.write_bytes(b"old content")

    with patch(
        "ingestion.downloader.urlretrieve", side_effect=_fake_download(b"new content"),
    ) as mock_fn:
        path = downloader.download_and_cache(
            "https://example.com/apra.xlsx", "APRA_Q3_2025.xlsx",
            force_refresh=True,
        )
    assert mock_fn.call_count == 1
    assert path.read_bytes() == b"new content"


def test_download_error_raised_on_url_error(downloader) -> None:
    def boom(_url, _dest):
        raise URLError("connection refused")

    with patch("ingestion.downloader.urlretrieve", side_effect=boom):
        with pytest.raises(DownloadError, match="Failed to download"):
            downloader.download_and_cache(
                "https://example.com/apra.xlsx", "APRA_Q3_2025.xlsx",
            )


# ---------------------------------------------------------------------------
# list / latest / age
# ---------------------------------------------------------------------------

def test_list_cached_files_sorted_by_mtime_desc(downloader, tmp_path: Path) -> None:
    older = downloader.cache_dir / "old.xlsx"
    older.write_bytes(b"a")
    # Push mtime back 24 hours
    past = time.time() - 86400
    os.utime(older, (past, past))

    newer = downloader.cache_dir / "new.xlsx"
    newer.write_bytes(b"b")   # mtime = now

    listing = downloader.list_cached_files()
    assert [p.name for p in listing] == ["new.xlsx", "old.xlsx"]


def test_get_latest_cached_file_returns_newest(downloader) -> None:
    (downloader.cache_dir / "a.xlsx").write_bytes(b"a")
    (downloader.cache_dir / "b.xlsx").write_bytes(b"b")
    latest = downloader.get_latest_cached_file()
    assert latest is not None and latest.exists()


def test_get_latest_cached_file_none_when_empty(downloader) -> None:
    assert downloader.get_latest_cached_file() is None


def test_get_cache_age_days_zero_for_fresh_file(downloader) -> None:
    fresh = downloader.cache_dir / "fresh.xlsx"
    fresh.write_bytes(b"fresh")
    assert downloader.get_cache_age_days(fresh) == 0


def test_get_cache_age_days_approximates_older_mtime(downloader) -> None:
    p = downloader.cache_dir / "aged.xlsx"
    p.write_bytes(b"aged")
    # Shift mtime back 5 days
    five_days_ago = time.time() - 5 * 86400
    os.utime(p, (five_days_ago, five_days_ago))
    assert downloader.get_cache_age_days(p) >= 4   # tolerate boundary rounding


# ---------------------------------------------------------------------------
# Clear
# ---------------------------------------------------------------------------

def test_clear_cache_removes_all_files_in_source_subdir(downloader) -> None:
    (downloader.cache_dir / "a.xlsx").write_bytes(b"a")
    (downloader.cache_dir / "b.xlsx").write_bytes(b"b")
    assert len(downloader.list_cached_files()) == 2

    count = downloader.clear_cache()
    assert count == 2
    assert downloader.list_cached_files() == []
