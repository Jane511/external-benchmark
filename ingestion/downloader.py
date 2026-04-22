"""Cached file downloader used by all ingestion scrapers.

One instance per (cache_base, source_name) — it owns the subdirectory
`cache_base/source_name/`. Files are identified by `filename`; the URL is
only used when the cache misses. This lets tests pre-populate cache files
and reason about download behaviour without hitting the network.

Scrapers that subclass BaseScraper compose a FileDownloader in their
`_fetch_via_cache()` path. The ingestion CLI exposes cache inspection +
cache invalidation through `benchmark cache status` / `benchmark cache clear`.
"""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Optional
from urllib.error import URLError
from urllib.request import urlretrieve

logger = logging.getLogger(__name__)


class DownloadError(Exception):
    """Raised when a remote download fails (network, HTTP, or filesystem)."""


class FileDownloader:
    """Download-or-reuse-from-cache for a single ingestion source."""

    def __init__(self, cache_dir: Path | str, source_name: str) -> None:
        self.source_name = source_name
        # Per-source subdirectory is created eagerly so scrapers never race.
        self.cache_dir: Path = Path(cache_dir) / source_name
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    def download_and_cache(
        self,
        url: str,
        filename: str,
        force_refresh: bool = False,
    ) -> Path:
        """Return a local Path to the file; download first if cache misses.

        Log messages are emitted at INFO level for both hit and miss so the
        audit trail captures every source touch.
        """
        dest = self.cache_dir / filename

        if dest.exists() and not force_refresh:
            logger.info(
                "Using cached %s for %s (source=%s)", filename, dest, self.source_name,
            )
            return dest

        logger.info(
            "Downloading %s -> %s (source=%s, force_refresh=%s)",
            url, dest, self.source_name, force_refresh,
        )
        try:
            urlretrieve(url, str(dest))
        except (URLError, TimeoutError, OSError) as exc:
            raise DownloadError(
                f"Failed to download {url} for source {self.source_name!r}: {exc}"
            ) from exc
        return dest

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------

    def list_cached_files(self) -> list[Path]:
        """Files currently cached for this source, newest first."""
        if not self.cache_dir.exists():
            return []
        files = [p for p in self.cache_dir.iterdir() if p.is_file()]
        files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return files

    def get_latest_cached_file(self) -> Optional[Path]:
        files = self.list_cached_files()
        return files[0] if files else None

    @staticmethod
    def get_cache_age_days(path: Path) -> int:
        """Integer days since the file was last modified."""
        age_seconds = time.time() - path.stat().st_mtime
        return max(0, int(age_seconds // 86400))

    def clear_cache(self) -> int:
        """Delete every file in this source's cache directory. Returns count removed."""
        if not self.cache_dir.exists():
            return 0
        count = 0
        for p in self.cache_dir.iterdir():
            if p.is_file():
                p.unlink()
                count += 1
        logger.info(
            "Cleared %d cached file(s) for source=%s", count, self.source_name,
        )
        return count
