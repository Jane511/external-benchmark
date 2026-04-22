"""Read-only inspector + clearer for the on-disk download cache.

`CacheManager` walks the distinct subdirectories declared in
`SOURCE_URLS` under a configurable `cache_base` (defaults to `data/raw`)
and reports per-subdir file counts / latest filenames / ages.

`clear_cache(source=...)` removes files for a specific subdir or all of
them. The CLI `benchmark cache clear` path wraps this with a confirmation
prompt when `--yes` isn't supplied.
"""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Optional

from ingestion.source_registry import SOURCE_URLS

logger = logging.getLogger(__name__)


def _subdir_from_cache_dir(cache_dir: str) -> str:
    """'data/raw/apra/' -> 'apra'. Handles trailing slash."""
    return Path(cache_dir).name


class CacheManager:
    """Inspect / clear the ingestion cache rooted at `cache_base`."""

    def __init__(self, cache_base: Path | str = Path("data/raw")) -> None:
        self.cache_base = Path(cache_base)

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def cache_status(self) -> dict[str, dict]:
        """Return one entry per distinct subdir declared in SOURCE_URLS.

        Each value is:
            count:            number of files currently cached
            latest:           filename of newest file (or None if empty)
            latest_age_days:  int age of newest file (or None if empty)
            files:            list[str] filenames, newest first
            source_keys:      list[str] SOURCE_URLS keys that write here
            description:      first description among source_keys using this subdir
        """
        # Group source_keys by subdir (multiple Pillar 3 banks share "pillar3").
        subdir_to_meta: dict[str, dict] = {}
        for source_key, info in SOURCE_URLS.items():
            subdir = _subdir_from_cache_dir(info["cache_dir"])
            entry = subdir_to_meta.setdefault(
                subdir,
                {"description": info.get("description", ""), "source_keys": []},
            )
            entry["source_keys"].append(source_key)

        result: dict[str, dict] = {}
        for subdir, meta in subdir_to_meta.items():
            subdir_path = self.cache_base / subdir
            if subdir_path.exists():
                files = [p for p in subdir_path.iterdir() if p.is_file()]
                files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            else:
                files = []

            latest = files[0] if files else None
            age = (
                max(0, int((time.time() - latest.stat().st_mtime) // 86400))
                if latest else None
            )

            result[subdir] = {
                "count": len(files),
                "latest": latest.name if latest else None,
                "latest_age_days": age,
                "files": [p.name for p in files],
                "source_keys": meta["source_keys"],
                "description": meta["description"],
            }
        return result

    # ------------------------------------------------------------------
    # Clear
    # ------------------------------------------------------------------

    def clear_cache(self, source: Optional[str] = None) -> int:
        """Delete cached files. Returns count removed.

        If `source` is a known subdir name ("apra", "pillar3", ...), only that
        subdir is cleared. If None, every subdir under `cache_base` is cleared.
        """
        subdirs = self._all_subdirs() if source is None else [source]
        total = 0
        for subdir in subdirs:
            subdir_path = self.cache_base / subdir
            if not subdir_path.exists():
                continue
            for p in subdir_path.iterdir():
                if p.is_file():
                    p.unlink()
                    total += 1
        logger.info("Cleared %d cached file(s) from %s", total, subdirs)
        return total

    @staticmethod
    def _all_subdirs() -> list[str]:
        seen: list[str] = []
        for info in SOURCE_URLS.values():
            subdir = _subdir_from_cache_dir(info["cache_dir"])
            if subdir not in seen:
                seen.append(subdir)
        return seen
