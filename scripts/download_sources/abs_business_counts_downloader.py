"""Download ABS Counts of Australian Businesses (cat. 8165) data cubes.

Scrapes the ABS ``/latest-release`` landing page, finds every
``8165DC*.xlsx`` anchor (Data Cubes 1 through 11), and caches them to
``data/abs/``. The primary target for the PD benchmark engine is
Data Cube 1 (``8165DC01.xlsx``) — top-level tables of business counts
and entries/exits by ANZSIC division — but we fetch the whole set so
downstream work (LVR-like breakdowns, state-level segmentation) has
what it needs.

Usage:
    python scripts/download_sources/abs_business_counts_downloader.py
    python scripts/download_sources/abs_business_counts_downloader.py --force-refresh
    python scripts/download_sources/abs_business_counts_downloader.py --cache-dir /tmp/abs

Selector notes
--------------
ABS publishes the entry-exit release under a URL slug that advances
once a year (e.g. ``jul2021-jun2025``). The adapter finds the current
release through ``/latest-release`` — the ABS redirects that to the
current slug — and then scrapes every ``8165DC*.xlsx`` anchor from the
resolved page. If a filename already exists in the cache we skip the
download unless ``--force-refresh``.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from common import logger, safe_download, validate_xlsx  # type: ignore
else:
    from .common import logger, safe_download, validate_xlsx


class AbsBusinessCountsDownloader:
    """Download the ABS Counts of Australian Businesses Data Cubes."""

    BASE_URL = "https://www.abs.gov.au"
    LATEST_RELEASE_URL = (
        f"{BASE_URL}/statistics/economy/business-indicators/"
        "counts-australian-businesses-including-entries-and-exits/latest-release"
    )
    # Matches 8165DC01.xlsx through 8165DC11.xlsx (and any future cubes).
    FILE_HREF_RE = re.compile(r"/8165DC\d+\.xlsx$", re.IGNORECASE)

    def __init__(
        self,
        cache_dir: Path = Path("data/abs"),
        max_retries: int = 3,
        timeout: int = 30,
    ) -> None:
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.max_retries = max_retries
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "external-benchmark-engine/0.1"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    def download_latest(self, force_refresh: bool = False) -> list[Path]:
        downloaded: list[Path] = []
        links = self._scrape_download_links()
        if not links:
            logger.warning(
                "ABS scraper found no 8165DC*.xlsx anchors on %s",
                self.LATEST_RELEASE_URL,
            )
            return downloaded

        for filename, url in links.items():
            path = self._download_file(url, filename, force_refresh=force_refresh)
            if path is not None:
                downloaded.append(path)
        return downloaded

    def _scrape_download_links(self) -> dict[str, str]:
        try:
            response = self.session.get(self.LATEST_RELEASE_URL, timeout=self.timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.error("Failed to fetch ABS landing page: %s", exc)
            return {}

        soup = BeautifulSoup(response.content, "html.parser")
        links: dict[str, str] = {}
        for anchor in soup.find_all("a"):
            href = anchor.get("href") or ""
            if not self.FILE_HREF_RE.search(href):
                continue
            absolute = urljoin(response.url or self.LATEST_RELEASE_URL, href)
            filename = href.rsplit("/", 1)[-1]
            links.setdefault(filename, absolute)
        return links

    def _download_file(
        self, url: str, filename: str, force_refresh: bool = False,
    ) -> Path | None:
        filepath = self.cache_dir / filename
        if filepath.exists() and not force_refresh:
            logger.info("Using cached %s: %s", filename, filepath)
            return filepath
        ok = safe_download(
            self.session, url, filepath,
            max_retries=self.max_retries,
            timeout=self.timeout,
            validator=validate_xlsx,
        )
        return filepath if ok else None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download ABS Counts of Australian Businesses (cat. 8165)",
    )
    parser.add_argument("--cache-dir", default="data/abs")
    parser.add_argument("--force-refresh", action="store_true")
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()

    downloader = AbsBusinessCountsDownloader(
        cache_dir=Path(args.cache_dir),
        max_retries=args.max_retries,
        timeout=args.timeout,
    )
    files = downloader.download_latest(force_refresh=args.force_refresh)
    print(f"Downloaded / cached {len(files)} file(s)")
    for path in files:
        print(f"  {path}")
    return 0 if files else 1


if __name__ == "__main__":
    raise SystemExit(main())
