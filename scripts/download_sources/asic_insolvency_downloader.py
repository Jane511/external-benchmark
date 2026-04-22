"""Download ASIC insolvency statistics (Series 1 & 2 — primary time-series).

The ASIC insolvency-statistics landing page publishes ~8 workbooks per
cycle: Series 1 & 2 combined (~14.7 MB, the primary industry time-series
the engine needs), plus Series 3.1/3.2/3.3 (causes of failure by
industry), Series 4 / 4A (registered liquidator statistics) and Series
5 (members' voluntary liquidations). ASIC opaquely hashes the
``/media/<hash>/`` URL segment, so the current release URL must be
scraped from the landing page each time.

Primary cache target is Series 1 & 2. Auxiliary series (3.x, 4, 4A, 5)
are fetched opportunistically via a loose filename glob; if ASIC's
layout shifts the adapter yields whatever it can find rather than
failing the whole cycle.

Usage:
    python scripts/download_sources/asic_insolvency_downloader.py
    python scripts/download_sources/asic_insolvency_downloader.py --force-refresh
    python scripts/download_sources/asic_insolvency_downloader.py --primary-only
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


class AsicInsolvencyDownloader:
    """Download ASIC insolvency statistics workbooks (Series 1+2 primary)."""

    BASE_URL = "https://www.asic.gov.au"
    LANDING_URL = (
        f"{BASE_URL}/about-asic/corporate-publications/statistics/insolvency-statistics/"
    )

    # Primary target — the combined Series 1 & 2 workbook.
    PRIMARY_HREF_RE = re.compile(
        r"series-1-and-series-2.*\.xlsx$", re.IGNORECASE,
    )

    # Auxiliary series (3.x, 4, 4A, 5) — opportunistic.
    AUX_HREF_RE = re.compile(
        r"(?:series-3[\-.]?\d*|series-4a?|series-5|asic-mvl-statistics-series-5).*\.xlsx$",
        re.IGNORECASE,
    )

    def __init__(
        self,
        cache_dir: Path = Path("data/asic"),
        max_retries: int = 3,
        timeout: int = 60,
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

    def download_latest(
        self, *, force_refresh: bool = False, primary_only: bool = False,
    ) -> list[Path]:
        downloaded: list[Path] = []
        primary_links, aux_links = self._scrape_download_links()

        if not primary_links:
            logger.warning(
                "ASIC scraper found no Series 1+2 anchor on %s", self.LANDING_URL,
            )

        for filename, url in primary_links.items():
            path = self._download_file(url, filename, force_refresh=force_refresh)
            if path:
                downloaded.append(path)

        if not primary_only:
            for filename, url in aux_links.items():
                path = self._download_file(url, filename, force_refresh=force_refresh)
                if path:
                    downloaded.append(path)

        return downloaded

    def _scrape_download_links(self) -> tuple[dict[str, str], dict[str, str]]:
        try:
            response = self.session.get(self.LANDING_URL, timeout=self.timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.error("Failed to fetch ASIC landing page: %s", exc)
            return {}, {}

        soup = BeautifulSoup(response.content, "html.parser")
        primary: dict[str, str] = {}
        aux: dict[str, str] = {}

        for anchor in soup.find_all("a"):
            href = anchor.get("href") or ""
            absolute = urljoin(response.url or self.LANDING_URL, href)
            filename = href.rsplit("/", 1)[-1]
            if self.PRIMARY_HREF_RE.search(href):
                primary.setdefault(filename, absolute)
            elif self.AUX_HREF_RE.search(href):
                aux.setdefault(filename, absolute)

        return primary, aux

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
        description="Download ASIC insolvency statistics",
    )
    parser.add_argument("--cache-dir", default="data/asic")
    parser.add_argument("--force-refresh", action="store_true")
    parser.add_argument(
        "--primary-only", action="store_true",
        help="Only fetch Series 1 & 2 (skip Series 3.x / 4 / 4A / 5).",
    )
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument(
        "--timeout", type=int, default=60,
        help="Per-request timeout (seconds). Series 1+2 is ~14 MB; default 60s.",
    )
    args = parser.parse_args()

    downloader = AsicInsolvencyDownloader(
        cache_dir=Path(args.cache_dir),
        max_retries=args.max_retries,
        timeout=args.timeout,
    )
    files = downloader.download_latest(
        force_refresh=args.force_refresh,
        primary_only=args.primary_only,
    )
    print(f"Downloaded / cached {len(files)} file(s)")
    for path in files:
        print(f"  {path}")
    return 0 if files else 1


if __name__ == "__main__":
    raise SystemExit(main())
