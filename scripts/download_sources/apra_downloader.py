"""Download APRA quarterly ADI statistics (Performance + Property Exposures).

Scrapes the APRA quarterly-ADI-statistics listing page, finds the latest
"ADI Performance" and "Property Exposures" XLSX releases, and caches them
under ``data/raw/apra/``.

Usage:
    python scripts/download_sources/apra_downloader.py
    python scripts/download_sources/apra_downloader.py --force-refresh
    python scripts/download_sources/apra_downloader.py --cache-dir /tmp/apra

Selector notes
--------------
APRA publishes each release under an anchor whose visible text is, e.g.
"Quarterly authorised deposit-taking institution performance statistics
December 2025" — note there is no literal "ADI" in the anchor text.
Matching is therefore done by scanning for any of several keywords,
case-insensitively, across BOTH the anchor text AND the href filename,
and falling back to looser keywords if the tight match misses.
"""

from __future__ import annotations

import argparse
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


# (file_type label, primary keywords, fallback keywords). A match wins if ANY
# of the keywords in a tier appears in the anchor text or href basename.
FILE_TYPE_MATCHERS: list[tuple[str, list[str], list[str]]] = [
    (
        "ADI Performance",
        ["adi performance", "institution performance", "performance statistics"],
        ["performance"],
    ),
    (
        "Property Exposures",
        ["property exposures"],
        ["property"],
    ),
]


class ApraAdiDownloader:
    """Download APRA quarterly ADI Performance and Property Exposures files."""

    BASE_URL = "https://www.apra.gov.au"
    LISTING_PAGE = (
        f"{BASE_URL}/quarterly-authorised-deposit-taking-institution-statistics"
    )

    def __init__(
        self,
        cache_dir: Path = Path("data/raw/apra"),
        max_retries: int = 3,
        timeout: int = 30,
    ) -> None:
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.max_retries = max_retries
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "external-benchmark-engine/0.1"})

    def download_latest(self, force_refresh: bool = False) -> list[Path]:
        downloaded: list[Path] = []
        links = self._scrape_download_links()

        for file_type, url in links.items():
            path = self._download_file(url, file_type, force_refresh=force_refresh)
            if path is not None:
                downloaded.append(path)

        expected = {m[0] for m in FILE_TYPE_MATCHERS}
        missed = expected - set(links.keys())
        if missed:
            logger.warning(
                "APRA listing page yielded no link for: %s", sorted(missed)
            )

        if not downloaded:
            logger.warning("No APRA ADI files downloaded (all cached or all failed)")
        return downloaded

    def _scrape_download_links(self) -> dict[str, str]:
        try:
            response = self.session.get(self.LISTING_PAGE, timeout=self.timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.error("Failed to fetch APRA listing page: %s", exc)
            return {}

        soup = BeautifulSoup(response.content, "html.parser")
        xlsx_anchors = [
            a for a in soup.find_all("a")
            if (a.get("href", "") or "").lower().split("?")[0].endswith(".xlsx")
        ]

        links: dict[str, str] = {}
        for file_type, primary, fallback in FILE_TYPE_MATCHERS:
            match = self._match_anchor(xlsx_anchors, primary, strict=True)
            tier = "primary"
            if match is None:
                match = self._match_anchor(xlsx_anchors, fallback, strict=False)
                tier = "fallback"
            if match is not None:
                href = match.get("href", "")
                links[file_type] = urljoin(self.LISTING_PAGE, href)
                logger.info(
                    "APRA matcher (%s) selected for %r: %s",
                    tier, file_type, href.rsplit("/", 1)[-1],
                )
            else:
                logger.warning(
                    "APRA matcher found no XLSX for %r (tried %s then %s)",
                    file_type, primary, fallback,
                )

        return links

    @staticmethod
    def _match_anchor(anchors, keywords: list[str], strict: bool):
        """Return first anchor whose text or href basename contains any keyword.

        When ``strict`` is True, keywords must appear as substrings in the
        lowercased text OR href basename. Fallback (``strict`` False) behaves
        the same — the distinction exists to keep the two tiers separate in
        logging; the matching rule itself is identical.
        """
        for anchor in anchors:
            text = anchor.get_text(" ", strip=True).lower()
            href = (anchor.get("href", "") or "").lower()
            basename = href.rsplit("/", 1)[-1]
            haystack = f"{text} || {basename}"
            if any(kw in haystack for kw in keywords):
                return anchor
        return None

    def _download_file(
        self, url: str, file_type: str, force_refresh: bool = False
    ) -> Path | None:
        filename = url.rsplit("/", 1)[-1] or f"{file_type.replace(' ', '_')}_latest.xlsx"
        filepath = self.cache_dir / filename

        if filepath.exists() and not force_refresh:
            logger.info("Using cached %s: %s", file_type, filepath)
            return filepath

        ok = safe_download(
            self.session,
            url,
            filepath,
            max_retries=self.max_retries,
            timeout=self.timeout,
            validator=validate_xlsx,
        )
        return filepath if ok else None


def main() -> int:
    parser = argparse.ArgumentParser(description="Download APRA ADI statistics")
    parser.add_argument("--cache-dir", default="data/raw/apra")
    parser.add_argument("--force-refresh", action="store_true")
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()

    downloader = ApraAdiDownloader(
        cache_dir=Path(args.cache_dir),
        max_retries=args.max_retries,
        timeout=args.timeout,
    )
    files = downloader.download_latest(force_refresh=args.force_refresh)

    print(f"Downloaded {len(files)} file(s)")
    for path in files:
        print(f"  {path}")
    return 0 if files else 1


if __name__ == "__main__":
    raise SystemExit(main())
