"""Download bank Pillar 3 disclosures (CBA XLSX, NAB/WBC/ANZ/MQG PDFs).

Usage:
    python scripts/download_sources/pillar3_downloader.py
    python scripts/download_sources/pillar3_downloader.py --bank cba
    python scripts/download_sources/pillar3_downloader.py --bank nab --force-refresh

URL change log
--------------
- NAB: the disclosure page moved from
    /about-us/shareholder-centre/financial-disclosures/pillar-3-disclosures  (404)
  to
    /about-us/shareholder-centre/regulatory-disclosures
- ANZ: the disclosure page moved from
    /shareholder/centre/reporting/pillar-3-disclosure/  (404)
  to
    /shareholder/centre/reporting/regulatory-disclosure/
- WBC: the disclosure page moved from
    /about-westpac/investor-centre/financial-information/pillar-3-disclosures/  (200
      but no PDF anchors found by previous selector)
  to
    /about-westpac/investor-centre/financial-information/regulatory-disclosures/
- CBA: URL unchanged.

Selector notes
--------------
Matching is case-insensitive and runs against BOTH the anchor visible text
and the href basename. Each bank has a primary glob and a list of fallback
keywords; if the primary glob doesn't match any anchor we try broader
keywords ("pillar 3" + ".pdf"/.xlsx). If a bank still yields no match the
first 500 chars of the landing-page HTML are logged so we can tell whether
the page is JS-rendered (and therefore requires manual download).
"""

from __future__ import annotations

import argparse
import fnmatch
import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from common import logger, safe_download, validate_by_extension  # type: ignore
else:
    from .common import logger, safe_download, validate_by_extension


class Pillar3Downloader:
    """Download Big 4 Pillar 3 disclosures."""

    BANKS: dict[str, dict] = {
        "cba": {
            # Quarterly APS 330 quantitative supplement (post-2025-01-01).
            # Publishes capital/RWA/EAD/NPE in dollars; no PD or LGD.
            "url": "https://www.commbank.com.au/about-us/investors/regulatory-disclosure/pillar-3-capital-disclosures.html",
            "file_pattern": "*pillar*3*quantitative*.xlsx",
            "fallback_keywords": ["pillar 3", "quantitative"],
            "fallback_ext": ".xlsx",
            "format": "xlsx",
        },
        "cba_annual": {
            # Half-year / full-year Pillar 3 PDF — carries CR6 (PD/LGD per
            # portfolio × PD band) and CR10 (specialised-lending slotting).
            # This is the document the engine needs for PD/LGD benchmarks.
            # Matcher must be tight enough to skip the shorter quarterly
            # disclosure PDFs published under the same landing page — those
            # are distinguished by "30 Sept" / "31 March" / "31 December" in
            # the filename vs "30 June" (H2/FY) and "31 December" (H1).
            "url": "https://www.commbank.com.au/about-us/investors/regulatory-disclosure/pillar-3-capital-disclosures.html",
            "file_pattern": "*basel*iii*pillar*3*capital*adequacy*risk*disclosures*june*.pdf",
            "fallback_keywords": ["full year", "30 june", "fy25", "fy26"],
            "fallback_ext": ".pdf",
            "format": "pdf",
        },
        "nab": {
            "url": "https://www.nab.com.au/about-us/shareholder-centre/regulatory-disclosures",
            "file_pattern": "*pillar*3*.pdf",
            "fallback_keywords": ["pillar 3", "pillar-3"],
            "fallback_ext": ".pdf",
            "format": "pdf",
        },
        "wbc": {
            "url": "https://www.westpac.com.au/about-westpac/investor-centre/financial-information/regulatory-disclosures/",
            "file_pattern": "*pillar*3*.pdf",
            "fallback_keywords": ["pillar 3", "pillar-3"],
            "fallback_ext": ".pdf",
            "format": "pdf",
        },
        "anz": {
            "url": "https://www.anz.com/shareholder/centre/reporting/regulatory-disclosure/",
            "file_pattern": "*pillar*3*.pdf",
            "fallback_keywords": ["pillar 3", "pillar-3", "aps 330"],
            "fallback_ext": ".pdf",
            "format": "pdf",
        },
        "mqg": {
            "url": "https://www.macquarie.com/investors/regulatory-disclosures.html",
            "file_pattern": "*pillar*3*disclosures*.pdf",
            "fallback_keywords": ["pillar 3", "pillar-3", "basel iii"],
            "fallback_ext": ".pdf",
            "format": "pdf",
        },
    }

    def __init__(
        self,
        cache_dir: Path = Path("data/raw/pillar3"),
        max_retries: int = 3,
        timeout: int = 30,
    ) -> None:
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.max_retries = max_retries
        self.timeout = timeout
        self.session = requests.Session()
        # A real-browser UA reduces bot-blocking on some bank CDNs.
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0 Safari/537.36 external-benchmark-engine/0.1"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            }
        )

    def download_bank(self, bank: str, force_refresh: bool = False) -> Path | None:
        if bank not in self.BANKS:
            logger.error("Unknown bank %r. Must be one of %s", bank, list(self.BANKS))
            return None

        cfg = self.BANKS[bank]
        response = None
        download_url: str | None = None
        tier = "none"

        if bank == "mqg":
            download_url, tier = self._find_macquarie_api_link()

        if not download_url:
            try:
                response = self.session.get(cfg["url"], timeout=self.timeout)
                response.raise_for_status()
            except requests.RequestException as exc:
                logger.error("Failed to fetch %s Pillar 3 page: %s", bank.upper(), exc)
                return None

            soup = BeautifulSoup(response.content, "html.parser")
            download_url, tier = self._find_download_link(
                soup,
                page_url=cfg["url"],
                file_pattern=cfg["file_pattern"],
                fallback_keywords=cfg["fallback_keywords"],
                fallback_ext=cfg["fallback_ext"],
            )

        if not download_url:
            body_preview = response.text[:500].replace("\n", " ") if response else ""
            logger.warning(
                "%s: no link matched primary glob %r or fallbacks %s. "
                "Page may be JS-rendered. Response preview: %s",
                bank.upper(),
                cfg["file_pattern"],
                cfg["fallback_keywords"],
                body_preview,
            )
            return None

        logger.info("%s matcher (%s) picked: %s", bank.upper(), tier, download_url)

        filename = self._derive_filename(download_url, bank, cfg["format"])
        filepath = self.cache_dir / filename

        if filepath.exists() and not force_refresh:
            logger.info("Using cached %s: %s", bank.upper(), filepath)
            return filepath

        ok = safe_download(
            self.session,
            download_url,
            filepath,
            max_retries=self.max_retries,
            timeout=self.timeout,
            validator=validate_by_extension,
        )
        return filepath if ok else None

    def download_all(self, force_refresh: bool = False) -> dict[str, Path | None]:
        return {
            bank: self.download_bank(bank, force_refresh=force_refresh)
            for bank in self.BANKS
        }

    @staticmethod
    def _find_download_link(
        soup: BeautifulSoup,
        page_url: str,
        file_pattern: str,
        fallback_keywords: list[str],
        fallback_ext: str,
    ) -> tuple[str | None, str]:
        pat = file_pattern.lower()
        ext = fallback_ext.lower()

        anchors = soup.find_all("a")

        # Tier 1 — primary glob on anchor text or href basename.
        for anchor in anchors:
            href = (anchor.get("href", "") or "")
            text = anchor.get_text(" ", strip=True) or ""
            href_tail = href.rsplit("/", 1)[-1].split("?", 1)[0].lower()
            if fnmatch.fnmatch(href_tail, pat) or fnmatch.fnmatch(text.lower(), pat):
                return urljoin(page_url, href), "primary-glob"

        # Tier 2 — fallback: link to correct extension whose text OR href
        # contains any fallback keyword.
        needles = [kw.lower() for kw in fallback_keywords]
        for anchor in anchors:
            href = (anchor.get("href", "") or "").lower()
            href_path = href.split("?", 1)[0]
            text = (anchor.get_text(" ", strip=True) or "").lower()
            if not href_path.endswith(ext):
                continue
            haystack = f"{text} || {href_path}"
            if any(kw in haystack for kw in needles):
                return urljoin(page_url, anchor.get("href", "")), "fallback-keywords"

        return None, "none"

    def _find_macquarie_api_link(self) -> tuple[str | None, str]:
        """Macquarie's regulatory page is React-rendered; query its search API.

        The latest quarter documents (June/December) publish only a subset of
        Pillar 3 tables. For CR6/CR10 parsing we need the March full-year or
        September half-year disclosure, so the API candidate filter excludes
        June and December subset PDFs.
        """
        try:
            response = self.session.get(
                "https://www.macquarie.com/api/search",
                params={
                    "q": "Pillar 3",
                    "c": "macq:investor-disclosures",
                    "size": "50",
                    "from": "0",
                    "currentUrl": "/au/en/investors/regulatory-disclosures.html",
                },
                timeout=self.timeout,
            )
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            logger.warning("MQG: failed to query Macquarie search API: %s", exc)
            return None, "macquarie-api-error"
        except ValueError as exc:
            logger.warning("MQG: Macquarie search API returned non-JSON: %s", exc)
            return None, "macquarie-api-error"

        candidates: list[tuple[int, str]] = []
        for hit in payload.get("hits", []):
            source = hit.get("_source", {})
            target = str(source.get("target-url", ""))
            tags = [str(t).lower() for t in source.get("tags", [])]
            lower_target = target.lower()
            if not lower_target.endswith(".pdf"):
                continue
            if "pillar" not in lower_target:
                continue
            if "macquarie-bank-limited" not in lower_target and "mbl-" not in lower_target:
                continue
            if any(token in lower_target for token in ("dec", "december", "jun", "june")):
                continue
            if not (
                "sep" in lower_target
                or "march" in lower_target
                or "mar-" in lower_target
                or lower_target.endswith("macquarie-bank-limited-pillar-3-disclosures.pdf")
            ):
                continue
            year = 0
            for tag in tags:
                match = re.search(r"/year/(\d{4})|financial-year/(\d{4})", tag)
                if match:
                    year = max(year, int(match.group(1) or match.group(2)))
            candidates.append((year, urljoin("https://www.macquarie.com", target)))

        if not candidates:
            return None, "macquarie-api-no-match"

        candidates.sort(key=lambda item: item[0], reverse=True)
        return candidates[0][1], "macquarie-search-api"

    @staticmethod
    def _derive_filename(url: str, bank: str, fmt: str) -> str:
        tail = urlparse(url).path.rsplit("/", 1)[-1]
        if tail and "." in tail:
            return re.sub(r"[^A-Za-z0-9._-]+", "_", tail)
        return f"{bank.upper()}_Pillar3_latest.{fmt}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Download bank Pillar 3 disclosures")
    parser.add_argument(
        "--bank",
        choices=["cba", "cba_annual", "nab", "wbc", "anz", "mqg", "all"],
        default="all",
    )
    parser.add_argument("--cache-dir", default="data/raw/pillar3")
    parser.add_argument("--force-refresh", action="store_true")
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()

    downloader = Pillar3Downloader(
        cache_dir=Path(args.cache_dir),
        max_retries=args.max_retries,
        timeout=args.timeout,
    )

    if args.bank == "all":
        results = downloader.download_all(force_refresh=args.force_refresh)
        any_ok = False
        for bank, path in results.items():
            if path:
                any_ok = True
                print(f"{bank.upper()}: OK  {path}")
            else:
                print(f"{bank.upper()}: FAIL")
        return 0 if any_ok else 1

    path = downloader.download_bank(args.bank, force_refresh=args.force_refresh)
    if path:
        print(f"{args.bank.upper()}: OK  {path}")
        return 0
    print(f"{args.bank.upper()}: FAIL")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
