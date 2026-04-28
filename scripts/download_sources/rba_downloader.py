"""Download RBA Financial Stability Review (FSR) and Securitisation system pages.

Usage:
    python scripts/download_sources/rba_downloader.py
    python scripts/download_sources/rba_downloader.py --target fsr
    python scripts/download_sources/rba_downloader.py --target securitisation
    python scripts/download_sources/rba_downloader.py --force-refresh

What this fetches
-----------------
- **FSR**: the latest Financial Stability Review PDF. The FSR landing page
  (https://www.rba.gov.au/publications/fsr/) lists every issue under
  ``/publications/fsr/<year>/<mar|apr|sep|oct>/``. The downloader walks
  the issue list, picks the most recent issue, and grabs that issue's
  ``Download PDF`` link (typically named
  ``financial-stability-review-YYYY-MM.pdf``). The FSR is what the
  ``RbaFsrAggregatesAdapter`` parses for system-wide household + business
  arrears.

- **Securitisation**: the public landing page is saved as HTML so the
  ``RbaSecuritisationAggregatesAdapter`` has a stable artefact to point
  at. The actual disaggregated dataset is gated behind a Securitisation
  System User Agreement; we record that gate in
  ``data/raw/rba/SECURITISATION_GATE.md`` so analysts know to follow up
  through RBA's user-agreement process for raw loan-level data. The
  *aggregates* (which the engine consumes) are published in periodic
  RBA Bulletin / FSR boxes — those flow through this same FSR PDF.

Selector strategy
-----------------
- FSR issue picker: regex on issue URLs ``/fsr/<year>/<month>/``,
  sort by (year desc, month order), pick the first.
- FSR PDF: anchor whose text == ``Download PDF`` and whose href ends in
  ``.pdf`` on the chosen issue page.

Both are public and require no authentication. If the FSR landing page
ever changes layout, the downloader logs the body preview so the URL
selector can be updated without breaking the rest of the pipeline.
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from common import logger, safe_download, validate_pdf  # type: ignore
else:
    from .common import logger, safe_download, validate_pdf


_FSR_LANDING = "https://www.rba.gov.au/publications/fsr/"
_SEC_LANDING = "https://www.rba.gov.au/securitisations/"
_RBA_ROOT = "https://www.rba.gov.au"

_MONTH_ORDER = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


class RbaDownloader:
    """Download RBA FSR PDF and snapshot the Securitisation page."""

    def __init__(
        self,
        cache_dir: Path = Path("data/raw/rba"),
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
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36 external-benchmark-engine/0.2"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    # ------------------------------------------------------------------
    # FSR
    # ------------------------------------------------------------------

    def download_fsr(self, force_refresh: bool = False) -> Path | None:
        """Fetch the most recent FSR PDF into data/raw/rba/."""
        try:
            r = self.session.get(_FSR_LANDING, timeout=self.timeout)
            r.raise_for_status()
        except requests.RequestException as exc:
            logger.error("FSR landing fetch failed: %s", exc)
            return None

        issue_url = self._latest_fsr_issue(r.text)
        if not issue_url:
            logger.warning(
                "FSR: no issue URL matched. Response preview: %s",
                r.text[:400].replace("\n", " "),
            )
            return None
        logger.info("FSR latest issue: %s", issue_url)

        try:
            r2 = self.session.get(issue_url, timeout=self.timeout)
            r2.raise_for_status()
        except requests.RequestException as exc:
            logger.error("FSR issue fetch failed: %s", exc)
            return None

        soup = BeautifulSoup(r2.content, "html.parser")
        pdf_url = None
        for a in soup.select("a"):
            href = (a.get("href") or "").strip()
            text = a.get_text(" ", strip=True)
            if href.endswith(".pdf") and "download pdf" in text.lower():
                pdf_url = urljoin(issue_url, href)
                break

        if not pdf_url:
            logger.warning("FSR issue %s: no Download PDF link", issue_url)
            return None

        filename = pdf_url.rsplit("/", 1)[-1]
        filepath = self.cache_dir / filename
        if filepath.exists() and not force_refresh:
            logger.info("Using cached FSR: %s", filepath)
            return filepath

        ok = safe_download(
            self.session, pdf_url, filepath,
            max_retries=self.max_retries, timeout=self.timeout,
            validator=validate_pdf,
        )
        return filepath if ok else None

    @staticmethod
    def _latest_fsr_issue(html: str) -> str | None:
        """Pick the most recent issue URL from the FSR landing HTML.

        Issue URLs have the shape ``/publications/fsr/<YYYY>/<mon>/`` where
        ``<mon>`` is e.g. ``mar`` or ``sep``. Sort by (year desc, month desc).
        """
        soup = BeautifulSoup(html, "html.parser")
        seen: set[tuple[int, int, str]] = set()
        for a in soup.select("a"):
            href = (a.get("href") or "").strip()
            m = re.match(r"^/publications/fsr/(\d{4})/([a-z]{3})/?$", href)
            if not m:
                continue
            year = int(m.group(1))
            month_key = m.group(2).lower()
            month_num = _MONTH_ORDER.get(month_key, 0)
            seen.add((year, month_num, href))
        if not seen:
            return None
        # Sort descending by (year, month)
        latest = sorted(seen, reverse=True)[0]
        return urljoin(_RBA_ROOT, latest[2])

    # ------------------------------------------------------------------
    # Securitisation system page snapshot + gate note
    # ------------------------------------------------------------------

    def snapshot_securitisation_page(self, force_refresh: bool = False) -> Path | None:
        """Save the Securitisation landing HTML and write a gate note.

        The disaggregated Securitisation System dataset requires a signed
        User Agreement with the RBA — it cannot be fetched anonymously.
        We snapshot the public-facing landing page and record the access
        gate so the user can decide whether to engage with RBA for
        loan-level data, or stick to FSR aggregates (which this engine
        already consumes).
        """
        html_path = self.cache_dir / "securitisation_landing.html"
        if html_path.exists() and not force_refresh:
            logger.info("Using cached securitisation landing: %s", html_path)
            return html_path

        try:
            r = self.session.get(_SEC_LANDING, timeout=self.timeout)
            r.raise_for_status()
        except requests.RequestException as exc:
            logger.error("Securitisation landing fetch failed: %s", exc)
            return None
        html_path.write_text(r.text, encoding="utf-8")
        logger.info("Snapshot saved: %s (%d B)", html_path, html_path.stat().st_size)

        gate_path = self.cache_dir / "SECURITISATION_GATE.md"
        gate_path.write_text(_SECURITISATION_GATE_NOTE.format(
            captured_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        ), encoding="utf-8")
        return html_path


_SECURITISATION_GATE_NOTE = """# RBA Securitisation System — access gate

Captured: {captured_at}

The disaggregated Securitisation System dataset (loan-level RMBS / ABS
performance) requires a signed **Securitisation System User Agreement**
with the RBA. It is not fetchable via anonymous HTTP.

The user-agreement form lives at:
  https://www.rba.gov.au/securitisations/files/securitisation-system-user-agreement.pdf

What this engine consumes today is the **aggregate** view — household and
business arrears published in the RBA Financial Stability Review (FSR)
PDF and the periodic RBA Bulletin boxes. The
``RbaFsrAggregatesAdapter`` reads those FSR PDFs (downloaded via
``rba_downloader.py --target fsr``) and emits ``arrears_*`` observations.

If/when loan-level Securitisation data is needed, the analyst must:
  1. Sign the User Agreement (annual renewal).
  2. Receive credentialed access from RBA.
  3. Add a credentialed-fetch path here that reads from the gated
     endpoint (do not commit credentials).
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Download RBA FSR + Securitisation")
    parser.add_argument(
        "--target", choices=["fsr", "securitisation", "all"], default="all",
    )
    parser.add_argument("--cache-dir", default="data/raw/rba")
    parser.add_argument("--force-refresh", action="store_true")
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()

    dl = RbaDownloader(
        cache_dir=Path(args.cache_dir),
        max_retries=args.max_retries,
        timeout=args.timeout,
    )

    any_ok = False
    if args.target in ("fsr", "all"):
        p = dl.download_fsr(force_refresh=args.force_refresh)
        if p:
            print(f"FSR: OK  {p}")
            any_ok = True
        else:
            print("FSR: FAIL")

    if args.target in ("securitisation", "all"):
        p = dl.snapshot_securitisation_page(force_refresh=args.force_refresh)
        if p:
            print(f"SECURITISATION: OK  {p}")
            any_ok = True
        else:
            print("SECURITISATION: FAIL")

    return 0 if any_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
