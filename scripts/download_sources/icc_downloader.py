"""ICC Trade Register download guide.

**Manual download by design.** All other sources in
``scripts/download_sources/`` auto-fetch; ICC stays manual because:

- 2025+ editions are paywalled (EUR 2,500+, manually delivered by ICC
  within 24h of payment confirmation).
- The free 2024 edition sits behind a registration form (name, email,
  institution) plus email-verification.
- Annual cadence — one file per year — does not justify building a
  form-scraper that's inevitably brittle against CAPTCHAs, email
  verification flows, and layout changes.

This script reports what is already cached under ``data/raw/icc/`` and
prints manual-download instructions. It does **not** attempt to
auto-download, and will not be refactored into one unless ICC moves to a
stable direct-link publication model.

Usage:
    python scripts/download_sources/icc_downloader.py
    python scripts/download_sources/icc_downloader.py --year 2024
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from common import logger  # type: ignore  # noqa: F401
else:
    from .common import logger  # noqa: F401


class IccTradeDownloader:
    """Guide manual ICC Trade Register download."""

    BASE_URL = (
        "https://iccwbo.org/news-publications/policies-reports/icc-trade-register-report/"
    )
    CACHE_DIR = Path("data/raw/icc")

    @classmethod
    def check_cached(cls, year: int | None = None) -> list[Path]:
        cls.CACHE_DIR.mkdir(parents=True, exist_ok=True)
        pattern = (
            f"ICC_Trade_Register_{year}.pdf" if year else "ICC_Trade_Register_*.pdf"
        )
        return sorted(cls.CACHE_DIR.glob(pattern))

    @classmethod
    def guide_manual_download(cls) -> None:
        cached = cls.check_cached()
        bar = "=" * 70
        print(f"\n{bar}")
        print("ICC Trade Register Download Instructions")
        print(f"{bar}\n")

        if cached:
            print("Cached ICC files:")
            now = datetime.now()
            for path in cached:
                age_days = (now - datetime.fromtimestamp(path.stat().st_mtime)).days
                print(f"  OK  {path.name} ({age_days} days old)")
            print()
        else:
            print(f"No cached ICC files in {cls.CACHE_DIR}\n")

        print("The ICC Trade Register is a commercial publication.")
        print("Free editions (2024 and prior) can be downloaded from:")
        print(f"  {cls.BASE_URL}\n")
        print("To use the data in the external benchmark engine:")
        print("  1. Download the PDF from the URL above")
        print(f"  2. Save as: {cls.CACHE_DIR}/ICC_Trade_Register_YYYY.pdf")
        print("     (e.g., ICC_Trade_Register_2024.pdf)")
        print("  3. Run: benchmark ingest icc --report-year YYYY\n")
        print("For 2025+, you may need to purchase the latest edition.")
        print(f"Contact ICC at {cls.BASE_URL} for pricing and access.")
        print(f"{bar}\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="ICC Trade Register download guide")
    parser.add_argument("--year", type=int, default=None)
    args = parser.parse_args()

    if args.year is not None:
        cached = IccTradeDownloader.check_cached(args.year)
        if cached:
            print(f"OK   Found: {cached[0]}")
            return 0
        print(f"MISS Not cached for year {args.year}")
        return 1

    IccTradeDownloader.guide_manual_download()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
