"""Download RBA FSR, Statement on Monetary Policy, and Chart Pack PDFs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.adapters.rba_publications_adapter import (  # noqa: E402
    RbaChartPackScraper,
    RbaFsrScraper,
    RbaSmpScraper,
)


_SCRAPERS = {
    "rba_fsr": RbaFsrScraper,
    "rba_smp": RbaSmpScraper,
    "rba_chart_pack": RbaChartPackScraper,
}
_ALIASES = {
    "fsr": "rba_fsr",
    "smp": "rba_smp",
    "chart_pack": "rba_chart_pack",
    "chart-pack": "rba_chart_pack",
}


class RbaDownloader:
    """Small facade retained for imports and scripts."""

    def __init__(
        self,
        cache_dir: Path = Path("data/raw/rba"),
        max_retries: int = 3,
        timeout: int = 30,
        db_path: Path | str | None = "benchmarks.db",
    ) -> None:
        self.cache_dir = Path(cache_dir)
        self.max_retries = max_retries
        self.timeout = timeout
        self.db_path = db_path

    def download_source(self, source: str, *, force_refresh: bool = False):
        key = _ALIASES.get(source, source)
        if key not in _SCRAPERS:
            raise ValueError(f"Unknown RBA source {source!r}")
        cache_base = self.cache_dir.parent if self.cache_dir.name == "rba" else self.cache_dir
        scraper = _SCRAPERS[key](cache_base=cache_base, audit_db_path=self.db_path)
        return scraper.run(force_refresh=force_refresh)

    def download_all(self, *, force_refresh: bool = False):
        return {
            key: self.download_source(key, force_refresh=force_refresh)
            for key in _SCRAPERS
        }

    def download_fsr(self, force_refresh: bool = False):
        return self.download_source("rba_fsr", force_refresh=force_refresh)


def main() -> int:
    parser = argparse.ArgumentParser(description="Download RBA publication PDFs")
    parser.add_argument(
        "--target",
        "--source",
        dest="target",
        choices=["rba_fsr", "rba_smp", "rba_chart_pack", "fsr", "smp", "chart_pack", "all"],
        default="all",
    )
    parser.add_argument("--cache-dir", default="data/raw/rba")
    parser.add_argument("--db", default="benchmarks.db")
    parser.add_argument("--force-refresh", action="store_true")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--max-retries", type=int, default=3)  # kept for CLI compatibility
    args = parser.parse_args()

    downloader = RbaDownloader(
        cache_dir=Path(args.cache_dir),
        max_retries=args.max_retries,
        timeout=args.timeout,
        db_path=args.db,
    )

    targets = list(_SCRAPERS) if args.target == "all" else [_ALIASES.get(args.target, args.target)]
    any_ok = False
    for key in targets:
        try:
            result = downloader.download_source(key, force_refresh=args.force_refresh)
        except Exception as exc:  # noqa: BLE001
            print(f"{key}: FAIL  {exc}")
            continue
        any_ok = True
        print(f"{key}: OK  {result.local_cached_file}  period={result.period}")
    return 0 if any_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
