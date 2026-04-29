"""Capture APRA Insight and CFR publications.

Both feeds publish irregularly. This downloader walks each landing page,
diffs against the per-source ``_manifest.json`` in the cache directory, and
captures only entries the manifest doesn't already record. Each captured
file gets one ``audit_log`` row and one new manifest entry.

Usage:
    python scripts/download_sources/governance_publications_downloader.py
    python scripts/download_sources/governance_publications_downloader.py --target apra_insight
    python scripts/download_sources/governance_publications_downloader.py --target cfr_publications
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.adapters.apra_insight_adapter import (  # noqa: E402
    ApraInsightScraper,
    CapturedPublication,
)
from ingestion.adapters.cfr_publications_adapter import (  # noqa: E402
    CfrPublicationsScraper,
)


_SCRAPERS = {
    "apra_insight": ApraInsightScraper,
    "cfr_publications": CfrPublicationsScraper,
}


class GovernancePublicationsDownloader:
    """Drive APRA Insight + CFR scrapers in one batch."""

    def __init__(
        self,
        cache_base: Path = Path("data/raw"),
        db_path: Path | str | None = "benchmarks.db",
        actor: str = "governance_publications_downloader",
    ) -> None:
        self.cache_base = Path(cache_base)
        self.db_path = db_path
        self.actor = actor

    def download_source(self, source: str) -> list[CapturedPublication]:
        if source not in _SCRAPERS:
            raise ValueError(f"Unknown governance source {source!r}")
        scraper = _SCRAPERS[source](
            cache_base=self.cache_base,
            audit_db_path=self.db_path,
            actor=self.actor,
        )
        return scraper.run()

    def download_all(self) -> dict[str, list[CapturedPublication]]:
        return {key: self.download_source(key) for key in _SCRAPERS}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Capture APRA Insight + CFR publications (newest-first, manifest-deduped)",
    )
    parser.add_argument(
        "--target",
        "--source",
        dest="target",
        choices=[*_SCRAPERS.keys(), "all"],
        default="all",
    )
    parser.add_argument("--cache-base", default="data/raw")
    parser.add_argument("--db", default="benchmarks.db")
    args = parser.parse_args()

    downloader = GovernancePublicationsDownloader(
        cache_base=Path(args.cache_base),
        db_path=args.db,
    )

    targets = list(_SCRAPERS) if args.target == "all" else [args.target]
    any_run = False
    for key in targets:
        try:
            captured = downloader.download_source(key)
        except Exception as exc:  # noqa: BLE001
            print(f"{key}: FAIL  {exc}")
            continue
        any_run = True
        if not captured:
            print(f"{key}: OK  no new publications")
            continue
        print(f"{key}: OK  captured {len(captured)} new publication(s)")
        for row in captured:
            print(f"  - {row.published_date or '????-??-??'}  {row.title}  ({row.local_path})")
    return 0 if any_run else 1


if __name__ == "__main__":
    raise SystemExit(main())
