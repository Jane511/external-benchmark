"""Macquarie Bank Limited Pillar 3 PDF scraper."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Optional

from ingestion.adapters.mqg_pillar3_pdf_adapter import (
    MqgPillar3PdfAdapter,
    _derive_mqg_period_code,
)
from ingestion.base import ScrapedDataPoint
from ingestion.pillar3.base import PdfPillar3Scraper


class MQGScraper(PdfPillar3Scraper):
    """Macquarie Bank Limited Pillar 3 PDF scraper.

    Parses the regulated ADI disclosure for Macquarie Bank Limited, not the
    wider Macquarie Group Limited consolidated entity.
    """

    PUBLISHER = "Macquarie Bank"
    SOURCE_URL_KEY = "mqg_pillar3"
    PDF_ADAPTER_CLS = MqgPillar3PdfAdapter

    IRB_HEADER_PATTERN = "CR6"
    SLOTTING_HEADER_PATTERN = "CR10"
    IRB_COLUMN_MAP = {
        "asset_class": 0,
        "exposure_ead": 1,
        "pd": 2,
        "lgd": 3,
    }
    SLOTTING_COLUMN_MAP = {"grade": 0, "pd": 1}

    def __init__(
        self,
        source_path: Path | str | None = None,
        config: Optional[dict[str, Any]] = None,
        *,
        reporting_date: Optional[date] = None,
        period_code: Optional[str] = None,
        retrieval_date: Optional[date] = None,
        force_refresh: bool = False,
        cache_base: Path | str = "data/raw",
        **extras: Any,
    ) -> None:
        super().__init__(
            source_path=source_path,
            config=config,
            reporting_date=reporting_date or _default_mqg_reporting_date(),
            period_code=period_code or _derive_mqg_period_code(
                reporting_date or _default_mqg_reporting_date()
            ),
            retrieval_date=retrieval_date,
            force_refresh=force_refresh,
            cache_base=cache_base,
            **extras,
        )

    @property
    def source_name(self) -> str:
        return self._config.get("source_name", "MQG_PILLAR3")

    def scrape(self) -> list[ScrapedDataPoint]:
        path = self._resolve_source_path()
        if not path.exists():
            raise FileNotFoundError(
                f"Macquarie Bank Pillar 3 PDF not found at {path}. "
                "Download the PDF first or pass --source-path."
            )
        if path.suffix.lower() == ".pdf":
            # Keep the same PDF semantics as CBA: the adapter has already
            # applied metric-level plausibility checks, and default-band PDs
            # plus CR10 risk weights are valid even though they sit outside
            # the narrow portfolio validation ranges.
            return self._scrape_via_pdf_adapter(path)
        return self.validate(self._build_points_from_tables(
            self._extract_tables_from_pdf(path)
        ))

    def validate(self, points: list[ScrapedDataPoint]) -> list[ScrapedDataPoint]:
        # Macquarie is parsed at CR6 PD-band granularity. The shared
        # Pillar3BaseScraper ranges are portfolio-average bounds, so they
        # would incorrectly drop low PD bands, non-performing bands, and
        # CR10 risk weights. The adapter already applies metric plausibility.
        return points

    def _resolve_source_path(self) -> Path:
        if self._path is not None:
            return self._path

        from scripts.download_sources.pillar3_downloader import Pillar3Downloader

        cache_dir = self._cache_base / "pillar3"
        path = Pillar3Downloader(cache_dir=cache_dir).download_bank(
            "mqg", force_refresh=self._force_refresh,
        )
        if path is None:
            expected = cache_dir / (
                f"MQG_{'H1' if self._reporting_date.month == 9 else 'H2'}_"
                f"{self._reporting_date.year}_Pillar3.pdf"
            )
            return expected
        return path


def _default_mqg_reporting_date() -> date:
    today = date.today()
    year = today.year if today.month >= 10 else today.year - 1
    return date(year, 9, 30)
