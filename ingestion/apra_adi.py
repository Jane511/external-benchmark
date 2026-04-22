"""APRA ADI Performance + Property Exposures scraper (Tier 1).

Two input shapes are supported:

1. **Canonical fixture** — a single sheet matching
   ``self._config["sheet"]`` (e.g. ``"Asset Quality"``) with columns
   ``Period | Category | <metric columns>``. This is what every existing
   fixture-based test produces, and it feeds the direct-read path
   unchanged.

2. **Live APRA workbook** — the real quarterly statistics release, whose
   layout is wide-format with per-sector ``Tab Xd`` sheets. Detected by
   the absence of the canonical sheet; the scraper routes through
   ``ingestion.adapters.apra_performance_adapter.ApraPerformanceAdapter``
   (Path A) which emits a long-format DataFrame of
   ``adi_sector_total`` rows.

QPEX (Property Exposures) live support is deferred to Path B — see
``outputs/apra_workbook_structure.md``. Until then, ``--source-key
apra_qpex`` against a live file logs a warning and yields zero points.

Live download is supported via ``fetch()``; in production the
orchestrator downloads the XLSX into a local cache directory and then
hands the path to the scraper. Tests bypass download entirely by
constructing an XLSX fixture programmatically and pointing
``source_path`` at it.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

import openpyxl
import pandas as pd

from ingestion.adapters.apra_performance_adapter import (
    SYNTHETIC_ASSET_CLASS,
    ApraPerformanceAdapter,
)
from ingestion.adapters.apra_qpex_adapter import ApraQpexAdapter
from ingestion.base import BaseScraper, ScrapedDataPoint
from ingestion.downloader import FileDownloader
from ingestion.source_registry import SOURCE_URLS

logger = logging.getLogger(__name__)


class ApraAdiScraper(BaseScraper):
    """Parses an APRA ADI Statistics XLSX and emits ScrapedDataPoints."""

    def __init__(
        self,
        source_path: Path | str | None = None,
        config: Optional[dict[str, Any]] = None,
        *,
        retrieval_date: Optional[date] = None,
        force_refresh: bool = False,
        cache_base: Path | str = "data/raw",
        **extras: Any,                         # tolerate unknown orchestrator kwargs
    ) -> None:
        if config is None:
            raise ValueError("ApraAdiScraper requires a `config` dict")
        self._path: Optional[Path] = Path(source_path) if source_path else None
        self._config = config
        self._retrieval_date = retrieval_date or date.today()
        self._force_refresh = force_refresh
        self._cache_base = Path(cache_base)

    # ------------------------------------------------------------------
    # BaseScraper contract
    # ------------------------------------------------------------------

    @property
    def source_name(self) -> str:
        return self._config.get("source_name", "APRA_ADI")

    @property
    def expected_frequency_days(self) -> int:
        return int(self._config.get("frequency_days", 120))

    def scrape(self) -> list[ScrapedDataPoint]:
        """Read the source XLSX and emit ScrapedDataPoints.

        Detection: if the configured canonical sheet (e.g. ``"Asset
        Quality"``) is present, use the direct-read path — this keeps
        every fixture-based test on its original code path. Otherwise the
        file is a live APRA release and we route through the adapter.
        """
        path = self._resolve_source_path()
        if not path.exists():
            raise FileNotFoundError(
                f"APRA XLSX not found at {path}. Download first or pass a fixture path."
            )

        canonical_sheet = self._config.get("sheet")
        if self._has_canonical_sheet(path, canonical_sheet):
            return self._scrape_canonical(path)
        return self._scrape_via_adapter(path)

    @staticmethod
    def _has_canonical_sheet(path: Path, sheet_name: str | None) -> bool:
        """Return True iff ``sheet_name`` exists in the workbook at ``path``."""
        if not sheet_name:
            return False
        try:
            wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        except Exception:
            return False
        try:
            return sheet_name in wb.sheetnames
        finally:
            wb.close()

    # ------------------------------------------------------------------
    # Path 1: canonical fixture (existing behaviour)
    # ------------------------------------------------------------------

    def _scrape_canonical(self, path: Path) -> list[ScrapedDataPoint]:
        df = pd.read_excel(path, sheet_name=self._config["sheet"])
        period_col = self._config["period_column"]
        category_col = self._config["category_column"]
        metrics = self._config["metrics"]
        url = self._config.get("url", "")

        points: list[ScrapedDataPoint] = []
        for _, row in df.iterrows():
            period = _to_date(row[period_col])
            category = str(row[category_col])
            for metric_spec in metrics:
                column = metric_spec["column"]
                if column not in row:
                    continue
                raw_value = row[column]
                if pd.isna(raw_value):
                    continue
                points.append(ScrapedDataPoint(
                    source_name=(
                        f"APRA_{category.upper()}_{metric_spec['metric'].upper()}"
                    ),
                    publisher="APRA",
                    raw_value=float(raw_value),
                    raw_unit=metric_spec.get("unit", "ratio"),
                    value_date=period,
                    period_years=1,
                    asset_class_raw=category,
                    geography="AU",
                    url=url,
                    retrieval_date=self._retrieval_date,
                    quality_indicators={"coverage": "all_adis"},
                    metadata={
                        "data_type_hint": metric_spec["data_type"],
                        "metric_column": column,
                    },
                ))
        return points

    # ------------------------------------------------------------------
    # Path 2: live APRA workbook — dispatch to Performance (Path A) or
    # QPEX (Path B) adapter based on the sources.yaml source_name.
    # ------------------------------------------------------------------

    def _scrape_via_adapter(self, path: Path) -> list[ScrapedDataPoint]:
        source_key = self._config.get("source_name", "").lower()
        file_name = self._config.get("file_name", "ADI Performance")

        is_qpex_source = (
            "qpex" in source_key
            or "property" in source_key
            or file_name == "Property Exposures"
        )
        if is_qpex_source:
            return self._scrape_qpex(path)

        is_performance_source = (
            file_name == "ADI Performance"
            or "performance" in source_key
        )
        if is_performance_source:
            return self._scrape_performance(path)

        logger.warning(
            "APRA live file detected but source_name=%r / file_name=%r "
            "matches neither Performance nor QPEX adapter — yielding 0 points.",
            source_key, file_name,
        )
        return []

    def _scrape_performance(self, path: Path) -> list[ScrapedDataPoint]:
        adapter = ApraPerformanceAdapter()
        df = adapter.normalise(path)
        if df.empty:
            logger.warning(
                "APRA Performance adapter returned 0 rows for %s", path.name,
            )
            return []

        url = self._config.get("url", "")
        points: list[ScrapedDataPoint] = []
        for _, row in df.iterrows():
            sector = str(row["institution_sector"])
            metric = str(row["metric_name"])
            value = float(row["value"])
            as_of = row["as_of_date"]
            sheet = str(row.get("_source_sheet", ""))
            source_row = int(row["_source_row"]) if "_source_row" in row else -1
            period_slug = str(row["period"])

            data_type_hint = "npl" if metric == "npl_ratio" else "90dpd"

            points.append(ScrapedDataPoint(
                source_name=f"APRA_{sector.upper()}_{metric.upper()}",
                publisher="APRA",
                raw_value=value,
                raw_unit="ratio",
                value_date=as_of,
                period_years=1,
                asset_class_raw=SYNTHETIC_ASSET_CLASS,
                geography="AU",
                url=url,
                retrieval_date=self._retrieval_date,
                quality_indicators={
                    "coverage": "apra_sector_aggregate",
                    "sector": sector,
                    "source_sheet": sheet,
                    "aps220_row": source_row,
                    "adapter": "ApraPerformanceAdapter",
                },
                metadata={
                    "data_type_hint": data_type_hint,
                    # metric_column becomes metric_code in source_id.
                    # Including the sector keeps source_ids unique across
                    # sectors even though they share asset_class.
                    "metric_column": f"{sector}_{metric}",
                    "period_code": period_slug,
                    "note": (
                        "APRA sector aggregate via ApraPerformanceAdapter "
                        "(Path A). Asset-class-keyed benchmarks come from "
                        "QPEX (Path B) or Pillar 3."
                    ),
                },
            ))
        return points

    def _scrape_qpex(self, path: Path) -> list[ScrapedDataPoint]:
        """Path B: compute NPL ratio per sector × asset class via QPEX arithmetic."""
        adapter = ApraQpexAdapter()
        df = adapter.normalise(path)
        if df.empty:
            logger.warning(
                "APRA QPEX adapter returned 0 rows for %s", path.name,
            )
            return []

        url = self._config.get("url", "")
        points: list[ScrapedDataPoint] = []
        for _, row in df.iterrows():
            sector = str(row["institution_sector"])
            asset_class = str(row["asset_class"])
            metric = str(row["metric_name"])
            value = float(row["value"])
            as_of = row["as_of_date"]
            sheet = str(row.get("_source_sheet", ""))
            num_row = int(row["_numerator_row"]) if "_numerator_row" in row else -1
            den_row = int(row["_denominator_row"]) if "_denominator_row" in row else -1
            numer = float(row["numerator_value"])
            denom = float(row["denominator_value"])
            period_slug = str(row["period"])

            points.append(ScrapedDataPoint(
                source_name=(
                    f"APRA_QPEX_{asset_class.upper()}_"
                    f"{sector.upper()}_{metric.upper()}"
                ),
                publisher="APRA",
                raw_value=value,
                raw_unit="ratio",
                value_date=as_of,
                period_years=1,
                asset_class_raw=asset_class,   # REAL enum value (no synthetic class)
                geography="AU",
                url=url,
                retrieval_date=self._retrieval_date,
                quality_indicators={
                    "coverage": "apra_qpex_asset_class",
                    "sector": sector,
                    "source_sheet": sheet,
                    "numerator_row": num_row,
                    "denominator_row": den_row,
                    "numerator_value": numer,
                    "denominator_value": denom,
                    "arithmetic": ApraQpexAdapter.ARITHMETIC_FORMULA,
                    "adapter": "ApraQpexAdapter",
                },
                metadata={
                    "data_type_hint": "npl",
                    # The sector disambiguates source_ids that share
                    # (asset_class, data_type) across ADI sectors.
                    "metric_column": f"qpex_{sector}_{metric}",
                    "period_code": period_slug,
                    "note": (
                        "APRA QPEX asset-class NPL ratio via row arithmetic. "
                        "Definition aligns with APS 220 'non-performing' "
                        "exposures. Numerator / denominator preserved in "
                        "quality_indicators for audit."
                    ),
                },
            ))
        return points

    def validate(self, points: list[ScrapedDataPoint]) -> list[ScrapedDataPoint]:
        """Drop rows whose value falls outside the per-metric validation_range."""
        metric_ranges = {
            m["metric"]: tuple(m.get("validation_range", [0.0, 1.0]))
            for m in self._config["metrics"]
        }
        valid: list[ScrapedDataPoint] = []
        for p in points:
            metric_hint = p.metadata.get("data_type_hint") or p.source_name.lower()
            # recover the metric key from the source_name suffix
            metric_key = p.source_name.rsplit("_", 1)[-1].lower()
            lo, hi = metric_ranges.get(metric_key, (0.0, 1.0))
            if lo <= p.raw_value <= hi:
                valid.append(p)
        return valid

    # ------------------------------------------------------------------
    # Source-path resolution (cache-aware)
    # ------------------------------------------------------------------

    def _resolve_source_path(self) -> Path:
        """Return the XLSX path, downloading into cache first if needed."""
        if self._path is not None:
            return self._path
        # Config must identify which file in SOURCE_URLS['apra_adi']['files'].
        # `file_name` defaults to 'ADI Performance' for the Performance sheet
        # and is overridden in sources.yaml for the QPEX variant.
        file_name = self._config.get("file_name", "ADI Performance")
        src_info = SOURCE_URLS["apra_adi"]
        file_spec = next(
            (f for f in src_info["files"] if f["name"] == file_name), None,
        )
        if file_spec is None:
            raise ValueError(
                f"Unknown APRA file_name={file_name!r}. Expected one of "
                f"{[f['name'] for f in src_info['files']]}."
            )

        rd = self._retrieval_date
        quarter = f"Q{(rd.month - 1) // 3 + 1}"
        filename = file_spec["filename_pattern"].format(
            quarter=quarter, year=rd.year,
        )
        downloader = FileDownloader(self._cache_base, "apra")
        path = downloader.download_and_cache(
            file_spec["url"], filename, force_refresh=self._force_refresh,
        )
        self._path = path
        return path

    # ------------------------------------------------------------------
    # Optional live download (legacy helper; preserved for backwards compat)
    # ------------------------------------------------------------------

    @staticmethod
    def fetch(url: str, destination: Path | str) -> Path:
        """Download an APRA XLSX directly to `destination`. Requires `requests`.

        Retained for scripts that invoked this before the caching layer landed;
        new code should let the scraper's `_resolve_source_path()` handle it.
        """
        import requests  # lazy import

        destination = Path(destination)
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(response.content)
        return destination


def _to_date(value: Any) -> date:
    """Coerce pandas/Excel date-like values to a plain `datetime.date`.

    Order matters: pandas Timestamp inherits from datetime which inherits
    from date, so the date check would mis-match a Timestamp. Check datetime
    first and call .date() to collapse to a plain date.
    """
    if isinstance(value, datetime):        # catches pd.Timestamp too
        return value.date()
    if isinstance(value, date):
        return value
    return pd.to_datetime(value).date()
