"""CBA Pillar 3 Excel companion parser.

Three input shapes are supported:

1. **Canonical fixture XLSX** — the tests' synthetic workbook with
   `IRB Credit Risk` + `Specialised Lending` sheets. Feeds the existing
   direct-read path unchanged.

2. **Live quarterly XLSX** (post-2025-01-01 APS 330 supplement) — sheet
   codes like `EAD & CRWA`, `CRB(f)(ii)`, etc. Publishes no PD or LGD;
   the engine computes portfolio-level NPL ratios via the ``CbaPillar3QuarterlyAdapter``
   (Option A — arithmetic, mirrors the APRA QPEX pattern).

3. **Live half-year / full-year PDF** — carries CR6 (PD + LGD per
   portfolio × PD band) and CR10 (specialised-lending slotting risk
   weights). Handled by ``CbaPillar3PdfAdapter`` (Option B). This is
   the primary source of segment-specific PD and LGD for the engine.

Detection is by file extension + sheet heuristic; see ``scrape()``.
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Any, Optional

import openpyxl
import pandas as pd

from ingestion.adapters.cba_pillar3_pdf_adapter import CbaPillar3PdfAdapter
from ingestion.adapters.cba_pillar3_quarterly_adapter import (
    CbaPillar3QuarterlyAdapter,
)
from ingestion.base import ScrapedDataPoint
from ingestion.downloader import FileDownloader
from ingestion.pillar3.base import Pillar3BaseScraper, default_cba_period_code
from ingestion.source_registry import SOURCE_URLS

logger = logging.getLogger(__name__)


class CBAScraper(Pillar3BaseScraper):
    """Extracts PD / LGD / slotting grades from CBA's Excel companion."""

    PUBLISHER = "CBA"

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
        **extras: Any,                         # tolerate unknown orchestrator kwargs
    ) -> None:
        if config is None:
            raise ValueError("CBAScraper requires a `config` dict")
        super().__init__(config)
        self._path: Optional[Path] = Path(source_path) if source_path else None
        # Default to CBA's most common reporting date (June 30) for tests.
        self._reporting_date = reporting_date or date(date.today().year, 6, 30)
        self._period_code = period_code or default_cba_period_code(self._reporting_date)
        self._retrieval_date = retrieval_date or date.today()
        self._force_refresh = force_refresh
        self._cache_base = Path(cache_base)

    @property
    def source_name(self) -> str:
        return self._config.get("source_name", "CBA_PILLAR3")

    # ------------------------------------------------------------------
    # scrape()
    # ------------------------------------------------------------------

    def scrape(self) -> list[ScrapedDataPoint]:
        path = self._resolve_source_path()
        if not path.exists():
            raise FileNotFoundError(
                f"CBA Pillar 3 source not found at {path}. "
                "Download the Excel companion or annual PDF first, or pass "
                "a fixture path."
            )

        self._path = path

        # Route by file type:
        # - .pdf → PDF adapter (CR6/CR10 — the primary PD/LGD source)
        # - .xlsx → canonical fixture (existing tests) OR live quarterly
        #           XLSX → quarterly adapter (NPL arithmetic)
        suffix = path.suffix.lower()
        if suffix == ".pdf":
            return self._scrape_via_pdf_adapter(path)
        if suffix == ".xlsx":
            if self._has_canonical_sheets(path):
                return self._scrape_canonical()
            return self._scrape_via_quarterly_adapter(path)

        logger.warning(
            "CBA source path %s has unexpected extension %r — no adapter matched",
            path, suffix,
        )
        return []

    # ------------------------------------------------------------------
    # Canonical fixture path (existing behaviour)
    # ------------------------------------------------------------------

    def _scrape_canonical(self) -> list[ScrapedDataPoint]:
        points: list[ScrapedDataPoint] = []
        points.extend(self._scrape_irb())
        if "slotting_sheet" in self._config:
            points.extend(self._scrape_slotting())
        return points

    @staticmethod
    def _has_canonical_sheets(path: Path) -> bool:
        """Return True iff the workbook carries the fixture sheet names."""
        try:
            wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        except Exception:
            return False
        try:
            return "IRB Credit Risk" in wb.sheetnames
        finally:
            wb.close()

    # ------------------------------------------------------------------
    # Live PDF adapter path (Option B — CR6 PD/LGD + CR10 risk weights)
    # ------------------------------------------------------------------

    def _scrape_via_pdf_adapter(self, path: Path) -> list[ScrapedDataPoint]:
        adapter = CbaPillar3PdfAdapter()
        df = adapter.normalise(path, reporting_date=self._reporting_date)
        if df.empty:
            logger.warning("CBA PDF adapter returned 0 rows for %s", path.name)
            return []

        url = self._config.get("url", "")
        points: list[ScrapedDataPoint] = []
        for _, row in df.iterrows():
            asset_class = str(row["asset_class"])
            metric_name = str(row["metric_name"])
            value = float(row["value"])
            source_table = str(row["source_table"])
            source_page = int(row["source_page"])
            pd_band = str(row["pd_band"])
            period_code = str(row["period_code"])

            data_type_hint = {"pd": "pd", "lgd": "lgd",
                              "risk_weight": "supervisory"}[metric_name]

            # metric_column disambiguates same-portfolio source_ids across
            # PD bands (each band produces a distinct entry).
            band_slug = (
                pd_band.replace(" ", "_").replace("<", "lt")
                .replace("(", "").replace(")", "").replace(".", "p")
            )
            metric_column = f"{band_slug}_{metric_name}" if metric_name != "risk_weight" else metric_name

            points.append(ScrapedDataPoint(
                source_name=(
                    f"CBA_{asset_class.upper()}_{metric_name.upper()}_{band_slug.upper()}"
                ),
                publisher=self.PUBLISHER,
                raw_value=value,
                raw_unit="ratio",
                value_date=self._reporting_date,
                period_years=1,
                asset_class_raw=asset_class,
                geography="AU",
                url=url,
                retrieval_date=self._retrieval_date,
                quality_indicators={
                    "coverage": "cba_pillar3_annual_pdf",
                    "source_table": source_table,
                    "source_page": source_page,
                    "pd_band": pd_band,
                    "value_basis": str(row["value_basis"]),
                    "adapter": "CbaPillar3PdfAdapter",
                },
                metadata={
                    "data_type_hint": data_type_hint,
                    "metric_column": metric_column,
                    "period_code": period_code,
                    "note": (
                        f"CBA {source_table} — PD band {pd_band!r}. "
                        "Published PD/LGD is exposure-weighted; slotting "
                        "risk weights are APS 113 prescribed."
                    ),
                },
            ))
        return points

    # ------------------------------------------------------------------
    # Live quarterly XLSX adapter path (Option A — NPL arithmetic)
    # ------------------------------------------------------------------

    def _scrape_via_quarterly_adapter(self, path: Path) -> list[ScrapedDataPoint]:
        adapter = CbaPillar3QuarterlyAdapter()
        df = adapter.normalise(path, reporting_date=self._reporting_date)
        if df.empty:
            logger.warning(
                "CBA quarterly adapter returned 0 rows for %s", path.name,
            )
            return []

        url = self._config.get("url", "")
        points: list[ScrapedDataPoint] = []
        for _, row in df.iterrows():
            asset_class = str(row["asset_class"])
            metric_name = str(row["metric_name"])
            value = float(row["value"])
            period_code = str(row["period_code"])
            numer = float(row["numerator_value"])
            denom = float(row["denominator_value"])

            points.append(ScrapedDataPoint(
                source_name=f"CBA_QUARTERLY_{asset_class.upper()}_{metric_name.upper()}",
                publisher=self.PUBLISHER,
                raw_value=value,
                raw_unit="ratio",
                value_date=self._reporting_date,
                period_years=1,
                asset_class_raw=asset_class,
                geography="AU",
                url=url,
                retrieval_date=self._retrieval_date,
                quality_indicators={
                    "coverage": "cba_pillar3_quarterly_xlsx",
                    "numerator_sheet": str(row["numerator_sheet"]),
                    "denominator_sheet": str(row["denominator_sheet"]),
                    "numerator_value": numer,
                    "denominator_value": denom,
                    "arithmetic": CbaPillar3QuarterlyAdapter.ARITHMETIC_FORMULA,
                    "adapter": "CbaPillar3QuarterlyAdapter",
                },
                metadata={
                    "data_type_hint": "npl",
                    "metric_column": f"quarterly_{metric_name}",
                    "period_code": period_code,
                    "note": (
                        "CBA quarterly APS 330 supplement — NPL ratio = "
                        "CRB(f)(ii) non-performing / EAD & CRWA EAD. "
                        "Arithmetic audit trail preserved."
                    ),
                },
            ))
        return points

    # ------------------------------------------------------------------
    # Source-path resolution (cache-aware)
    # ------------------------------------------------------------------

    def _resolve_source_path(self) -> Path:
        """Return the XLSX path; auto-download into cache when no path supplied.

        Half (H1/H2) and calendar year are derived from the reporting_date:
            Jan–Jun -> H1, Jul–Dec -> H2.
        """
        if self._path is not None:
            return self._path

        src_info = SOURCE_URLS["cba_pillar3"]
        file_spec = src_info["files"][0]
        rd = self._reporting_date
        half = "H1" if rd.month <= 6 else "H2"
        filename = file_spec["filename_pattern"].format(half=half, year=rd.year)

        downloader = FileDownloader(self._cache_base, "pillar3")
        return downloader.download_and_cache(
            file_spec["url"], filename, force_refresh=self._force_refresh,
        )

    # ------------------------------------------------------------------
    # IRB credit risk (residential / CRE / corporate SME)
    # ------------------------------------------------------------------

    def _scrape_irb(self) -> list[ScrapedDataPoint]:
        df = pd.read_excel(self._path, sheet_name=self._config["irb_sheet"])

        portfolio_col = self._config["portfolio_column"]
        pd_col = self._config["pd_column"]
        lgd_col = self._config.get("lgd_column")

        points: list[ScrapedDataPoint] = []
        for _, row in df.iterrows():
            portfolio = str(row[portfolio_col]).strip()

            pd_value = row.get(pd_col)
            if pd.notna(pd_value):
                points.append(self._build_point(
                    asset_class_raw=portfolio,
                    metric="PD",
                    raw_value=float(pd_value),
                    data_type_hint="pd",
                    # No metric_column — source_id stays CBA_<asset>_PD_<period>
                ))

            if lgd_col and lgd_col in row and pd.notna(row[lgd_col]):
                points.append(self._build_point(
                    asset_class_raw=portfolio,
                    metric="LGD",
                    raw_value=float(row[lgd_col]),
                    data_type_hint="lgd",
                ))

        return points

    # ------------------------------------------------------------------
    # Specialised lending slotting grades
    # ------------------------------------------------------------------

    def _scrape_slotting(self) -> list[ScrapedDataPoint]:
        # Tolerate a workbook that omits the slotting sheet — real CBA editions
        # occasionally publish IRB without updating specialised lending tables.
        sheet_name = self._config["slotting_sheet"]
        all_sheets = pd.ExcelFile(self._path).sheet_names
        if sheet_name not in all_sheets:
            return []
        df = pd.read_excel(self._path, sheet_name=sheet_name)
        grade_col = self._config["grade_column"]
        pd_col = self._config["slotting_pd_column"]

        points: list[ScrapedDataPoint] = []
        for _, row in df.iterrows():
            grade = str(row[grade_col]).strip()
            pd_value = row.get(pd_col)
            if pd.isna(pd_value):
                continue
            points.append(self._build_point(
                asset_class_raw="Specialised Lending",
                metric=f"SLOTTING_{grade.upper()}_PD",
                raw_value=float(pd_value),
                data_type_hint="pd",
                metric_column=grade.upper(),   # disambiguates source_id
            ))
        return points

    # ------------------------------------------------------------------
    # Point builder (shared)
    # ------------------------------------------------------------------

    def _build_point(
        self,
        *,
        asset_class_raw: str,
        metric: str,
        raw_value: float,
        data_type_hint: str,
        metric_column: str = "",
    ) -> ScrapedDataPoint:
        source_name = (
            f"CBA_{asset_class_raw.upper().replace(' ', '_')}_{metric.upper()}"
        )
        metadata: dict[str, Any] = {
            "data_type_hint": data_type_hint,
            "period_code": self._period_code,
        }
        if metric_column:
            metadata["metric_column"] = metric_column

        return ScrapedDataPoint(
            source_name=source_name,
            publisher=self.PUBLISHER,
            raw_value=raw_value,
            raw_unit="ratio",
            value_date=self._reporting_date,
            period_years=1,
            asset_class_raw=asset_class_raw,
            geography="AU",
            url=self._config.get("url", ""),
            retrieval_date=self._retrieval_date,
            quality_indicators={"coverage": "CBA IRB portfolio"},
            metadata=metadata,
        )
