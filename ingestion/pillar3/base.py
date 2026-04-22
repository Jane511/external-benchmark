"""Shared Pillar 3 parsing logic (APS 330 tables, validation ranges, peer checks).

Common ground across the four Big 4 scrapers:
  * APS 330 credit risk tables have standard portfolios (residential mortgage,
    CRE investment, corporate/SME) with exposure-weighted PD and LGD
  * Specialised lending reports slotting grades (Strong/Good/Satisfactory/Weak)
  * Validation bounds come from plan §Phase 2
  * Cross-bank divergence >3x from peer median is a governance flag
"""
from __future__ import annotations

import logging
from abc import abstractmethod
from datetime import date
from pathlib import Path
from typing import Any, Optional

from ingestion.base import BaseScraper, ScrapedDataPoint
from ingestion.downloader import FileDownloader
from ingestion.source_registry import SOURCE_URLS

logger = logging.getLogger(__name__)


class Pillar3BaseScraper(BaseScraper):
    """Base class for CBA/NAB/WBC/ANZ Pillar 3 scrapers.

    Subclasses implement `scrape()` for their source format (CBA = Excel,
    others = PDF). Validation logic, range lookup, and peer-comparison
    helpers are shared.
    """

    # APS 330 plausibility ranges — per plan §Phase 2.
    # Picked so that transcription errors (e.g. PD in % not ratio) are caught
    # while the realistic Big 4 range is comfortably inside.
    VALIDATION_RANGES: dict[str, tuple[float, float]] = {
        "residential_mortgage_pd": (0.003, 0.025),
        "commercial_property_investment_pd": (0.01, 0.08),
        "corporate_sme_pd": (0.01, 0.10),
        "lgd_generic": (0.0, 1.0),
        "slotting_strong_pd": (0.001, 0.010),
        "slotting_good_pd": (0.003, 0.020),
        "slotting_satisfactory_pd": (0.010, 0.060),
        "slotting_weak_pd": (0.030, 0.200),
    }

    def __init__(self, config: dict[str, Any]) -> None:
        self._config = config

    # ------------------------------------------------------------------
    # BaseScraper contract
    # ------------------------------------------------------------------

    @abstractmethod
    def scrape(self) -> list[ScrapedDataPoint]:
        ...  # pragma: no cover — abstract

    def validate(self, points: list[ScrapedDataPoint]) -> list[ScrapedDataPoint]:
        """Keep only points whose raw_value falls in the APS 330 plausibility range."""
        valid: list[ScrapedDataPoint] = []
        for p in points:
            lo, hi = self._pick_range(p)
            if lo <= p.raw_value <= hi:
                valid.append(p)
        return valid

    @property
    def expected_frequency_days(self) -> int:
        """Big 4 publish semi-annually; allow ~180-day grace."""
        return 180

    # ------------------------------------------------------------------
    # Helpers (shared across banks)
    # ------------------------------------------------------------------

    def _pick_range(self, point: ScrapedDataPoint) -> tuple[float, float]:
        """Select the right (lo, hi) bucket for this data point.

        Resolution order:
          1. Slotting grade (Strong/Good/Satisfactory/Weak) — inferred from source_name
          2. LGD (from data_type_hint) — generic LGD range
          3. Asset-class-specific PD range (residential / CRE / corporate SME)
          4. Fallback: generic LGD range (permissive) so unknown rows aren't dropped silently
        """
        name = point.source_name.lower()
        hint = str(point.metadata.get("data_type_hint", "")).lower()

        if "slotting" in name:
            for grade in ("strong", "good", "satisfactory", "weak"):
                if f"slotting_{grade}" in name:
                    return self.VALIDATION_RANGES[f"slotting_{grade}_pd"]

        if hint == "lgd":
            return self.VALIDATION_RANGES["lgd_generic"]

        label = point.asset_class_raw.lower()
        if "residential" in label:
            return self.VALIDATION_RANGES["residential_mortgage_pd"]
        if "commercial" in label or "cre" in label:
            return self.VALIDATION_RANGES["commercial_property_investment_pd"]
        if "corporate" in label or "sme" in label:
            return self.VALIDATION_RANGES["corporate_sme_pd"]

        return self.VALIDATION_RANGES["lgd_generic"]

    @staticmethod
    def peer_comparison_flag(
        value: float,
        peer_median: float,
        *,
        threshold_multiple: float = 3.0,
    ) -> bool:
        """Flag values that diverge from peer_median by more than `threshold_multiple`x.

        Used by governance.peer_comparison_report() for APS 330 cross-bank checks —
        if any one bank's PD is 3x the median of its peers, something is probably
        wrong at the parsing or classification layer.
        """
        if peer_median <= 0 or value < 0:
            return False
        if value == 0:
            return True  # zero where peers are non-zero is suspicious
        ratio = max(value / peer_median, peer_median / value)
        return ratio > threshold_multiple

    # ------------------------------------------------------------------
    # PDF-table helpers (used by NAB / WBC / ANZ scrapers)
    # CBA uses Excel and ignores these. Keeping them on the shared base
    # so banks can mix both paths if a future CBA edition switches formats.
    # ------------------------------------------------------------------

    @staticmethod
    def _find_table_by_header_pattern(
        pdf: Any,
        pattern: str,
    ) -> Optional[tuple[int, list[list[str]]]]:
        """Locate a pdfplumber table whose first rows contain `pattern` (ci substring).

        Returns (page_index, list-of-rows) or None.

        The search scans at most the first three rows of every detected table so
        multi-row APS 330 headers (e.g. "Credit risk exposures / by portfolio /
        and PD band") still match on any of the header fragments.
        """
        needle = pattern.lower()
        for page_num, page in enumerate(pdf.pages):
            try:
                tables = page.extract_tables() or []
            except Exception:  # pdfplumber can raise varied errors on malformed pages
                continue
            for table in tables:
                if not table:
                    continue
                for row in table[:3]:
                    if any(needle in (cell or "").lower() for cell in row if cell):
                        return page_num, table
        return None

    @staticmethod
    def _extract_pd_lgd_row(
        row: list,
        pd_col: int,
        lgd_col: int,
        exposure_col: Optional[int] = None,
    ) -> dict[str, Optional[float]]:
        """Parse a single row's PD/LGD/exposure cells into floats.

        Handles percent signs ("0.72%" -> 0.0072), thousands separators, and
        missing-value placeholders ("—", "–", "-", "n/a", ""). Returns None
        for cells that cannot be parsed rather than raising — the caller
        decides whether to emit a data point.
        """
        def _parse(cell: Any) -> Optional[float]:
            if cell is None:
                return None
            s = str(cell).strip()
            if s in ("", "—", "–", "-", "n/a", "N/A", "na", "NA"):
                return None
            has_pct = "%" in s
            cleaned = s.replace("%", "").replace(",", "").strip()
            try:
                v = float(cleaned)
            except ValueError:
                return None
            return v / 100.0 if has_pct else v

        out: dict[str, Optional[float]] = {
            "pd": _parse(row[pd_col]) if pd_col < len(row) else None,
            "lgd": _parse(row[lgd_col]) if lgd_col < len(row) else None,
        }
        if exposure_col is not None and exposure_col < len(row):
            out["exposure_ead_mn"] = _parse(row[exposure_col])
        return out

    @staticmethod
    def _normalise_asset_class_label(raw_label: str) -> str:
        """Bank-variant asset-class label -> canonical engine value.

        Handles the three ways each bank describes the main IRB portfolios
        (residential / commercial / corporate), plus specialised lending.
        Unknown labels fall through to a slugified form so downstream code
        can still see them (they'll likely fail the APS 330 range check).
        """
        s = raw_label.strip().lower()
        if any(t in s for t in ("residential", "housing", "home loan", "owner-occupied")):
            return "residential_mortgage"
        if any(t in s for t in (
            "commercial property", "commercial real estate",
            "commercial mortgage", "cre",
        )):
            return "commercial_property_investment"
        if any(t in s for t in ("corporate", "sme", "business lending")):
            return "corporate_sme"
        if "specialised" in s or "specialized" in s or "slotting" in s:
            return "development"
        return s.replace(" ", "_").replace("-", "_")


# ---------------------------------------------------------------------------
# PDF scraper template — shared __init__ + scrape orchestration for NAB/WBC/ANZ
# ---------------------------------------------------------------------------

class PdfPillar3Scraper(Pillar3BaseScraper):
    """Template parent for the three PDF-based Big 4 Pillar 3 scrapers.

    Subclasses declare the following class attributes (and nothing else, typically):
        PUBLISHER               e.g. "NAB"
        SOURCE_URL_KEY          e.g. "nab_pillar3" — matches SOURCE_URLS key
        IRB_HEADER_PATTERN      case-insensitive substring to locate the IRB CR6 table
        SLOTTING_HEADER_PATTERN similar, for specialised lending
        IRB_COLUMN_MAP          {"asset_class": i, "exposure_ead": i, "pd": i, "lgd": i}
        SLOTTING_COLUMN_MAP     {"grade": i, "pd": i}

    Tests bypass pdfplumber by calling `_build_points_from_tables(parsed_tables)`
    directly with a list of dicts matching the canonical shape emitted by
    `_extract_tables_from_pdf()`.
    """

    # Subclasses MUST override these (empty defaults raise during scrape).
    PUBLISHER: str = ""
    SOURCE_URL_KEY: str = ""
    IRB_HEADER_PATTERN: str = ""
    SLOTTING_HEADER_PATTERN: str = ""
    IRB_COLUMN_MAP: dict[str, int] = {}
    SLOTTING_COLUMN_MAP: dict[str, int] = {}

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
        super().__init__(config or {})
        self._path: Optional[Path] = Path(source_path) if source_path else None
        self._reporting_date = reporting_date or date(date.today().year, 6, 30)
        self._period_code = period_code or default_cba_period_code(self._reporting_date)
        self._retrieval_date = retrieval_date or date.today()
        self._force_refresh = force_refresh
        self._cache_base = Path(cache_base)

    @property
    def source_name(self) -> str:
        return self._config.get("source_name", f"{self.PUBLISHER}_PILLAR3")

    # ------------------------------------------------------------------
    # scrape() — pdfplumber path
    # ------------------------------------------------------------------

    # Per-bank subclasses set this to route live PDFs through a
    # CbaPillar3PdfAdapter subclass. ``None`` means this bank doesn't
    # have an adapter yet and should fall through to the legacy fixture-
    # shaped table extraction path.
    PDF_ADAPTER_CLS = None

    def scrape(self) -> list[ScrapedDataPoint]:
        if not self.PUBLISHER or not self.SOURCE_URL_KEY:
            raise NotImplementedError(
                f"{type(self).__name__} must set PUBLISHER and SOURCE_URL_KEY"
            )
        path = self._resolve_source_path()
        if not path.exists():
            raise FileNotFoundError(
                f"{self.PUBLISHER} Pillar 3 PDF not found at {path}. "
                "Download the PDF first or pass --source-path."
            )
        # Live annual / half-year PDF → CbaPillar3PdfAdapter subclass.
        if self.PDF_ADAPTER_CLS is not None and path.suffix.lower() == ".pdf":
            points = self._scrape_via_pdf_adapter(path)
            return self.validate(points)

        # Legacy path: fixture-shaped parsed tables from pdfplumber.
        parsed = self._extract_tables_from_pdf(path)
        points = self._build_points_from_tables(parsed)
        return self.validate(points)

    def _scrape_via_pdf_adapter(self, path: Path) -> list[ScrapedDataPoint]:
        """Run the configured PDF adapter; map its DataFrame rows to points."""
        adapter = self.PDF_ADAPTER_CLS()
        df = adapter.normalise(path, reporting_date=self._reporting_date)
        if df.empty:
            return []

        url = self._config.get("url", "")
        pts: list[ScrapedDataPoint] = []
        for _, row in df.iterrows():
            asset_class = str(row["asset_class"])
            metric_name = str(row["metric_name"])
            value = float(row["value"])
            source_table = str(row["source_table"])
            source_page = int(row["source_page"])
            pd_band = str(row["pd_band"])
            period_code = str(row["period_code"])

            data_type_hint = {
                "pd": "pd", "lgd": "lgd", "risk_weight": "supervisory",
            }[metric_name]

            band_slug = (
                pd_band.replace(" ", "_").replace("<", "lt")
                .replace("(", "").replace(")", "").replace(".", "p")
            )
            metric_column = (
                f"{band_slug}_{metric_name}"
                if metric_name != "risk_weight" else metric_name
            )

            pts.append(ScrapedDataPoint(
                source_name=(
                    f"{self.PUBLISHER}_{asset_class.upper()}_"
                    f"{metric_name.upper()}_{band_slug.upper()}"
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
                    "coverage": f"{self.PUBLISHER.lower()}_pillar3_annual_pdf",
                    "source_table": source_table,
                    "source_page": source_page,
                    "pd_band": pd_band,
                    "value_basis": str(row["value_basis"]),
                    "adapter": type(adapter).__name__,
                },
                metadata={
                    "data_type_hint": data_type_hint,
                    "metric_column": metric_column,
                    "period_code": period_code,
                    "note": (
                        f"{self.PUBLISHER} {source_table} — "
                        f"PD band {pd_band!r}. Exposure-weighted PD/LGD "
                        f"from IRB; slotting risk weights APS 113 prescribed."
                    ),
                },
            ))
        return pts

    def _resolve_source_path(self) -> Path:
        """Identical pattern to CBA: H1/H2 derivation + FileDownloader."""
        if self._path is not None:
            return self._path
        info = SOURCE_URLS[self.SOURCE_URL_KEY]
        file_spec = info["files"][0]
        rd = self._reporting_date
        half = "H1" if rd.month <= 6 else "H2"
        filename = file_spec["filename_pattern"].format(half=half, year=rd.year)
        downloader = FileDownloader(self._cache_base, "pillar3")
        return downloader.download_and_cache(
            file_spec["url"], filename, force_refresh=self._force_refresh,
        )

    # ------------------------------------------------------------------
    # PDF extraction
    # ------------------------------------------------------------------

    def _extract_tables_from_pdf(self, pdf_path: Path) -> list[dict]:
        """Open the PDF and return a list of canonical-shape parsed tables.

        Each dict: {"name": "IRB Credit Risk" | "Specialised Lending", "rows": [...]}.
        Missing tables are logged as warnings and omitted — the rest of the
        scrape proceeds so one layout drift doesn't abort the whole refresh.
        """
        try:
            import pdfplumber
        except ImportError as exc:
            raise ImportError(
                "pdfplumber is required for Big 4 Pillar 3 PDF scraping. "
                "Install with: pip install external_benchmark_engine[ingestion]"
            ) from exc

        results: list[dict] = []
        with pdfplumber.open(pdf_path) as pdf:
            irb = self._find_table_by_header_pattern(pdf, self.IRB_HEADER_PATTERN)
            if irb is not None:
                _page, rows = irb
                results.append({
                    "name": "IRB Credit Risk",
                    "rows": self._parse_irb_table_rows(rows),
                })
            else:
                logger.warning(
                    "%s: IRB table not found (pattern=%r) — skipping that table only",
                    self.PUBLISHER, self.IRB_HEADER_PATTERN,
                )

            slotting = self._find_table_by_header_pattern(pdf, self.SLOTTING_HEADER_PATTERN)
            if slotting is not None:
                _page, rows = slotting
                results.append({
                    "name": "Specialised Lending",
                    "rows": self._parse_slotting_table_rows(rows),
                })
            else:
                logger.warning(
                    "%s: Specialised Lending table not found (pattern=%r)",
                    self.PUBLISHER, self.SLOTTING_HEADER_PATTERN,
                )
        return results

    def _parse_irb_table_rows(self, table_rows: list[list[str]]) -> list[dict]:
        """Turn a pdfplumber table into canonical IRB row dicts."""
        data: list[dict] = []
        cm = self.IRB_COLUMN_MAP
        ac_col = cm["asset_class"]
        for row in table_rows[1:]:   # skip header
            if not row or ac_col >= len(row):
                continue
            label = (row[ac_col] or "").strip()
            if not label:
                continue
            vals = self._extract_pd_lgd_row(
                row, cm["pd"], cm["lgd"], cm.get("exposure_ead"),
            )
            if vals["pd"] is None and vals["lgd"] is None:
                continue
            data.append({
                "asset_class_raw": label,
                "pd": vals["pd"],
                "lgd": vals["lgd"],
                "exposure_ead_mn": vals.get("exposure_ead_mn"),
            })
        return data

    def _parse_slotting_table_rows(self, table_rows: list[list[str]]) -> list[dict]:
        cm = self.SLOTTING_COLUMN_MAP
        grade_col = cm["grade"]
        pd_col = cm["pd"]
        valid_grades = {"strong", "good", "satisfactory", "weak"}
        data: list[dict] = []
        for row in table_rows[1:]:
            if not row or grade_col >= len(row):
                continue
            grade = (row[grade_col] or "").strip()
            if grade.lower() not in valid_grades:
                continue
            vals = self._extract_pd_lgd_row(row, pd_col, pd_col)
            if vals["pd"] is None:
                continue
            data.append({"grade": grade, "pd": vals["pd"]})
        return data

    # ------------------------------------------------------------------
    # Canonical parsed-tables -> ScrapedDataPoints
    # (tests call this directly with JSON fixtures to bypass pdfplumber)
    # ------------------------------------------------------------------

    def _build_points_from_tables(
        self, parsed_tables: list[dict],
    ) -> list[ScrapedDataPoint]:
        points: list[ScrapedDataPoint] = []
        for table in parsed_tables:
            if table["name"] == "IRB Credit Risk":
                points.extend(self._build_irb_points(table["rows"]))
            elif table["name"] == "Specialised Lending":
                points.extend(self._build_slotting_points(table["rows"]))
        return points

    def _build_irb_points(self, rows: list[dict]) -> list[ScrapedDataPoint]:
        points: list[ScrapedDataPoint] = []
        for row in rows:
            raw_label = row["asset_class_raw"]
            if row.get("pd") is not None:
                points.append(self._build_point(
                    asset_class_raw=raw_label, metric="PD",
                    raw_value=row["pd"], data_type_hint="pd",
                ))
            if row.get("lgd") is not None:
                points.append(self._build_point(
                    asset_class_raw=raw_label, metric="LGD",
                    raw_value=row["lgd"], data_type_hint="lgd",
                ))
        return points

    def _build_slotting_points(self, rows: list[dict]) -> list[ScrapedDataPoint]:
        points: list[ScrapedDataPoint] = []
        for row in rows:
            grade = row["grade"]
            points.append(self._build_point(
                asset_class_raw="Specialised Lending",
                metric=f"SLOTTING_{grade.upper()}_PD",
                raw_value=row["pd"],
                data_type_hint="pd",
                metric_column=grade.upper(),
            ))
        return points

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
            f"{self.PUBLISHER}_{asset_class_raw.upper().replace(' ', '_')}_{metric.upper()}"
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
            quality_indicators={"coverage": f"{self.PUBLISHER} IRB portfolio"},
            metadata=metadata,
        )


def default_cba_period_code(reporting_date: date) -> str:
    """Convert a reporting date to CBA's fiscal-year period code.

    CBA fiscal year = July 1 .. June 30.
      - June 30, YYYY   -> FY{YYYY}    (full year)
      - December 31, YY -> H1FY{YY+1}  (half-year sits inside next FY)
      - anything else   -> FY{fy}_M{mm} as a fallback
    """
    if reporting_date.month == 6 and reporting_date.day == 30:
        return f"FY{reporting_date.year}"
    if reporting_date.month == 12 and reporting_date.day == 31:
        return f"H1FY{reporting_date.year + 1}"
    fy = reporting_date.year if reporting_date.month <= 6 else reporting_date.year + 1
    return f"FY{fy}_M{reporting_date.month:02d}"
