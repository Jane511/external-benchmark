"""ICC Trade Register scraper (Phase 3) — manual-download only.

Unlike APRA / Pillar 3, the ICC Trade Register is paid (EUR 2,500+ from 2025
onward); the 2024 edition was public. The user drops the PDF into
`data/raw/icc/` and names it `ICC_Trade_Register_{YEAR}.pdf`. This scraper
does NOT download; `--force-refresh` is accepted for CLI parity but is a no-op.

Data shape:
    Default rate table   one row per product, three weighting schemes
                         (exposure / obligor / transaction). Three points
                         per product, disambiguated by metric_code.
    LGD / recovery table (optional)  one row per product, up to two points
                         per row (data_type=lgd and data_type=recovery_rate).

Extension points (class attributes, one-line edits per real PDF):
    DEFAULT_RATE_TABLE_HEADER / LGD_TABLE_HEADER   pdfplumber search strings
    DEFAULT_RATE_COLUMN_MAP / LGD_COLUMN_MAP       column index layout
    PRODUCT_MAP                                    raw label -> canonical asset_class
    VALIDATION_RANGES                              per-product plausibility bounds
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Any, Optional

from ingestion.base import BaseScraper, ScrapedDataPoint

logger = logging.getLogger(__name__)


class IccTradeScraper(BaseScraper):
    """Extract default-rate and LGD tables from a manually-downloaded ICC PDF."""

    # ------------------------------------------------------------------
    # Extension points — tune when a real PDF arrives
    # ------------------------------------------------------------------

    DEFAULT_RATE_TABLE_HEADER = "Default Rate"
    LGD_TABLE_HEADER = "Loss Given Default"

    DEFAULT_RATE_COLUMN_MAP: dict[str, int] = {
        "product": 0,
        "exposure_weighted": 1,
        "obligor_weighted": 2,
        "transaction_weighted": 3,
    }
    LGD_COLUMN_MAP: dict[str, int] = {
        "product": 0,
        "lgd": 1,
        "recovery_rate": 2,
    }

    PRODUCT_MAP: dict[str, str] = {
        "import lc": "trade_import_lc",
        "import letter of credit": "trade_import_lc",
        "export lc": "trade_export_lc",
        "export letter of credit": "trade_export_lc",
        "performance guarantee": "trade_performance_guarantee",
        "performance guarantees and standbys": "trade_performance_guarantee",
        "standby": "trade_performance_guarantee",
        "standby letter of credit": "trade_performance_guarantee",
        "trade loan": "trade_loan",
        "trade finance loan": "trade_loan",
        "trade loans": "trade_loan",
        "supply chain finance": "scf_payables",
        "scf payables": "scf_payables",
        "payables finance": "scf_payables",
    }

    # Default-rate ranges per product (from plan + ICC 2024 narrative).
    # LGD row values use the lgd_generic range regardless of product.
    VALIDATION_RANGES: dict[str, tuple[float, float]] = {
        "trade_import_lc":              (0.0, 0.005),
        "trade_export_lc":              (0.0, 0.005),
        "trade_performance_guarantee":  (0.0, 0.005),
        "trade_loan":                   (0.0, 0.015),
        "scf_payables":                 (0.0, 0.003),
        "lgd_generic":                  (0.0, 1.0),
    }

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(
        self,
        source_path: Path | str | None = None,
        config: Optional[dict[str, Any]] = None,
        *,
        report_year: Optional[int] = None,
        retrieval_date: Optional[date] = None,
        force_refresh: bool = False,
        cache_base: Path | str = "data/raw",
        **extras: Any,
    ) -> None:
        self._config = config or {}
        self._path: Optional[Path] = Path(source_path) if source_path else None
        self._report_year = report_year
        self._retrieval_date = retrieval_date or date.today()
        # Accepted for CLI API parity; no-op since ICC is manual-download.
        self._force_refresh = force_refresh
        if force_refresh:
            logger.info(
                "ICC Trade Register: --force-refresh accepted but is a no-op "
                "(manual-download source)."
            )
        self._cache_base = Path(cache_base)

    # ------------------------------------------------------------------
    # BaseScraper contract
    # ------------------------------------------------------------------

    @property
    def source_name(self) -> str:
        return self._config.get("source_name", "ICC_TRADE")

    @property
    def expected_frequency_days(self) -> int:
        return int(self._config.get("frequency_days", 395))

    def scrape(self) -> list[ScrapedDataPoint]:
        pdf_path = self._resolve_source_path()
        if not pdf_path.exists():
            raise FileNotFoundError(
                f"ICC Trade Register PDF not found at {pdf_path}. "
                "Manual-download source: place ICC_Trade_Register_YYYY.pdf in "
                f"{self._icc_cache_dir()} and try again."
            )

        report_year = self._report_year or self._infer_year_from_filename(pdf_path)
        parsed = self._extract_tables_from_pdf(pdf_path)
        if report_year and "report_year" not in parsed:
            parsed["report_year"] = report_year
        return self.validate(self._build_points_from_tables(parsed))

    def validate(
        self, points: list[ScrapedDataPoint],
    ) -> list[ScrapedDataPoint]:
        """Keep only values inside per-product plausibility ranges.

        Out-of-range rows log a warning and are dropped — the rest of the
        scrape proceeds. This matches the Pillar 3 policy: one layout drift
        or extreme value shouldn't abort the whole ingest.
        """
        valid: list[ScrapedDataPoint] = []
        for p in points:
            hint = str(p.metadata.get("data_type_hint", "")).lower()
            canonical = self._normalise_product(p.asset_class_raw)
            if hint in ("lgd", "recovery_rate"):
                lo, hi = self.VALIDATION_RANGES["lgd_generic"]
            else:
                lo, hi = self.VALIDATION_RANGES.get(canonical, (0.0, 1.0))

            if lo <= p.raw_value <= hi:
                valid.append(p)
            else:
                logger.warning(
                    "ICC: dropping %s (value=%s outside [%s, %s])",
                    p.source_name, p.raw_value, lo, hi,
                )
        return valid

    # ------------------------------------------------------------------
    # Source-path resolution (manual download; no network)
    # ------------------------------------------------------------------

    def _icc_cache_dir(self) -> Path:
        return self._cache_base / "icc"

    def _resolve_source_path(self) -> Path:
        """Locate the ICC PDF in data/raw/icc/, preferring explicit path then year.

        Search order:
            1. self._path (explicit --source-path)
            2. data/raw/icc/ICC_Trade_Register_{report_year}.pdf (if report_year set)
            3. Newest matching ICC_Trade_Register_*.pdf in data/raw/icc/

        Any miss raises FileNotFoundError with a manual-download hint — this
        scraper never fetches automatically.
        """
        if self._path is not None:
            return self._path

        icc_dir = self._icc_cache_dir()
        if not icc_dir.exists():
            raise FileNotFoundError(
                f"ICC cache directory {icc_dir} does not exist. Manual-download "
                "source: create the directory and drop ICC_Trade_Register_YYYY.pdf in it."
            )

        if self._report_year is not None:
            candidate = icc_dir / f"ICC_Trade_Register_{self._report_year}.pdf"
            if not candidate.exists():
                raise FileNotFoundError(
                    f"ICC Trade Register {self._report_year} edition not found at "
                    f"{candidate}. Download from "
                    "https://iccwbo.org/news-publications/policies-reports/"
                    "icc-trade-register-report/ and save as "
                    f"ICC_Trade_Register_{self._report_year}.pdf in {icc_dir}."
                )
            return candidate

        matches = sorted(
            icc_dir.glob("ICC_Trade_Register_*.pdf"),
            key=lambda p: p.name,
            reverse=True,
        )
        if not matches:
            raise FileNotFoundError(
                f"No ICC_Trade_Register_*.pdf files found in {icc_dir}. "
                "Download from https://iccwbo.org/news-publications/policies-reports/"
                "icc-trade-register-report/ and save as ICC_Trade_Register_YYYY.pdf."
            )
        return matches[0]

    @staticmethod
    def _infer_year_from_filename(path: Path) -> Optional[int]:
        """ICC_Trade_Register_2024.pdf -> 2024."""
        import re
        m = re.search(r"(\d{4})", path.stem)
        return int(m.group(1)) if m else None

    # ------------------------------------------------------------------
    # PDF extraction (pdfplumber path)
    # ------------------------------------------------------------------

    def _extract_tables_from_pdf(self, pdf_path: Path) -> dict:
        """Return a canonical dict with default_rates + optional lgd_rates + report_year.

        Missing tables are logged at WARNING and omitted — if an edition only
        publishes default rates, the scrape still emits those points.
        """
        try:
            import pdfplumber  # lazy import; optional extra
        except ImportError as exc:
            raise ImportError(
                "pdfplumber is required for ICC PDF scraping. Install with: "
                "pip install external_benchmark_engine[ingestion]"
            ) from exc

        out: dict[str, Any] = {"default_rates": [], "lgd_rates": []}

        with pdfplumber.open(pdf_path) as pdf:
            dr = self._find_table(pdf, self.DEFAULT_RATE_TABLE_HEADER)
            if dr is not None:
                out["default_rates"] = self._parse_default_rate_rows(dr[1])
            else:
                logger.warning(
                    "ICC: default-rate table not found (pattern=%r)",
                    self.DEFAULT_RATE_TABLE_HEADER,
                )

            lgd = self._find_table(pdf, self.LGD_TABLE_HEADER)
            if lgd is not None:
                out["lgd_rates"] = self._parse_lgd_rows(lgd[1])
            else:
                # Not all editions publish LGD tables — informational only.
                logger.info(
                    "ICC: LGD table not found (pattern=%r); continuing with "
                    "default rates only.",
                    self.LGD_TABLE_HEADER,
                )

        return out

    @staticmethod
    def _find_table(pdf: Any, pattern: str) -> Optional[tuple[int, list[list[str]]]]:
        """Case-insensitive substring search for a table header across pages.

        Mirrors Pillar3BaseScraper._find_table_by_header_pattern but kept local
        to IccTradeScraper so ICC doesn't need to inherit the Pillar 3 base.
        """
        needle = pattern.lower()
        for page_num, page in enumerate(pdf.pages):
            try:
                tables = page.extract_tables() or []
            except Exception:
                continue
            for table in tables:
                if not table:
                    continue
                for row in table[:3]:
                    if any(needle in (cell or "").lower() for cell in row if cell):
                        return page_num, table
        return None

    def _parse_default_rate_rows(
        self, table_rows: list[list[str]],
    ) -> list[dict]:
        cm = self.DEFAULT_RATE_COLUMN_MAP
        data: list[dict] = []
        for row in table_rows[1:]:   # skip header
            if not row or cm["product"] >= len(row):
                continue
            product = (row[cm["product"]] or "").strip()
            if not product:
                continue
            out = {"product": product}
            for key in ("exposure_weighted", "obligor_weighted", "transaction_weighted"):
                idx = cm.get(key)
                if idx is not None and idx < len(row):
                    out[key] = _parse_cell(row[idx])
                else:
                    out[key] = None
            if any(out[k] is not None for k in
                   ("exposure_weighted", "obligor_weighted", "transaction_weighted")):
                data.append(out)
        return data

    def _parse_lgd_rows(self, table_rows: list[list[str]]) -> list[dict]:
        cm = self.LGD_COLUMN_MAP
        data: list[dict] = []
        for row in table_rows[1:]:
            if not row or cm["product"] >= len(row):
                continue
            product = (row[cm["product"]] or "").strip()
            if not product:
                continue
            out: dict[str, Any] = {"product": product}
            if "lgd" in cm and cm["lgd"] < len(row):
                out["lgd"] = _parse_cell(row[cm["lgd"]])
            if "recovery_rate" in cm and cm["recovery_rate"] < len(row):
                out["recovery_rate"] = _parse_cell(row[cm["recovery_rate"]])
            if out.get("lgd") is not None or out.get("recovery_rate") is not None:
                data.append(out)
        return data

    # ------------------------------------------------------------------
    # Canonical tables -> ScrapedDataPoints
    # (tests inject fixture JSON here to bypass pdfplumber)
    # ------------------------------------------------------------------

    def _build_points_from_tables(
        self, parsed: dict,
    ) -> list[ScrapedDataPoint]:
        report_year = parsed.get("report_year") or date.today().year
        value_date = date(report_year, 12, 31)
        period_code = f"FY{report_year}"

        points: list[ScrapedDataPoint] = []
        points.extend(self._build_default_rate_points(
            parsed.get("default_rates", []), value_date, period_code,
        ))
        points.extend(self._build_lgd_points(
            parsed.get("lgd_rates", []), value_date, period_code,
        ))
        return points

    def _build_default_rate_points(
        self,
        rows: list[dict],
        value_date: date,
        period_code: str,
    ) -> list[ScrapedDataPoint]:
        points: list[ScrapedDataPoint] = []
        measures = (
            ("exposure_weighted", "EXPOSURE_WEIGHTED"),
            ("obligor_weighted", "OBLIGOR_WEIGHTED"),
            ("transaction_weighted", "TRANSACTION_WEIGHTED"),
        )
        for row in rows:
            product_raw = row["product"]
            for measure_key, measure_code in measures:
                val = row.get(measure_key)
                if val is None:
                    continue
                points.append(self._build_point(
                    asset_class_raw=product_raw,
                    metric=f"{measure_code}_DEFAULT_RATE",
                    raw_value=float(val),
                    data_type_hint="default_rate",
                    metric_column=measure_code,
                    value_date=value_date,
                    period_code=period_code,
                ))
        return points

    def _build_lgd_points(
        self,
        rows: list[dict],
        value_date: date,
        period_code: str,
    ) -> list[ScrapedDataPoint]:
        points: list[ScrapedDataPoint] = []
        for row in rows:
            product_raw = row["product"]
            if row.get("lgd") is not None:
                points.append(self._build_point(
                    asset_class_raw=product_raw,
                    metric="LGD",
                    raw_value=float(row["lgd"]),
                    data_type_hint="lgd",
                    value_date=value_date,
                    period_code=period_code,
                ))
            if row.get("recovery_rate") is not None:
                points.append(self._build_point(
                    asset_class_raw=product_raw,
                    metric="RECOVERY_RATE",
                    raw_value=float(row["recovery_rate"]),
                    data_type_hint="recovery_rate",
                    value_date=value_date,
                    period_code=period_code,
                ))
        return points

    def _build_point(
        self,
        *,
        asset_class_raw: str,
        metric: str,
        raw_value: float,
        data_type_hint: str,
        value_date: date,
        period_code: str,
        metric_column: str = "",
    ) -> ScrapedDataPoint:
        source_name = (
            f"ICC_{asset_class_raw.upper().replace(' ', '_')}_{metric.upper()}"
        )
        metadata: dict[str, Any] = {
            "data_type_hint": data_type_hint,
            "period_code": period_code,
        }
        if metric_column:
            metadata["metric_column"] = metric_column

        return ScrapedDataPoint(
            source_name=source_name,
            publisher="ICC",
            raw_value=raw_value,
            raw_unit="ratio",
            value_date=value_date,
            period_years=10,          # ICC Trade Register carries 10+ year history
            asset_class_raw=asset_class_raw,
            geography="GLOBAL",       # ICC is cross-border; not AU-specific
            url=self._config.get("url", ""),
            retrieval_date=self._retrieval_date,
            quality_indicators={"coverage": "ICC Trade Register (300+ banks)"},
            metadata=metadata,
        )

    # ------------------------------------------------------------------
    # Label normalisation
    # ------------------------------------------------------------------

    def _normalise_product(self, raw_label: str) -> str:
        key = raw_label.strip().lower()
        if key in self.PRODUCT_MAP:
            return self.PRODUCT_MAP[key]
        return key.replace(" ", "_").replace("-", "_")


def _parse_cell(cell: Any) -> Optional[float]:
    """Shared cell parser — handles percent signs, commas, placeholder dashes."""
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
