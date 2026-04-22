"""ASIC + ABS failure-rate importer (Phase 6).

Combines two user-staged CSVs to produce a failure rate per ANZSIC division:

    failure_rate = insolvency_count / business_count

ASIC insolvency counts:  data/asic/asic_insolvency_extract.csv
ABS business counts:     data/abs/abs_business_counts.csv (ABS cat. 8165)

Both files must be present. Either missing -> WARNING + empty result (never
raises). The missing-file message includes the source URL so the analyst can
download what they need and re-run.

Extension points:
    ASIC_FILE_PATTERNS / ABS_FILE_PATTERNS  glob patterns searched in the dirs
    ANZSIC_MAP                              raw industry label -> canonical asset_class
    VALIDATION                              per-metric plausibility bounds
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from ingestion.adapters.abs_business_counts_adapter import (
    AbsBusinessCountsAdapter,
)
from ingestion.adapters.asic_insolvency_adapter import AsicInsolvencyAdapter
from ingestion.base import BaseScraper, ScrapedDataPoint
from ingestion.transform import _period_slug

logger = logging.getLogger(__name__)


class ASICABSFailureRateImporter(BaseScraper):
    """Merge ASIC insolvency extracts + ABS business counts -> failure rates.

    Two input shapes per source are supported:

    - **CSV fixture**: the tests' pre-aggregated
      ``(as_of_date, industry, insolvency_count)`` / ``(as_of_date, industry,
      business_count)`` shape. Direct ``pd.read_csv`` — no adapter.
    - **Live XLSX**: the ASIC Series 1+2 workbook and ABS 8165 Data Cube
      01 Table 1. Each routes through its dedicated adapter
      (``AsicInsolvencyAdapter`` / ``AbsBusinessCountsAdapter``) which
      reshapes to the same canonical long-format DataFrame the CSV path
      already produces.

    Detection is by file extension; CSV fixtures are preferred when both
    are present (test compat).
    """

    ASIC_FILE_PATTERNS: tuple[str, ...] = (
        "asic_insolvency_extract.csv",
        "asic_insolvency_*.csv",
        "asic-insolvency-statistics-series-1-and-series-2*.xlsx",
    )
    ABS_FILE_PATTERNS: tuple[str, ...] = (
        "abs_business_counts.csv",
        "abs_business_*.csv",
        "81650*.csv",       # ABS cat. 8165 raw filename prefix
        "8165DC01*.xlsx",   # live ABS Data Cube 01
    )

    # Canonical ANZSIC division -> engine asset_class. 19 divisions per ABS cat. 8165.
    ANZSIC_MAP: dict[str, str] = {
        "agriculture forestry and fishing": "industry_agriculture",
        "agriculture, forestry and fishing": "industry_agriculture",
        "mining": "industry_mining",
        "manufacturing": "industry_manufacturing",
        "electricity gas water and waste services": "industry_utilities",
        "electricity, gas, water and waste services": "industry_utilities",
        "construction": "industry_construction",
        "wholesale trade": "industry_wholesale_trade",
        "retail trade": "industry_retail_trade",
        "accommodation and food services": "industry_accommodation_food",
        "transport postal and warehousing": "industry_transport",
        "transport, postal and warehousing": "industry_transport",
        "information media and telecommunications": "industry_information_media",
        "information media and telecoms": "industry_information_media",
        "financial and insurance services": "industry_financial",
        "rental hiring and real estate services": "industry_rental_real_estate",
        "rental, hiring and real estate services": "industry_rental_real_estate",
        "professional scientific and technical services": "industry_professional",
        "professional, scientific and technical services": "industry_professional",
        "administrative and support services": "industry_admin_support",
        "public administration and safety": "industry_public_admin",
        "education and training": "industry_education",
        "health care and social assistance": "industry_healthcare",
        "arts and recreation services": "industry_arts_recreation",
        "other services": "industry_other_services",
    }

    VALIDATION: dict[str, tuple[float, float]] = {
        "failure_rate": (0.0, 0.10),
    }

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(
        self,
        asic_dir: Path | str | None = None,
        abs_dir: Path | str | None = None,
        config: Optional[dict[str, Any]] = None,
        *,
        retrieval_date: Optional[date] = None,
        force_refresh: bool = False,
        **extras: Any,
    ) -> None:
        self._config = config or {}
        self._asic_dir = Path(asic_dir) if asic_dir else Path(
            self._config.get("asic_dir", "data/asic/")
        )
        self._abs_dir = Path(abs_dir) if abs_dir else Path(
            self._config.get("abs_dir", "data/abs/")
        )
        self._retrieval_date = retrieval_date or date.today()
        self._force_refresh = force_refresh  # no-op

    # ------------------------------------------------------------------
    # BaseScraper contract
    # ------------------------------------------------------------------

    @property
    def source_name(self) -> str:
        return self._config.get("source_name", "asic_abs")

    @property
    def expected_frequency_days(self) -> int:
        return int(self._config.get("frequency_days", 90))

    def scrape(self) -> list[ScrapedDataPoint]:
        asic_df = self._load_source(
            self._asic_dir, self.ASIC_FILE_PATTERNS,
            source_label="ASIC", adapter_cls=AsicInsolvencyAdapter,
        )
        abs_df = self._load_source(
            self._abs_dir, self.ABS_FILE_PATTERNS,
            source_label="ABS", adapter_cls=AbsBusinessCountsAdapter,
        )

        if asic_df.empty:
            logger.warning(
                "ASIC insolvency data missing at %s. Download the latest "
                "insolvency statistics from https://asic.gov.au/regulatory-"
                "resources/find-a-document/statistics/insolvency-statistics/ "
                "and save as %s.",
                self._asic_dir, self._asic_dir / "asic_insolvency_extract.csv",
            )
        if abs_df.empty:
            logger.warning(
                "ABS business counts missing at %s. Download ABS cat. 8165 from "
                "https://www.abs.gov.au/statistics/industry/industry-overview/"
                "counts-australian-businesses-including-entries-and-exits/ "
                "and save as %s.",
                self._abs_dir, self._abs_dir / "abs_business_counts.csv",
            )
        if asic_df.empty or abs_df.empty:
            return []   # can't compute without both

        points = self._compute_failure_rates(asic_df, abs_df)
        return self.validate(points)

    def validate(
        self, points: list[ScrapedDataPoint],
    ) -> list[ScrapedDataPoint]:
        """Drop failure rates outside [0, 0.10] with a WARNING."""
        lo, hi = self.VALIDATION["failure_rate"]
        valid: list[ScrapedDataPoint] = []
        for p in points:
            if lo <= p.raw_value <= hi:
                valid.append(p)
            else:
                logger.warning(
                    "ASIC_ABS: dropping %s (failure_rate=%s outside [%s, %s])",
                    p.source_name, p.raw_value, lo, hi,
                )
        return valid

    # ------------------------------------------------------------------
    # CSV loading
    # ------------------------------------------------------------------

    @staticmethod
    def _load_source(
        dir_path: Path,
        patterns: tuple[str, ...],
        *,
        source_label: str,
        adapter_cls: type,
    ) -> pd.DataFrame:
        """Return the first matching file, dispatching on extension.

        .csv → direct ``pd.read_csv`` (fixture path — unchanged contract).
        .xlsx → ``adapter_cls().normalise(path)`` (live publisher path).

        Patterns are tried in the order declared; fixtures are listed
        first so tests that place both a CSV and an XLSX in the same
        directory still prefer the fixture.
        """
        if not dir_path.exists():
            return pd.DataFrame()
        for pattern in patterns:
            matches = sorted(dir_path.glob(pattern))
            if not matches:
                continue
            path = matches[0]
            try:
                if path.suffix.lower() == ".csv":
                    return pd.read_csv(path)
                if path.suffix.lower() == ".xlsx":
                    return adapter_cls().normalise(path)
                logger.warning(
                    "%s: unexpected extension %r for %s; skipping.",
                    source_label, path.suffix, path,
                )
                return pd.DataFrame()
            except Exception as exc:
                logger.warning(
                    "%s: failed to read %s (%s); skipping.",
                    source_label, path, exc,
                )
                return pd.DataFrame()
        return pd.DataFrame()

    # Kept for backwards compat with any callers still importing the
    # CSV-only helper. New code should use ``_load_source``.
    @staticmethod
    def _load_csv(
        dir_path: Path, patterns: tuple[str, ...], *, source_label: str,
    ) -> pd.DataFrame:
        if not dir_path.exists():
            return pd.DataFrame()
        for pattern in patterns:
            matches = sorted(dir_path.glob(pattern))
            if not matches:
                continue
            path = matches[0]
            if path.suffix.lower() != ".csv":
                continue
            try:
                return pd.read_csv(path)
            except Exception as exc:
                logger.warning(
                    "%s: failed to read %s (%s); skipping.",
                    source_label, path, exc,
                )
                return pd.DataFrame()
        return pd.DataFrame()

    # ------------------------------------------------------------------
    # Failure-rate computation
    # ------------------------------------------------------------------

    def _compute_failure_rates(
        self, asic_df: pd.DataFrame, abs_df: pd.DataFrame,
    ) -> list[ScrapedDataPoint]:
        """Inner-join on normalised industry label; emit one point per match."""
        asic_norm = asic_df.copy()
        abs_norm = abs_df.copy()
        asic_norm["_industry_key"] = asic_norm["industry"].map(_normalise_industry)
        abs_norm["_industry_key"] = abs_norm["industry"].map(_normalise_industry)

        asic_keys = set(asic_norm["_industry_key"])
        abs_keys = set(abs_norm["_industry_key"])
        missing_in_abs = asic_keys - abs_keys
        for key in sorted(missing_in_abs):
            logger.warning(
                "ASIC_ABS: ASIC industry %r has no ABS counterpart — skipping that sector.",
                key,
            )

        # Merge keys: industry always; fiscal_year when both sides carry it
        # (adapter-derived path). CSV fixtures don't carry fiscal_year, so
        # they stay on the industry-only join — matches historical behaviour.
        merge_keys = ["_industry_key"]
        if (
            "fiscal_year" in asic_norm.columns
            and "fiscal_year" in abs_norm.columns
        ):
            merge_keys.append("fiscal_year")

        merged = asic_norm.merge(
            abs_norm, on=merge_keys, how="inner",
            suffixes=("_asic", "_abs"),
        )

        points: list[ScrapedDataPoint] = []
        for _, row in merged.iterrows():
            industry_raw = str(row["industry_asic"]).strip()
            canonical = self._normalise_anzsic(industry_raw)

            try:
                insolv = float(row["insolvency_count"])
                biz = float(row["business_count"])
            except (KeyError, ValueError, TypeError):
                logger.warning(
                    "ASIC_ABS: unparseable count row for %r — skipping", industry_raw,
                )
                continue
            if biz <= 0:
                logger.warning(
                    "ASIC_ABS: non-positive business_count for %r — skipping",
                    industry_raw,
                )
                continue

            failure_rate = insolv / biz

            # Period = ASIC as_of_date quarter (ASIC publishes quarterly).
            as_of_raw = row.get("as_of_date_asic") or row.get("as_of_date")
            try:
                as_of = pd.to_datetime(as_of_raw).date()
            except Exception:
                logger.warning(
                    "ASIC_ABS: unparseable ASIC as_of_date %r — skipping", as_of_raw,
                )
                continue

            qi: dict[str, Any] = {
                "insolvency_count": int(insolv),
                "business_count": int(biz),
                "industry_raw": industry_raw,
                "arithmetic": "failure_rate = insolvency_count / business_count",
            }
            # Propagate adapter audit fields when they came from live files.
            # The ASIC adapter stamps `filter_applied` + `source_sheet`; the
            # ABS adapter stamps `source_sheet` + `anzsic_division_code` +
            # `fiscal_year`. Present-only-if-not-null keeps CSV fixtures
            # on their existing minimal quality_indicators shape.
            for col, key in (
                ("filter_applied", "asic_filter"),
                ("source_sheet_asic", "asic_source_sheet"),
                ("source_sheet_abs", "abs_source_sheet"),
                ("source_sheet", "source_sheet"),
                ("anzsic_division_code", "anzsic_division_code"),
                ("fiscal_year_asic", "fiscal_year"),
                ("fiscal_year", "fiscal_year"),
            ):
                val = row.get(col) if col in row.index else None
                if val is not None and key not in qi and not (
                    isinstance(val, float) and pd.isna(val)
                ):
                    qi[key] = val

            points.append(ScrapedDataPoint(
                source_name=f"ASIC_ABS_{canonical.upper()}_FAILURE_RATE",
                publisher="ASIC_ABS",
                raw_value=failure_rate,
                raw_unit="ratio",
                value_date=as_of,
                period_years=1,
                asset_class_raw=canonical,
                geography="AU",
                url=self._config.get(
                    "url",
                    "https://asic.gov.au/regulatory-resources/find-a-document/"
                    "statistics/insolvency-statistics/",
                ),
                retrieval_date=self._retrieval_date,
                quality_indicators=qi,
                metadata={
                    "data_type_hint": "failure_rate",
                    "period_code": _period_slug(as_of),
                },
            ))
        return points

    def _normalise_anzsic(self, raw_label: str) -> str:
        key = raw_label.strip().lower()
        return self.ANZSIC_MAP.get(key, "industry_" + key.replace(" ", "_").replace(",", ""))


def _normalise_industry(label: Any) -> str:
    """Merge-key builder: lowercase + strip + collapse punctuation/spaces.

    Ensures "Retail Trade", "retail trade", and "Retail, Trade" all join.
    """
    if label is None:
        return ""
    s = str(label).lower().strip()
    # Drop commas so "Retail Trade" and "Retail, Trade" match
    s = s.replace(",", " ")
    # Collapse multiple spaces
    s = " ".join(s.split())
    return s
