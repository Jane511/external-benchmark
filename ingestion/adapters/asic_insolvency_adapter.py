"""ASIC insolvency statistics (Series 1+2 workbook) adapter.

ASIC's Series 1 & 2 workbook carries individual appointment records
(~66k rows, 1999-present) on the ``Data set`` sheet. For the failure-rate
benchmark we need counts of **distinct businesses entering** external
administration per (ANZSIC division × fiscal year), which is the
Series 1 definition. ASIC publishes a dedicated ``Series 1 (companies
entering)`` Yes/No flag column, so the adapter filters on that native
flag rather than maintaining a hand-curated appointment-type list.

Why Series 1 only — methodology note
------------------------------------

A single failing business can generate multiple appointment rows in
Series 2 (e.g. Voluntary administration → Creditors' voluntary
liquidation). The failure-rate denominator is **distinct businesses**
(ABS Counts of Australian Businesses), so the numerator must also count
distinct businesses. Series 1 is ASIC's own first-appointment filter
and aligns the definitions cleanly.

See ``outputs/asic_abs_structure.md`` for the inspection that drove this
choice and the column layout.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ingestion.adapters.base import AbstractAdapter

logger = logging.getLogger(__name__)


# The "Industry type (division)" column uses the same 19 ANZSIC labels
# as ABS 8165. We drop "Unknown" (a non-ANZSIC catch-all) and any other
# label that isn't one of the 19 standard divisions.
_KNOWN_DIVISIONS: frozenset[str] = frozenset({
    "Agriculture, Forestry and Fishing",
    "Mining",
    "Manufacturing",
    "Electricity, Gas, Water and Waste Services",
    "Construction",
    "Wholesale Trade",
    "Retail Trade",
    "Accommodation and Food Services",
    "Transport, Postal and Warehousing",
    "Information Media and Telecommunications",
    "Financial and Insurance Services",
    "Rental, Hiring and Real Estate Services",
    "Professional, Scientific and Technical Services",
    "Administrative and Support Services",
    "Public Administration and Safety",
    "Education and Training",
    "Health Care and Social Assistance",
    "Arts and Recreation Services",
    "Other Services",
})


class AsicInsolvencyAdapter(AbstractAdapter):
    """Aggregate Series 1 appointments to (ANZSIC division × FY) counts."""

    _SOURCE_NAME = "asic_insolvency"
    _CANONICAL_COLUMNS = [
        "as_of_date",
        "industry",
        "insolvency_count",
        "fiscal_year",
        "filter_applied",
        "source_sheet",
    ]

    # ---- tunables ---------------------------------------------------------

    SHEET_NAME = "Data set"
    HEADER_ROW = 6                            # 0-indexed; row 7 in the file

    SERIES_1_COLUMN = "Series 1 \n(companies entering)"
    SERIES_1_VALUE = "Yes"

    INDUSTRY_COLUMN = "Industry type (division)"
    FY_COLUMN = "Period (financial year)"     # native label, e.g. "2021-2022"

    FILTER_RATIONALE = (
        "Series 1 (companies entering): first-time appointments only. "
        "Excludes subsequent appointments (e.g. court liquidation following "
        "voluntary administration of the same company) to align the "
        "failure-rate numerator with the ABS Counts denominator "
        "(distinct businesses)."
    )

    PLAUSIBILITY: dict[str, tuple[float, float]] = {
        "insolvency_count": (0, 100_000),
    }

    # ---- AbstractAdapter contract -----------------------------------------

    @property
    def source_name(self) -> str:
        return self._SOURCE_NAME

    @property
    def canonical_columns(self) -> list[str]:
        return list(self._CANONICAL_COLUMNS)

    def normalise(self, file_path: Path) -> pd.DataFrame:
        try:
            df = pd.read_excel(
                file_path, sheet_name=self.SHEET_NAME, header=self.HEADER_ROW,
            )
        except (KeyError, ValueError) as exc:
            logger.warning(
                "%s: cannot read sheet %r from %s (%s)",
                type(self).__name__, self.SHEET_NAME, file_path.name, exc,
            )
            empty = pd.DataFrame(columns=self._CANONICAL_COLUMNS)
            self.validate_output(empty)
            return empty

        required = {self.SERIES_1_COLUMN, self.INDUSTRY_COLUMN, self.FY_COLUMN}
        missing = required - set(df.columns)
        if missing:
            logger.warning(
                "%s: required columns %s not found (have %s)",
                type(self).__name__, sorted(missing), list(df.columns)[:12],
            )
            empty = pd.DataFrame(columns=self._CANONICAL_COLUMNS)
            self.validate_output(empty)
            return empty

        total = len(df)
        series1 = df[df[self.SERIES_1_COLUMN] == self.SERIES_1_VALUE]
        logger.info(
            "%s: Series 1 filter retained %d of %d rows",
            type(self).__name__, len(series1), total,
        )

        # Drop rows we can't attribute to a known ANZSIC division.
        in_known = series1[self.INDUSTRY_COLUMN].isin(_KNOWN_DIVISIONS)
        unmapped = series1.loc[~in_known, self.INDUSTRY_COLUMN].value_counts()
        if not unmapped.empty:
            logger.warning(
                "%s: %d rows with unmapped industry labels (dropping): %s",
                type(self).__name__, int(unmapped.sum()),
                unmapped.head(5).to_dict(),
            )
        filtered = series1[in_known]

        grouped = (
            filtered.groupby([self.INDUSTRY_COLUMN, self.FY_COLUMN])
            .size()
            .reset_index(name="insolvency_count")
        )

        records: list[dict[str, Any]] = []
        for _, row in grouped.iterrows():
            count = int(row["insolvency_count"])
            if not self._is_plausible("insolvency_count", count):
                logger.warning(
                    "%s: dropping implausible insolvency_count=%d (%s %s)",
                    type(self).__name__, count,
                    row[self.INDUSTRY_COLUMN], row[self.FY_COLUMN],
                )
                continue
            fy_end = _fy_label_to_end_date(str(row[self.FY_COLUMN]))
            if fy_end is None:
                continue
            records.append({
                "as_of_date": fy_end,
                "industry": str(row[self.INDUSTRY_COLUMN]),
                "insolvency_count": count,
                "fiscal_year": f"FY{fy_end.year}",
                "filter_applied": "Series 1 (companies entering)",
                "source_sheet": self.SHEET_NAME,
            })

        out = pd.DataFrame.from_records(records) if records else \
              pd.DataFrame(columns=self._CANONICAL_COLUMNS)
        self.validate_output(out)
        return out

    @classmethod
    def _is_plausible(cls, metric_name: str, value: float) -> bool:
        lo, hi = cls.PLAUSIBILITY.get(metric_name, (0.0, float("inf")))
        return lo <= value <= hi


def _fy_label_to_end_date(label: str) -> date | None:
    """Convert an ASIC FY label like ``"2021-2022"`` to 2022-06-30."""
    parts = label.strip().split("-")
    if len(parts) != 2:
        return None
    try:
        end_year = int(parts[1])
    except ValueError:
        return None
    return date(end_year, 6, 30)
