"""Phase 3.D §A.3 — ANZ intra-bank reconciliation tests.

ANZ contributes ~46% of Big-4 Manufacturing EAD; concentration warrants
explicit reconciliation that ``intra_bank_industry_totals`` for ANZ
sums correctly across the published gross-carrying-amount components.

ANZ's CRB(e) industry table publishes, per industry, four columns:
    Total | of which: loans | of which: off-balance sheet | of which: other

The adapter intentionally emits ONLY the three component rows (Phase
3.B.3 §B.1 — "Total" is the sum of components and is not re-emitted to
avoid double-counting). The reconciliation invariant is therefore:
intra-bank sum across the three components == ANZ's published Total
column for that industry.

Pinned values are taken verbatim from the FY2025 PDF page 43.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from ingestion.adapters.anz_pillar3_industry import extract_anz_industry_rows
from ingestion.aggregation.pillar3_big4_aggregator import (
    intra_bank_industry_totals,
)


REAL_PDF = (
    Path(__file__).resolve().parents[3]
    / "data" / "raw" / "pillar3" / "ANZ_FY2025_Pillar3_Annual.pdf"
)


# Published Total column from the ANZ FY2025 CRB(e) industry table.
# These are the column-1 values per industry, equal to
# loans + off_balance_sheet + other.
_PUBLISHED_TOTALS = {
    "Agriculture, Forestry, Fishing & Mining": 55_628.0,
    "Manufacturing":                            50_831.0,
    "Construction":                             13_211.0,
}


@pytest.fixture(scope="module")
def anz_intra() -> pd.DataFrame:
    if not REAL_PDF.exists():
        pytest.skip(f"ANZ FY2025 PDF not present at {REAL_PDF}")
    return intra_bank_industry_totals(extract_anz_industry_rows(REAL_PDF))


@pytest.mark.parametrize(
    "industry,expected_total",
    list(_PUBLISHED_TOTALS.items()),
    ids=lambda v: v if isinstance(v, str) else f"={v}",
)
def test_anz_intra_bank_sum_reconciles_to_published_industry_total(
    anz_intra: pd.DataFrame, industry: str, expected_total: float,
) -> None:
    """Intra-bank exposure sum must equal ANZ's published per-industry
    Total column. A miss surfaces a 3.B parser bug (Phase 3.D §A.3
    explicit escalation rule)."""
    rows = anz_intra[
        (anz_intra["industry_published"] == industry)
        & (anz_intra["metric"] == "exposure_aud_m")
    ]
    assert len(rows) == 1, (
        f"ANZ {industry}: expected one intra-bank exposure row, got {len(rows)}"
    )
    assert rows["value_aud_m"].iloc[0] == expected_total, (
        f"ANZ {industry}: intra-bank sum {rows['value_aud_m'].iloc[0]} "
        f"does not reconcile to published total {expected_total}"
    )


def test_anz_total_industries_grand_total_reconciles(
    anz_intra: pd.DataFrame,
) -> None:
    """ANZ publishes an all-industries grand total of 1,486,120 (FY2025).
    Sum across all intra-bank exposure rows should match. This is the
    fallback assertion if per-industry reconciliation ever weakens."""
    grand_total = anz_intra[
        anz_intra["metric"] == "exposure_aud_m"
    ]["value_aud_m"].sum()
    assert grand_total == 1_486_120.0, (
        f"ANZ grand-total intra-bank sum {grand_total} does not match "
        f"published 1,486,120"
    )
