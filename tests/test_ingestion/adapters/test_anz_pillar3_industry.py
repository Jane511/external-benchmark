"""Tests for ANZ Pillar 3 industry-table extraction (Phase 3.B.2 §5)."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from ingestion.adapters.anz_pillar3_industry import (
    ANZ_INDUSTRIES,
    extract_anz_industry_rows,
    parse_industry_text,
)
from ingestion.adapters.anz_pillar3_pdf_adapter import AnzPillar3PdfAdapter
from ingestion.adapters.anzsic_harmonisation import (
    is_business_lending,
    resolve,
)
from ingestion.adapters.pillar3_industry_schema import (
    COL_GROSS_CARRYING_COMPONENT,
    COL_PROVISION_BASIS,
    GROSS_COMPONENT_LOANS,
    GROSS_COMPONENT_OFF_BS,
    GROSS_COMPONENT_OTHER,
    METRIC_EXPOSURE,
    METRIC_NPE,
    METRIC_PROVISIONS,
    METRIC_WRITE_OFFS,
    PROVISION_BASIS_AASB9_STAGE3,
    REDACTION_DASH_OR_HYPHEN,
    assert_row_well_formed,
)


# Trimmed FY2025 industry table; preserves the row-index prefix that
# ANZ uses on every line (1..16) and the verbatim Govt-all-dashes row.
_ANZ_INDUSTRY_MINI = """\
ANZ Basel III Pillar 3 disclosure
September 2025
Sep 25
$M $M $M $M $M $M $M $M
1 Agriculture, Forestry, Fishing & Mining 55,628 41,326 13,517 785 671 171 83 33
2 Business & Property Services 23,532 14,348 8,888 296 111 30 38 21
3 Commercial Property 80,758 62,782 16,593 1,383 462 228 64 36
4 Construction 13,211 6,508 6,657 46 148 44 43 19
5 Electricity, Gas & Water Supply 23,658 11,590 11,192 876 4 3 1 1
6 Entertainment, Leisure & Tourism 17,670 13,750 3,829 91 151 30 33 16
7 Financial, Investment & Insurance 402,472 86,293 51,424 264,755 23 8 9 6
8 Government & Official Institutions 146,648 2,436 1,173 143,039 - - - -
9 Manufacturing 50,831 26,053 23,205 1,573 216 78 43 28
10 Personal Lending 20,788 6,897 13,853 38 99 22 81 15
11 Residential Mortgage 554,118 498,599 54,108 1,411 5,987 257 436 57
12 Retail Trade 17,969 11,480 6,418 71 219 122 101 83
13 Transport & Storage 21,170 11,644 8,736 790 62 34 18 12
14 Wholesale Trade 25,252 12,706 11,439 1,107 55 20 18 11
15 Other 32,415 18,191 11,062 3,162 202 98 82 61
16 Total 1,486,120 824,603 242,094 419,423 8,410 1,145 1,050 399
"""


# ---------------------------------------------------------------------------
# 1. Schema contract
# ---------------------------------------------------------------------------

def test_every_row_passes_well_formed_invariant() -> None:
    rows = parse_industry_text(_ANZ_INDUSTRY_MINI, source_publication="x", source_page=43)
    for r in rows:
        assert_row_well_formed(r)


def test_canonical_columns_plus_optional_extras() -> None:
    real = (
        Path(__file__).resolve().parents[3]
        / "data" / "raw" / "pillar3" / "ANZ_FY2025_Pillar3_Annual.pdf"
    )
    if not real.exists():
        pytest.skip(f"ANZ FY2025 PDF not present at {real}")
    df = extract_anz_industry_rows(real)
    assert COL_PROVISION_BASIS in df.columns
    assert COL_GROSS_CARRYING_COMPONENT in df.columns


# ---------------------------------------------------------------------------
# 2. Row counts — 5 rows per industry (3 exposure + 1 NPE + 1 provision)
# ---------------------------------------------------------------------------

def test_row_count_industries_times_five() -> None:
    rows = parse_industry_text(_ANZ_INDUSTRY_MINI, source_publication="x", source_page=43)
    assert len(rows) == len(ANZ_INDUSTRIES) * 5

    by_metric: dict[str, int] = {}
    for r in rows:
        by_metric[r["metric"]] = by_metric.get(r["metric"], 0) + 1
    assert by_metric[METRIC_EXPOSURE] == len(ANZ_INDUSTRIES) * 3  # 3 components
    assert by_metric[METRIC_NPE] == len(ANZ_INDUSTRIES)
    assert by_metric[METRIC_PROVISIONS] == len(ANZ_INDUSTRIES)


def test_no_total_gross_carrying_row_emitted() -> None:
    """Total gross carrying = sum of three components; not emitted as a row."""
    rows = parse_industry_text(_ANZ_INDUSTRY_MINI, source_publication="x", source_page=43)
    exposure_components = {
        r.get(COL_GROSS_CARRYING_COMPONENT)
        for r in rows if r["metric"] == METRIC_EXPOSURE
    }
    assert exposure_components == {
        GROSS_COMPONENT_LOANS, GROSS_COMPONENT_OFF_BS, GROSS_COMPONENT_OTHER,
    }
    assert "total" not in exposure_components


# ---------------------------------------------------------------------------
# 3. Five guardrails
# ---------------------------------------------------------------------------

def test_provenance_on_every_row() -> None:
    rows = parse_industry_text(_ANZ_INDUSTRY_MINI,
                               source_publication="ANZ Pillar 3 — September 2025",
                               source_page=43)
    for r in rows:
        assert r["source_publication"] == "ANZ Pillar 3 — September 2025"
        assert r["source_table_ref"] == "Exposures/NPE/Provisions by industry"
        assert r["source_page"] == 43
        assert r["bank_code"] == "anz"
        assert r["data_source"] == "pillar3_anz"
        assert r["aggregation_level"] == "single_bank"


def test_government_dashes_emit_honest_zero() -> None:
    """Per instructions §5.2 + recon §1.8, Govt all-dashes = honest zero."""
    rows = parse_industry_text(_ANZ_INDUSTRY_MINI, source_publication="x", source_page=43)
    govt_npe = next(
        r for r in rows
        if r["industry_published"] == "Government & Official Institutions"
        and r["metric"] == METRIC_NPE
    )
    assert govt_npe["value_aud_m"] == 0.0
    assert govt_npe["redaction_reason"] is None

    govt_prov = next(
        r for r in rows
        if r["industry_published"] == "Government & Official Institutions"
        and r["metric"] == METRIC_PROVISIONS
    )
    assert govt_prov["value_aud_m"] == 0.0
    assert govt_prov["redaction_reason"] is None


def test_dash_for_non_govt_industries_still_redacted() -> None:
    """Honest-zero override applies ONLY to the listed honest-zero
    industries. Other industries with dashes remain redacted under
    guardrail 2."""
    snippet = _ANZ_INDUSTRY_MINI.replace(
        "9 Manufacturing 50,831 26,053 23,205 1,573 216 78 43 28",
        "9 Manufacturing 50,831 26,053 23,205 1,573 - 78 43 28",
    )
    rows = parse_industry_text(snippet, source_publication="x", source_page=43)
    mfg_npe = next(
        r for r in rows
        if r["industry_published"] == "Manufacturing" and r["metric"] == METRIC_NPE
    )
    assert mfg_npe["value_aud_m"] is None
    assert mfg_npe["redaction_reason"] == REDACTION_DASH_OR_HYPHEN


def test_as_of_date_taken_from_table_marker() -> None:
    rows = parse_industry_text(_ANZ_INDUSTRY_MINI, source_publication="x", source_page=43)
    assert {r["as_of_date"] for r in rows} == {date(2025, 9, 30)}


def test_missing_as_at_raises() -> None:
    bad = _ANZ_INDUSTRY_MINI.replace("Sep 25", "")
    with pytest.raises(ValueError, match="Sep <yy>"):
        parse_industry_text(bad, source_publication="x", source_page=43)


def test_no_contributing_banks_column() -> None:
    rows = parse_industry_text(_ANZ_INDUSTRY_MINI, source_publication="x", source_page=43)
    assert all("contributing_banks" not in r for r in rows)


def test_stocks_have_no_period_length() -> None:
    rows = parse_industry_text(_ANZ_INDUSTRY_MINI, source_publication="x", source_page=43)
    assert all(r["period_length_months"] is None for r in rows)


# ---------------------------------------------------------------------------
# 4. ANZ-specific quirks
# ---------------------------------------------------------------------------

def test_gross_carrying_component_split_three_ways() -> None:
    rows = parse_industry_text(_ANZ_INDUSTRY_MINI, source_publication="x", source_page=43)
    agri_exposure = [
        r for r in rows
        if r["industry_published"] == "Agriculture, Forestry, Fishing & Mining"
        and r["metric"] == METRIC_EXPOSURE
    ]
    assert len(agri_exposure) == 3
    by_comp = {r[COL_GROSS_CARRYING_COMPONENT]: r["value_aud_m"] for r in agri_exposure}
    # Per published table: loans=41326, off_bs=13517, other=785
    assert by_comp[GROSS_COMPONENT_LOANS] == 41_326.0
    assert by_comp[GROSS_COMPONENT_OFF_BS] == 13_517.0
    assert by_comp[GROSS_COMPONENT_OTHER] == 785.0


def test_provision_basis_aasb9_on_every_provision_row() -> None:
    rows = parse_industry_text(_ANZ_INDUSTRY_MINI, source_publication="x", source_page=43)
    prov_rows = [r for r in rows if r["metric"] == METRIC_PROVISIONS]
    assert len(prov_rows) == len(ANZ_INDUSTRIES)
    for r in prov_rows:
        assert r[COL_PROVISION_BASIS] == PROVISION_BASIS_AASB9_STAGE3


def test_no_write_off_rows_emitted() -> None:
    rows = parse_industry_text(_ANZ_INDUSTRY_MINI, source_publication="x", source_page=43)
    write_offs = [r for r in rows if r["metric"] == METRIC_WRITE_OFFS]
    assert write_offs == []


def test_geography_total_for_every_row() -> None:
    rows = parse_industry_text(_ANZ_INDUSTRY_MINI, source_publication="x", source_page=43)
    assert all(r["geography"] == "Total" for r in rows)


# ---------------------------------------------------------------------------
# 5. Strict label matching
# ---------------------------------------------------------------------------

def test_unknown_industry_label_raises() -> None:
    bad = _ANZ_INDUSTRY_MINI.replace(
        "4 Construction 13,211 6,508 6,657 46 148 44 43 19",
        "4 Quantum Computing 13,211 6,508 6,657 46 148 44 43 19",
    )
    with pytest.raises(ValueError, match="unmatched industry"):
        parse_industry_text(bad, source_publication="x", source_page=43)


# ---------------------------------------------------------------------------
# 6. Consumer routing — Personal / Residential Mortgage
# ---------------------------------------------------------------------------

def test_personal_lending_routes_to_consumer_personal() -> None:
    canon = resolve("anz", "Personal Lending")
    assert canon == "consumer_lending_personal"
    assert is_business_lending(canon) is False


def test_residential_mortgage_routes_to_consumer_residential() -> None:
    canon = resolve("anz", "Residential Mortgage")
    assert canon == "consumer_lending_residential_mortgage"
    assert is_business_lending(canon) is False


def test_personal_lending_does_not_appear_under_business_lending() -> None:
    """Per instructions §5.4 — assert routing isolation."""
    rows = parse_industry_text(_ANZ_INDUSTRY_MINI, source_publication="x", source_page=43)
    personal = [r for r in rows if r["industry_published"] == "Personal Lending"]
    assert len(personal) > 0
    for r in personal:
        canon = resolve("anz", r["industry_published"])
        assert not canon.startswith("business_lending_")
        assert is_business_lending(canon) is False


def test_residential_mortgage_does_not_appear_under_business_lending() -> None:
    rows = parse_industry_text(_ANZ_INDUSTRY_MINI, source_publication="x", source_page=43)
    rm = [r for r in rows if r["industry_published"] == "Residential Mortgage"]
    assert len(rm) > 0
    for r in rm:
        canon = resolve("anz", r["industry_published"])
        assert not canon.startswith("business_lending_")
        assert is_business_lending(canon) is False


def test_every_anz_label_resolves() -> None:
    for ind in ANZ_INDUSTRIES:
        resolve("anz", ind)


# ---------------------------------------------------------------------------
# 7. Real-PDF golden-row pins
# ---------------------------------------------------------------------------

REAL_PDF = (
    Path(__file__).resolve().parents[3]
    / "data" / "raw" / "pillar3" / "ANZ_FY2025_Pillar3_Annual.pdf"
)


@pytest.mark.skipif(not REAL_PDF.exists(),
                    reason=f"ANZ FY2025 PDF not present at {REAL_PDF}")
def test_real_pdf_round_trip_and_golden_values() -> None:
    df = extract_anz_industry_rows(REAL_PDF)
    assert len(df) == 75  # 15 industries × 5 rows

    def get(ind: str, met: str, comp: str | None = None) -> float | None:
        sl = (df.industry_published == ind) & (df.metric == met)
        if comp is not None:
            sl = sl & (df.gross_carrying_component == comp)
        s = df[sl].value_aud_m
        return None if s.empty else s.iloc[0]

    # Five golden values pinned to the published FY2025 ANZ table.
    assert get(
        "Agriculture, Forestry, Fishing & Mining", METRIC_EXPOSURE, "loans"
    ) == 41_326.0
    assert get("Manufacturing", METRIC_NPE) == 216.0
    assert get("Residential Mortgage", METRIC_PROVISIONS) == 57.0
    assert get("Construction", METRIC_EXPOSURE, "off_balance_sheet") == 6_657.0
    assert get("Government & Official Institutions", METRIC_NPE) == 0.0  # honest zero


@pytest.mark.skipif(not REAL_PDF.exists(),
                    reason=f"ANZ FY2025 PDF not present at {REAL_PDF}")
def test_delegation_calls_extractor() -> None:
    direct = extract_anz_industry_rows(REAL_PDF)
    via_adapter = AnzPillar3PdfAdapter().extract_industry_rows(REAL_PDF)
    assert direct.equals(via_adapter)


# ---------------------------------------------------------------------------
# 8. Preservation rule sanity
# ---------------------------------------------------------------------------

def test_existing_adapter_api_preserved() -> None:
    a = AnzPillar3PdfAdapter()
    assert hasattr(a, "normalise")
    assert hasattr(a, "source_name")
    assert hasattr(a, "canonical_columns")
    assert a.source_name == "anz_pillar3_annual"
    assert a.FISCAL_YEAR_END_MONTH == 9
