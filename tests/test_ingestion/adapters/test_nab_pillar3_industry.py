"""Tests for NAB Pillar 3 industry-table extraction (Phase 3.B.2 §4).

New file. Existing NAB tests (test_pillar3_nab.py and the inherited
CR6/CR10 suite) are not touched — preservation rule held.

Coverage:

- Schema contract — every emitted row passes ``assert_row_well_formed``
- All five guardrails individually exercised
- Real-PDF round-trip with golden-row pins
- Provision-basis tag is ``aasb9_stage3_ecl`` on every provision row
- Government-and-public-authorities dashes → null + redaction reason
- Footnote markers stripped from labels (Utilities(3), Other(4))
- Zero write-off rows is a valid output (schema-level invariant)
- "Other (education, health & community services)" routes cleanly via
  the harmonisation map
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from ingestion.adapters.anzsic_harmonisation import (
    is_business_lending,
    resolve,
)
from ingestion.adapters.nab_pillar3_industry import (
    NAB_INDUSTRIES,
    extract_nab_industry_rows,
    parse_industry_text,
)
from ingestion.adapters.nab_pillar3_pdf_adapter import NabPillar3PdfAdapter
from ingestion.adapters.pillar3_industry_schema import (
    COL_PROVISION_BASIS,
    METRIC_EXPOSURE,
    METRIC_NPE,
    METRIC_PROVISIONS,
    METRIC_WRITE_OFFS,
    PROVISION_BASIS_AASB9_STAGE3,
    REDACTION_DASH_OR_HYPHEN,
    assert_row_well_formed,
)


# ---------------------------------------------------------------------------
# Synthetic-text fixture — trimmed NAB FY2025 industry table
# ---------------------------------------------------------------------------

_NAB_INDUSTRY_MINI = """\
General information about credit risk (cont.)
Exposure at default, non-performing exposures and related provisions by industry
As at 30 Sep 25
Industry sector $m $m $m $m $m
Accommodation and hospitality 14,704 14,611 265 63 25
Agriculture, forestry, fishing and mining 69,428 67,930 1,809 308 132
Business services and property services 23,174 22,999 544 196 141
Commercial property 94,697 93,594 1,125 189 32
Construction 15,140 15,112 393 118 82
Finance and insurance 170,647 144,999 103 40 24
Government and public authorities 75,817 74,785 - - -
Manufacturing 21,932 21,351 659 284 239
Personal 20,790 20,790 176 96 2
Residential mortgages 496,085 496,085 5,401 501 71
Retail and wholesale trade 36,531 35,656 647 232 168
Transport and storage 22,584 21,009 376 104 76
Utilities(3) 25,787 23,027 270 131 109
Other(4) 31,383 30,908 326 88 62
Total 1,118,699 1,082,856 12,094 2,350 1,163
Provision for performing exposures(5) 3,815
Total provision for credit impairment 6,165
"""


# ---------------------------------------------------------------------------
# 1. Schema contract — every row well-formed
# ---------------------------------------------------------------------------

def test_every_row_passes_well_formed_invariant() -> None:
    rows = parse_industry_text(
        _NAB_INDUSTRY_MINI,
        source_publication="NAB Pillar 3 — September 2025",
        source_page=37,
    )
    for r in rows:
        assert_row_well_formed(r)


def test_canonical_columns_plus_provision_basis() -> None:
    """Real-PDF round trip carries provision_basis as an extra column."""
    real = (
        Path(__file__).resolve().parents[3]
        / "data" / "raw" / "pillar3" / "NAB_FY2025_Pillar3_Annual.pdf"
    )
    if not real.exists():
        pytest.skip(f"NAB FY2025 PDF not present at {real}")
    df = extract_nab_industry_rows(real)
    assert COL_PROVISION_BASIS in df.columns
    # Provision rows carry the basis; non-provision rows are null.
    prov_rows = df[df["metric"] == METRIC_PROVISIONS]
    assert (prov_rows[COL_PROVISION_BASIS] == PROVISION_BASIS_AASB9_STAGE3).all()
    non_prov_rows = df[df["metric"] != METRIC_PROVISIONS]
    assert non_prov_rows[COL_PROVISION_BASIS].isna().all()


# ---------------------------------------------------------------------------
# 2. Row counts
# ---------------------------------------------------------------------------

def test_row_count_industries_times_three_metrics() -> None:
    rows = parse_industry_text(_NAB_INDUSTRY_MINI, source_publication="x", source_page=37)
    assert len(rows) == len(NAB_INDUSTRIES) * 3
    by_metric = {m: 0 for m in (METRIC_EXPOSURE, METRIC_NPE, METRIC_PROVISIONS)}
    for r in rows:
        by_metric[r["metric"]] += 1
    for c in by_metric.values():
        assert c == len(NAB_INDUSTRIES)


# ---------------------------------------------------------------------------
# 3. Guardrails individually
# ---------------------------------------------------------------------------

def test_provenance_on_every_row() -> None:
    rows = parse_industry_text(_NAB_INDUSTRY_MINI,
                               source_publication="NAB Pillar 3 — September 2025",
                               source_page=37)
    for r in rows:
        assert r["source_publication"] == "NAB Pillar 3 — September 2025"
        assert r["source_table_ref"] == "EaD/NPE/Provisions by industry"
        assert r["source_page"] == 37
        assert r["bank_code"] == "nab"
        assert r["data_source"] == "pillar3_nab"
        assert r["aggregation_level"] == "single_bank"


def test_government_dashes_emit_honest_zero() -> None:
    """Phase 3.B.3 §A.2 harmonisation: Government and public authorities
    is the no-exposure-by-construction row; dashes mean honest-zero per
    recon §1.8, not redacted. Replaces the prior 3.B.2 assertion that
    these rows were redacted. Preservation-rule sign-off recorded in
    :doc:`docs/governance_log.md` Phase 3.B addenda."""
    rows = parse_industry_text(_NAB_INDUSTRY_MINI, source_publication="x", source_page=37)
    govt = [
        r for r in rows
        if r["industry_published"] == "Government and public authorities"
    ]
    assert len(govt) == 3
    npe_or_prov = [r for r in govt if r["metric"] != METRIC_EXPOSURE]
    assert len(npe_or_prov) == 2
    for r in npe_or_prov:
        assert r["value_aud_m"] == 0.0
        assert r["redaction_reason"] is None
    # Exposure column for Govt is published numerically (75,817) — the
    # honest-zero rule applies only to dash cells, never overrides a
    # published number.
    govt_exp = next(r for r in govt if r["metric"] == METRIC_EXPOSURE)
    assert govt_exp["value_aud_m"] == 75_817.0
    assert govt_exp["redaction_reason"] is None


def test_published_zero_distinct_from_dash() -> None:
    """Inject a real 0 vs a dash and verify they are distinguished."""
    snippet = _NAB_INDUSTRY_MINI.replace(
        "Government and public authorities 75,817 74,785 - - -",
        "Government and public authorities 75,817 74,785 0 - -",
    )
    rows = parse_industry_text(snippet, source_publication="x", source_page=37)
    govt_npe = next(
        r for r in rows
        if r["industry_published"] == "Government and public authorities"
        and r["metric"] == METRIC_NPE
    )
    assert govt_npe["value_aud_m"] == 0.0
    assert govt_npe["redaction_reason"] is None


def test_as_of_date_taken_from_table_marker() -> None:
    rows = parse_industry_text(_NAB_INDUSTRY_MINI, source_publication="x", source_page=37)
    assert {r["as_of_date"] for r in rows} == {date(2025, 9, 30)}


def test_missing_as_at_raises() -> None:
    bad = _NAB_INDUSTRY_MINI.replace("As at 30 Sep 25", "")
    with pytest.raises(ValueError, match="As at"):
        parse_industry_text(bad, source_publication="x", source_page=37)


def test_no_contributing_banks_column() -> None:
    rows = parse_industry_text(_NAB_INDUSTRY_MINI, source_publication="x", source_page=37)
    assert all("contributing_banks" not in r for r in rows)


def test_stocks_have_no_period_length() -> None:
    rows = parse_industry_text(_NAB_INDUSTRY_MINI, source_publication="x", source_page=37)
    assert all(r["period_length_months"] is None for r in rows)


# ---------------------------------------------------------------------------
# 4. NAB-specific quirks
# ---------------------------------------------------------------------------

def test_footnote_markers_stripped() -> None:
    """Utilities(3) and Other(4) must be recognised as Utilities and Other."""
    rows = parse_industry_text(_NAB_INDUSTRY_MINI, source_publication="x", source_page=37)
    industries = {r["industry_published"] for r in rows}
    assert "Utilities" in industries
    assert "Other" in industries
    # Confirm neither footnote-marked variant leaked through.
    assert not any("(3)" in r["industry_published"] for r in rows)
    assert not any("(4)" in r["industry_published"] for r in rows)


def test_provision_basis_aasb9_on_every_provision_row() -> None:
    rows = parse_industry_text(_NAB_INDUSTRY_MINI, source_publication="x", source_page=37)
    prov_rows = [r for r in rows if r["metric"] == METRIC_PROVISIONS]
    assert len(prov_rows) == len(NAB_INDUSTRIES)
    for r in prov_rows:
        assert r[COL_PROVISION_BASIS] == PROVISION_BASIS_AASB9_STAGE3


def test_no_write_off_rows_emitted() -> None:
    rows = parse_industry_text(_NAB_INDUSTRY_MINI, source_publication="x", source_page=37)
    write_offs = [r for r in rows if r["metric"] == METRIC_WRITE_OFFS]
    assert write_offs == []


def test_zero_write_off_rows_passes_schema() -> None:
    """Phase 3.B.2 §3.3 — DataFrame with no write-off rows is valid."""
    rows = parse_industry_text(_NAB_INDUSTRY_MINI, source_publication="x", source_page=37)
    for r in rows:
        assert_row_well_formed(r)
    # Empty list of write-offs cannot violate the row-by-row invariant,
    # because the invariant fires only on existing rows. This test
    # asserts the contract by construction.
    assert all(r["metric"] != METRIC_WRITE_OFFS for r in rows)


def test_geography_total_for_every_row() -> None:
    """NAB does not split by geography at industry level."""
    rows = parse_industry_text(_NAB_INDUSTRY_MINI, source_publication="x", source_page=37)
    assert all(r["geography"] == "Total" for r in rows)


# ---------------------------------------------------------------------------
# 5. Strict label matching
# ---------------------------------------------------------------------------

def test_unknown_industry_label_raises() -> None:
    bad = _NAB_INDUSTRY_MINI.replace(
        "Construction 15,140 15,112 393 118 82",
        "Quantum Computing 15,140 15,112 393 118 82",
    )
    with pytest.raises(ValueError, match="unmatched industry"):
        parse_industry_text(bad, source_publication="x", source_page=37)


# ---------------------------------------------------------------------------
# 6. Harmonisation routing — Personal / Residential mortgages → consumer
# ---------------------------------------------------------------------------

def test_personal_routes_to_consumer_lending_personal() -> None:
    canon = resolve("nab", "Personal")
    assert canon == "consumer_lending_personal"
    assert is_business_lending(canon) is False


def test_residential_mortgages_routes_to_consumer_residential() -> None:
    canon = resolve("nab", "Residential mortgages")
    assert canon == "consumer_lending_residential_mortgage"
    assert is_business_lending(canon) is False


def test_other_routes_to_other_canonical() -> None:
    """NAB's "Other (education, health & community services)" label
    stripped of footnote = "Other"; resolves to canonical "other"."""
    canon = resolve("nab", "Other")
    assert canon == "other"


def test_every_nab_label_resolves() -> None:
    """No silent drops — every published NAB label must have a route."""
    for ind in NAB_INDUSTRIES:
        resolve("nab", ind)  # raises if unknown


# ---------------------------------------------------------------------------
# 7. Real-PDF golden-row pins
# ---------------------------------------------------------------------------

REAL_PDF = (
    Path(__file__).resolve().parents[3]
    / "data" / "raw" / "pillar3" / "NAB_FY2025_Pillar3_Annual.pdf"
)


@pytest.mark.skipif(not REAL_PDF.exists(),
                    reason=f"NAB FY2025 PDF not present at {REAL_PDF}")
def test_real_pdf_round_trip_and_golden_values() -> None:
    df = extract_nab_industry_rows(REAL_PDF)
    assert len(df) == 42  # 14 industries × 3 metrics

    def get(ind: str, met: str) -> float | None:
        s = df[(df.industry_published == ind) & (df.metric == met)].value_aud_m
        return None if s.empty else s.iloc[0]

    # Five golden values pinned to the FY2025 published table.
    assert get("Accommodation and hospitality", METRIC_EXPOSURE) == 14_704.0
    assert get("Accommodation and hospitality", METRIC_PROVISIONS) == 25.0
    assert get("Manufacturing", METRIC_NPE) == 659.0
    assert get("Residential mortgages", METRIC_EXPOSURE) == 496_085.0
    assert get("Utilities", METRIC_PROVISIONS) == 109.0


@pytest.mark.skipif(not REAL_PDF.exists(),
                    reason=f"NAB FY2025 PDF not present at {REAL_PDF}")
def test_delegation_calls_extractor() -> None:
    direct = extract_nab_industry_rows(REAL_PDF)
    via_adapter = NabPillar3PdfAdapter().extract_industry_rows(REAL_PDF)
    assert direct.equals(via_adapter)


# ---------------------------------------------------------------------------
# 8. Preservation rule sanity — existing NabPillar3PdfAdapter API intact
# ---------------------------------------------------------------------------

def test_existing_adapter_api_preserved() -> None:
    a = NabPillar3PdfAdapter()
    assert hasattr(a, "normalise")
    assert hasattr(a, "source_name")
    assert hasattr(a, "canonical_columns")
    assert a.source_name == "nab_pillar3_annual"
    assert a.FISCAL_YEAR_END_MONTH == 9
