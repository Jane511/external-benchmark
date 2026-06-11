"""Tests for CBA Pillar 3 industry-table extraction (Phase 3.B.3 §B.6)."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from ingestion.adapters.anzsic_harmonisation import (
    is_business_lending,
    resolve,
)
from ingestion.adapters.cba_pillar3_industry import (
    _CBA_CRB_F_INDUSTRIES,
    _CBA_INDUSTRIES_LEFT,
    _CBA_INDUSTRIES_RIGHT,
    _CBA_PORTFOLIO_ORDER,
    extract_cba_industry_rows,
    parse_crb_e_text,
    parse_crb_f_text,
)
from ingestion.adapters.pillar3_industry_schema import (
    COL_PORTFOLIO_TYPE,
    COL_PROVISION_BASIS,
    METRIC_EXPOSURE,
    METRIC_NPE,
    METRIC_PROVISIONS,
    METRIC_WRITE_OFFS,
    PORTFOLIO_TYPE_VALUES,
    PROVISION_BASIS_APS220,
    REDACTION_DASH_OR_HYPHEN,
    assert_row_well_formed,
    is_honest_zero_table,
)


REAL_PDF = (
    Path(__file__).resolve().parents[3]
    / "data" / "raw" / "pillar3" / "CBA_FY2025_Pillar3_Annual.pdf"
)


# ---------------------------------------------------------------------------
# 1. Schema contract
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not REAL_PDF.exists(),
                    reason=f"CBA FY2025 PDF not present at {REAL_PDF}")
def test_every_row_passes_well_formed_invariant() -> None:
    df = extract_cba_industry_rows(REAL_PDF)
    for _, r in df.iterrows():
        assert_row_well_formed(r.to_dict())


@pytest.mark.skipif(not REAL_PDF.exists(),
                    reason=f"CBA FY2025 PDF not present at {REAL_PDF}")
def test_canonical_columns_plus_optional_extras() -> None:
    df = extract_cba_industry_rows(REAL_PDF)
    assert COL_PROVISION_BASIS in df.columns
    assert COL_PORTFOLIO_TYPE in df.columns


# ---------------------------------------------------------------------------
# 2. Row counts — 390 = 300 exposures + 90 from CRB(f)(i)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not REAL_PDF.exists(),
                    reason=f"CBA FY2025 PDF not present at {REAL_PDF}")
def test_real_pdf_row_count_matches_recon() -> None:
    df = extract_cba_industry_rows(REAL_PDF)
    assert len(df) == 390  # 10 portfolios × 15 industries × 2 + 15 × 3 × 2
    by_metric = df["metric"].value_counts().to_dict()
    assert by_metric[METRIC_EXPOSURE] == 300
    assert by_metric[METRIC_NPE] == 30
    assert by_metric[METRIC_PROVISIONS] == 30
    assert by_metric[METRIC_WRITE_OFFS] == 30


@pytest.mark.skipif(not REAL_PDF.exists(),
                    reason=f"CBA FY2025 PDF not present at {REAL_PDF}")
def test_two_periods_present_with_correct_period_lengths() -> None:
    df = extract_cba_industry_rows(REAL_PDF)
    assert set(df["as_of_date"].unique()) == {date(2025, 6, 30), date(2024, 12, 31)}
    # Write-off rows for Jun-25 should be period_length_months=12;
    # for Dec-24 should be 6.
    wo = df[df["metric"] == METRIC_WRITE_OFFS]
    jun = wo[wo["as_of_date"] == date(2025, 6, 30)]
    dec = wo[wo["as_of_date"] == date(2024, 12, 31)]
    assert (jun["period_length_months"] == 12).all()
    assert (dec["period_length_months"] == 6).all()


# ---------------------------------------------------------------------------
# 3. Five guardrails
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not REAL_PDF.exists(),
                    reason=f"CBA FY2025 PDF not present at {REAL_PDF}")
def test_provenance_on_every_row() -> None:
    df = extract_cba_industry_rows(REAL_PDF)
    assert (df["bank_code"] == "cba").all()
    assert (df["data_source"] == "pillar3_cba").all()
    assert (df["aggregation_level"] == "single_bank").all()
    table_refs = set(df["source_table_ref"].unique())
    assert table_refs == {"CRB(e)(ii)", "CRB(f)(i)"}
    assert (df["source_page"].isin({36, 37, 39, 40})).all()


@pytest.mark.skipif(not REAL_PDF.exists(),
                    reason=f"CBA FY2025 PDF not present at {REAL_PDF}")
def test_government_dashes_emit_honest_zero() -> None:
    """Govt row in CRB(f)(i) is all-dash for both periods → honest zero."""
    df = extract_cba_industry_rows(REAL_PDF)
    govt = df[
        (df["industry_published"] == "Government Administration & Defence")
        & (df["source_table_ref"] == "CRB(f)(i)")
    ]
    assert len(govt) == 6  # 3 metrics × 2 periods
    assert (govt["value_aud_m"] == 0.0).all()
    assert govt["redaction_reason"].isna().all()


@pytest.mark.skipif(not REAL_PDF.exists(),
                    reason=f"CBA FY2025 PDF not present at {REAL_PDF}")
def test_crb_e_matrix_dashes_emit_honest_zero() -> None:
    """Phase 3.B.3 §B.1 §8 ruling: CBA CRB(e)(ii) dashes are
    structural zeros (the portfolio type does not extend to that
    industry). Sovereign × Manufacturing is the canonical example."""
    df = extract_cba_industry_rows(REAL_PDF)
    sov_mfg = df[
        (df["portfolio_type"] == "sovereign")
        & (df["industry_published"] == "Manufacturing")
        & (df["source_table_ref"] == "CRB(e)(ii)")
    ]
    assert len(sov_mfg) == 2  # both periods
    assert (sov_mfg["value_aud_m"] == 0.0).all()
    assert sov_mfg["redaction_reason"].isna().all()


def test_table_level_honest_zero_override_is_cba_only() -> None:
    """The honest-zero table override applies to (cba, CRB(e)(ii)) only —
    not to CBA CRB(f)(i), nor to any other bank."""
    assert is_honest_zero_table("cba", "CRB(e)(ii)") is True
    assert is_honest_zero_table("cba", "CRB(f)(i)") is False
    assert is_honest_zero_table("wbc", "CRB(e)") is False
    assert is_honest_zero_table("nab", "EaD/NPE/Provisions by industry") is False
    assert is_honest_zero_table("anz", "Exposures/NPE/Provisions by industry") is False


@pytest.mark.skipif(not REAL_PDF.exists(),
                    reason=f"CBA FY2025 PDF not present at {REAL_PDF}")
def test_no_synthetic_dates() -> None:
    """Every emitted as_of_date must equal one of the two published periods."""
    df = extract_cba_industry_rows(REAL_PDF)
    assert set(df["as_of_date"].unique()).issubset(
        {date(2025, 6, 30), date(2024, 12, 31)}
    )


@pytest.mark.skipif(not REAL_PDF.exists(),
                    reason=f"CBA FY2025 PDF not present at {REAL_PDF}")
def test_no_contributing_banks_column() -> None:
    df = extract_cba_industry_rows(REAL_PDF)
    assert "contributing_banks" not in df.columns


@pytest.mark.skipif(not REAL_PDF.exists(),
                    reason=f"CBA FY2025 PDF not present at {REAL_PDF}")
def test_stocks_have_no_period_length_writeoffs_have_period_length() -> None:
    df = extract_cba_industry_rows(REAL_PDF)
    for stock_metric in (METRIC_EXPOSURE, METRIC_NPE, METRIC_PROVISIONS):
        assert df[df["metric"] == stock_metric]["period_length_months"].isna().all()
    wo = df[df["metric"] == METRIC_WRITE_OFFS]
    assert wo["period_length_months"].isin({6, 12}).all()


# ---------------------------------------------------------------------------
# 4. CBA-specific quirks
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not REAL_PDF.exists(),
                    reason=f"CBA FY2025 PDF not present at {REAL_PDF}")
def test_portfolio_type_populated_for_all_exposure_rows() -> None:
    df = extract_cba_industry_rows(REAL_PDF)
    exposure = df[df["metric"] == METRIC_EXPOSURE]
    assert exposure[COL_PORTFOLIO_TYPE].notna().all()
    assert exposure[COL_PORTFOLIO_TYPE].isin(PORTFOLIO_TYPE_VALUES).all()
    # All 10 portfolio types should appear.
    assert set(exposure[COL_PORTFOLIO_TYPE].unique()) == set(_CBA_PORTFOLIO_ORDER)


@pytest.mark.skipif(not REAL_PDF.exists(),
                    reason=f"CBA FY2025 PDF not present at {REAL_PDF}")
def test_portfolio_type_null_for_non_exposure_rows() -> None:
    df = extract_cba_industry_rows(REAL_PDF)
    non_exposure = df[df["metric"] != METRIC_EXPOSURE]
    assert non_exposure[COL_PORTFOLIO_TYPE].isna().all()


@pytest.mark.skipif(not REAL_PDF.exists(),
                    reason=f"CBA FY2025 PDF not present at {REAL_PDF}")
def test_provision_basis_aps220_on_every_provision_row() -> None:
    df = extract_cba_industry_rows(REAL_PDF)
    prov = df[df["metric"] == METRIC_PROVISIONS]
    assert (prov[COL_PROVISION_BASIS] == PROVISION_BASIS_APS220).all()


@pytest.mark.skipif(not REAL_PDF.exists(),
                    reason=f"CBA FY2025 PDF not present at {REAL_PDF}")
def test_aps120_securitisation_exclusion_in_npe_publication() -> None:
    """NPE rows must carry the APS 120 exclusion note in source_publication."""
    df = extract_cba_industry_rows(REAL_PDF)
    npe = df[df["metric"] == METRIC_NPE]
    assert npe["source_publication"].str.contains(
        "APS 120 securitisation excluded", regex=False,
    ).all()
    # Non-NPE rows must NOT carry the note.
    non_npe = df[df["metric"] != METRIC_NPE]
    assert not non_npe["source_publication"].str.contains(
        "APS 120", regex=False,
    ).any()


# ---------------------------------------------------------------------------
# 5. Golden-row pins (mix of large and small exposures across both periods)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not REAL_PDF.exists(),
                    reason=f"CBA FY2025 PDF not present at {REAL_PDF}")
def test_golden_values() -> None:
    df = extract_cba_industry_rows(REAL_PDF)

    def get(period: date, ind: str, met: str, pf: str | None = None) -> float | None:
        sl = (df.as_of_date == period) & (df.industry_published == ind) & (df.metric == met)
        if pf is not None:
            sl = sl & (df[COL_PORTFOLIO_TYPE] == pf)
        s = df[sl].value_aud_m
        return None if s.empty else s.iloc[0]

    jun25 = date(2025, 6, 30)
    dec24 = date(2024, 12, 31)

    # CRB(e)(ii) — exposure across both periods, mix of portfolios
    assert get(jun25, "Manufacturing", METRIC_EXPOSURE,
               "corporate_incl_large_and_sme") == 13_977.0
    assert get(jun25, "Government Administration & Defence", METRIC_EXPOSURE,
               "sovereign") == 140_364.0
    assert get(jun25, "Consumer", METRIC_EXPOSURE,
               "residential_mortgage") == 709_385.0
    assert get(dec24, "Consumer", METRIC_EXPOSURE,
               "residential_mortgage") == 689_600.0

    # CRB(f)(i) — NPE, full-year and half-year actual losses
    assert get(jun25, "Consumer", METRIC_NPE) == 8_205.0
    assert get(jun25, "Consumer", METRIC_WRITE_OFFS) == 408.0  # full year
    assert get(dec24, "Consumer", METRIC_WRITE_OFFS) == 199.0  # half year
    # Mining recovery — published as "(1)"; absolute value emitted.
    assert get(jun25, "Mining, Oil & Gas", METRIC_WRITE_OFFS) == 1.0


# ---------------------------------------------------------------------------
# 6. Distinguish honest-zero from redacted on a non-Government industry
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not REAL_PDF.exists(),
                    reason=f"CBA FY2025 PDF not present at {REAL_PDF}")
def test_electricity_gas_water_zero_npe_via_honest_zero_or_dash() -> None:
    """Per CRB(f)(i) FY25, Electricity/Gas/Water has all-dash NPE row.
    This is NOT the Govt-row override and is NOT the CBA matrix override
    — Section A rules apply: dash → null + redaction reason. Confirms
    the honest-zero rule is scoped to Govt only in CRB(f)(i)."""
    df = extract_cba_industry_rows(REAL_PDF)
    egw = df[
        (df.industry_published == "Electricity, Gas & Water")
        & (df.source_table_ref == "CRB(f)(i)")
        & (df.metric == METRIC_NPE)
    ]
    assert len(egw) == 2  # both periods
    # All cells are dashes → null + redacted (not Govt, not CBA matrix override).
    assert egw["value_aud_m"].isna().all()
    assert (egw["redaction_reason"] == REDACTION_DASH_OR_HYPHEN).all()


# ---------------------------------------------------------------------------
# 7. Strict label matching — unknown labels raise
# ---------------------------------------------------------------------------

def test_crb_f_unknown_industry_raises() -> None:
    snippet = """\
CRB(f)(i): NPE etc
30 June 2025
Industry Sector $M $M $M
Quantum Computing 100 50 10
Total 100 50 10
"""
    with pytest.raises(ValueError, match="unmatched industry"):
        parse_crb_f_text(
            snippet, source_publication="x", source_page=39,
            period_length_months=12,
        )


# ---------------------------------------------------------------------------
# 8. Harmonisation routing — every CBA label resolves
# ---------------------------------------------------------------------------

def test_every_cba_published_label_resolves_via_harmonisation() -> None:
    all_labels = set(_CBA_INDUSTRIES_LEFT) | set(_CBA_INDUSTRIES_RIGHT) | set(_CBA_CRB_F_INDUSTRIES)
    for label in all_labels:
        canon = resolve("cba", label)
        assert canon, f"CBA label {label!r} resolved to empty bucket"


def test_cba_consumer_routes_to_consumer_combined() -> None:
    canon = resolve("cba", "Consumer")
    assert canon == "consumer_combined"
    assert is_business_lending(canon) is False


def test_cba_health_and_entertainment_route_to_other() -> None:
    """Per Phase 2 Issue 6 ruling — both pool to canonical 'other'."""
    assert resolve("cba", "Health & Community Services") == "other"
    assert resolve("cba", "Entertainment, Leisure & Tourism") == "other"


# ---------------------------------------------------------------------------
# 9. Preservation rule — existing CbaPillar3PdfAdapter untouched
# ---------------------------------------------------------------------------

def test_existing_cba_adapter_untouched() -> None:
    """Importing the existing CR6/CR10 adapter must still work and its
    public API surface must be unchanged."""
    from ingestion.adapters.cba_pillar3_pdf_adapter import CbaPillar3PdfAdapter
    a = CbaPillar3PdfAdapter()
    assert a.source_name == "cba_pillar3_annual"
    assert hasattr(a, "normalise")
    assert hasattr(a, "canonical_columns")
