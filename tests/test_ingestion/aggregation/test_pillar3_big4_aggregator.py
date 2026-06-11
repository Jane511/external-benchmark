"""Tests for the Big-4 Pillar 3 aggregator (Phase 3.C).

Layered:

1. **Refusal taxonomy** — five distinct refusals each tested with an
   informative-error assertion.
2. **Intra-bank aggregation** — collapses sub-row dimensions per
   bank, with null-propagation when sub-rows are partially redacted.
3. **Inter-bank aggregation** — sums per canonical bucket, populates
   `contributing_banks` and per-bank as-of-date manifest.
4. **Coverage ratios** — Big-4 EAD vs RBA D14.1 stocks; caveats
   propagate; low-coverage flag fires at the 60% threshold.
5. **Write-off period filter** — 12-month vs 6-month aggregations
   produce distinct rows.
6. **Consumer-bucket separation** — three buckets emitted separately;
   total-consumer view refused.
7. **As-of-date manifest** — no synthetic dates; manifest carries
   per-bank dates.
8. **Real-data integration** — full pipeline against FY2025 PDFs +
   live D14.1 file with ≥5 golden values pinned.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from ingestion.aggregation.pillar3_big4_aggregator import (
    LOW_COVERAGE_THRESHOLD,
    CrossBankAggregationRefusedError,
    IncompatiblePeriodLengthError,
    MissingProvenanceError,
    aggregate_provisions_same_basis,
    compute_coverage_ratios,
    flag_aps120_caveat_for_npe_aggregates,
    get_refusal_log,
    inter_bank_aggregate,
    intra_bank_industry_totals,
    RefusalEvent,
    reset_refusal_log,
)


# ---------------------------------------------------------------------------
# Synthetic fixtures
# ---------------------------------------------------------------------------

def _make_canonical_row(
    bank_code: str,
    industry: str,
    metric: str,
    value: float | None,
    *,
    as_of: date,
    period_length_months: int | None = None,
    geography: str = "Total",
    provision_basis: str | None = None,
    portfolio_type: str | None = None,
    gross_carrying_component: str | None = None,
    redaction_reason: str | None = None,
) -> dict:
    row = {
        "data_source": f"pillar3_{bank_code}",
        "aggregation_level": "single_bank",
        "bank_code": bank_code,
        "as_of_date": as_of,
        "period_length_months": period_length_months,
        "geography": geography,
        "industry_published": industry,
        "metric": metric,
        "value_aud_m": value,
        "redaction_reason": redaction_reason,
        "source_publication": f"{bank_code} Pillar 3 — synthetic",
        "source_table_ref": "synthetic",
        "source_page": 1,
    }
    if provision_basis is not None:
        row["provision_basis"] = provision_basis
    if portfolio_type is not None:
        row["portfolio_type"] = portfolio_type
    if gross_carrying_component is not None:
        row["gross_carrying_component"] = gross_carrying_component
    return row


def _df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame.from_records(rows)


# ---------------------------------------------------------------------------
# 1. Refusal taxonomy
# ---------------------------------------------------------------------------

def test_pd_aggregation_refused() -> None:
    """Phase 2 §5.4 — cross-bank PD aggregation is definitionally
    refused. The error must name the metric and the rule."""
    df = _df([
        _make_canonical_row("nab", "Manufacturing", "pd_value", 0.012,
                            as_of=date(2025, 9, 30)),
    ])
    with pytest.raises(CrossBankAggregationRefusedError) as ei:
        inter_bank_aggregate(df, metric="pd_value")
    msg = str(ei.value)
    assert "pd_value" in msg
    assert "master scales" in msg or "5.4" in msg


def test_provision_mixed_basis_refused() -> None:
    df = _df([
        _make_canonical_row("cba", "Manufacturing",
                            "individually_assessed_provision_aud_m", 100.0,
                            as_of=date(2025, 6, 30),
                            provision_basis="aps220_specific"),
        _make_canonical_row("nab", "Manufacturing",
                            "individually_assessed_provision_aud_m", 200.0,
                            as_of=date(2025, 9, 30),
                            provision_basis="aasb9_stage3_ecl"),
    ])
    with pytest.raises(CrossBankAggregationRefusedError) as ei:
        aggregate_provisions_same_basis(df, provision_basis="aasb9_stage3_ecl")
    assert "APS 220" in str(ei.value) or "aps220" in str(ei.value).lower()


def test_provision_aggregation_via_inter_bank_refused() -> None:
    """inter_bank_aggregate refuses provisions outright — callers
    must use aggregate_provisions_same_basis to make the basis check
    unmissable."""
    df = _df([
        _make_canonical_row("nab", "Manufacturing",
                            "individually_assessed_provision_aud_m", 100.0,
                            as_of=date(2025, 9, 30),
                            provision_basis="aasb9_stage3_ecl"),
    ])
    with pytest.raises(CrossBankAggregationRefusedError):
        inter_bank_aggregate(df, metric="individually_assessed_provision_aud_m")


def test_writeoff_no_period_length_refused() -> None:
    df = _df([
        _make_canonical_row("cba", "Manufacturing", "write_offs_aud_m", 38.0,
                            as_of=date(2025, 6, 30),
                            period_length_months=12),
        _make_canonical_row("wbc", "Manufacturing", "write_offs_aud_m", 10.0,
                            as_of=date(2025, 9, 30),
                            period_length_months=12),
    ])
    with pytest.raises(IncompatiblePeriodLengthError) as ei:
        inter_bank_aggregate(df, metric="write_offs_aud_m")
    assert "period_length_months" in str(ei.value)


def test_missing_provenance_refused() -> None:
    """An adapter that fails to populate provenance metadata is a
    contract violation; surfacing here protects the aggregation
    pipeline from silent garbage."""
    bad = pd.DataFrame([{
        "metric": "exposure_aud_m",
        "value_aud_m": 100.0,
        # missing data_source / bank_code / as_of_date / etc.
    }])
    with pytest.raises(MissingProvenanceError) as ei:
        intra_bank_industry_totals(bad)
    assert "provenance" in str(ei.value).lower()


# ---------------------------------------------------------------------------
# 2. Intra-bank aggregation
# ---------------------------------------------------------------------------

def test_intra_bank_pass_through_for_nab() -> None:
    """NAB has no sub-row dimensions; intra-bank should pass through
    unchanged in row count and value."""
    rows = [
        _make_canonical_row("nab", "Manufacturing", "exposure_aud_m", 21932.0,
                            as_of=date(2025, 9, 30)),
        _make_canonical_row("nab", "Construction", "exposure_aud_m", 15140.0,
                            as_of=date(2025, 9, 30)),
    ]
    out = intra_bank_industry_totals(_df(rows))
    assert len(out) == 2
    assert out[out["industry_published"] == "Manufacturing"]["value_aud_m"].iloc[0] == 21932.0


def test_intra_bank_collapses_anz_components_for_exposure() -> None:
    """ANZ exposure has 3 component rows per industry; intra-bank
    sums them to a single row tagged derived_from_intra_bank_sum=True."""
    rows = [
        _make_canonical_row("anz", "Manufacturing", "exposure_aud_m", 26053.0,
                            as_of=date(2025, 9, 30),
                            gross_carrying_component="loans"),
        _make_canonical_row("anz", "Manufacturing", "exposure_aud_m", 23205.0,
                            as_of=date(2025, 9, 30),
                            gross_carrying_component="off_balance_sheet"),
        _make_canonical_row("anz", "Manufacturing", "exposure_aud_m", 1573.0,
                            as_of=date(2025, 9, 30),
                            gross_carrying_component="other"),
    ]
    out = intra_bank_industry_totals(_df(rows))
    assert len(out) == 1
    assert out["value_aud_m"].iloc[0] == 26053.0 + 23205.0 + 1573.0
    assert bool(out["derived_from_intra_bank_sum"].iloc[0]) is True


def test_intra_bank_collapses_cba_portfolio_types_for_exposure() -> None:
    rows = [
        _make_canonical_row("cba", "Construction", "exposure_aud_m", 7398.0,
                            as_of=date(2025, 6, 30),
                            portfolio_type="corporate_incl_large_and_sme"),
        _make_canonical_row("cba", "Construction", "exposure_aud_m", 2955.0,
                            as_of=date(2025, 6, 30),
                            portfolio_type="sme_retail"),
        _make_canonical_row("cba", "Construction", "exposure_aud_m", 1252.0,
                            as_of=date(2025, 6, 30),
                            portfolio_type="rbnz_regulated_entities"),
    ]
    out = intra_bank_industry_totals(_df(rows))
    assert len(out) == 1
    assert out["value_aud_m"].iloc[0] == 7398.0 + 2955.0 + 1252.0


def test_intra_bank_null_propagation_partial_redaction() -> None:
    """If any sub-row is null, intra-bank sum is null with
    null_reason='partial_redaction_in_subrows'."""
    rows = [
        _make_canonical_row("anz", "Manufacturing", "exposure_aud_m", 26053.0,
                            as_of=date(2025, 9, 30),
                            gross_carrying_component="loans"),
        _make_canonical_row("anz", "Manufacturing", "exposure_aud_m", None,
                            as_of=date(2025, 9, 30),
                            gross_carrying_component="off_balance_sheet",
                            redaction_reason="published_as_dash"),
    ]
    out = intra_bank_industry_totals(_df(rows))
    assert len(out) == 1
    assert pd.isna(out["value_aud_m"].iloc[0])
    assert out["null_reason"].iloc[0] == "partial_redaction_in_subrows"


# ---------------------------------------------------------------------------
# 3. Inter-bank aggregation
# ---------------------------------------------------------------------------

def test_inter_bank_ead_sums_across_four_banks() -> None:
    """Manufacturing EAD: contribution from CBA + NAB + WBC + ANZ."""
    rows = [
        _make_canonical_row("cba", "Manufacturing", "exposure_aud_m", 13977.0,
                            as_of=date(2025, 6, 30)),
        _make_canonical_row("nab", "Manufacturing", "exposure_aud_m", 21932.0,
                            as_of=date(2025, 9, 30)),
        _make_canonical_row("wbc", "Manufacturing", "exposure_aud_m", 14763.0,
                            as_of=date(2025, 9, 30)),
        _make_canonical_row("anz", "Manufacturing", "exposure_aud_m", 50831.0,
                            as_of=date(2025, 9, 30)),
    ]
    out = inter_bank_aggregate(_df(rows), metric="exposure_aud_m")
    assert len(out) == 1
    assert out["value_aud_m"].iloc[0] == 13977.0 + 21932.0 + 14763.0 + 50831.0
    assert tuple(out["contributing_banks"].iloc[0]) == ("anz", "cba", "nab", "wbc")
    assert out["canonical_bucket"].iloc[0] == "manufacturing"


def test_inter_bank_drops_consumer_when_no_view_set() -> None:
    """Default consumer_view=None → consumer rows excluded."""
    rows = [
        _make_canonical_row("nab", "Manufacturing", "exposure_aud_m", 100.0,
                            as_of=date(2025, 9, 30)),
        _make_canonical_row("nab", "Personal", "exposure_aud_m", 200.0,
                            as_of=date(2025, 9, 30)),
    ]
    out = inter_bank_aggregate(_df(rows), metric="exposure_aud_m")
    assert "consumer_lending_personal" not in out["canonical_bucket"].values
    assert "manufacturing" in out["canonical_bucket"].values


def test_inter_bank_filters_to_most_recent_per_bank() -> None:
    """CBA reports Jun-25 and Dec-24; aggregator keeps only Jun-25."""
    rows = [
        _make_canonical_row("cba", "Manufacturing", "exposure_aud_m", 13977.0,
                            as_of=date(2025, 6, 30)),  # Jun-25
        _make_canonical_row("cba", "Manufacturing", "exposure_aud_m", 13447.0,
                            as_of=date(2024, 12, 31)),  # Dec-24 — should be dropped
    ]
    out = inter_bank_aggregate(_df(rows), metric="exposure_aud_m")
    assert out["value_aud_m"].iloc[0] == 13977.0
    assert out["as_of_date"].iloc[0] == date(2025, 6, 30)


def test_inter_bank_sums_wbc_geographies() -> None:
    """WBC publishes per-geography exposure rows; intra+inter aggregator
    must sum them all (Australia + NZ + Other Overseas)."""
    rows = [
        _make_canonical_row("wbc", "Construction", "exposure_aud_m", 11766.0,
                            as_of=date(2025, 9, 30), geography="Australia"),
        _make_canonical_row("wbc", "Construction", "exposure_aud_m", 882.0,
                            as_of=date(2025, 9, 30), geography="New Zealand"),
        _make_canonical_row("wbc", "Construction", "exposure_aud_m", 70.0,
                            as_of=date(2025, 9, 30), geography="Other overseas"),
    ]
    intra = intra_bank_industry_totals(_df(rows))
    out = inter_bank_aggregate(intra, metric="exposure_aud_m")
    assert out["value_aud_m"].iloc[0] == 11766.0 + 882.0 + 70.0


def test_aasb9_provisions_aggregate_when_basis_uniform() -> None:
    """NAB + WBC + ANZ all carry aasb9_stage3_ecl provisions; basis-
    matched aggregation should succeed."""
    rows = [
        _make_canonical_row("nab", "Manufacturing",
                            "individually_assessed_provision_aud_m", 239.0,
                            as_of=date(2025, 9, 30),
                            provision_basis="aasb9_stage3_ecl"),
        _make_canonical_row("wbc", "Manufacturing",
                            "individually_assessed_provision_aud_m", 132.0,
                            as_of=date(2025, 9, 30),
                            provision_basis="aasb9_stage3_ecl"),
        _make_canonical_row("anz", "Manufacturing",
                            "individually_assessed_provision_aud_m", 28.0,
                            as_of=date(2025, 9, 30),
                            provision_basis="aasb9_stage3_ecl"),
    ]
    out = aggregate_provisions_same_basis(
        _df(rows), provision_basis="aasb9_stage3_ecl",
    )
    assert len(out) == 1
    assert out["value_aud_m"].iloc[0] == 239.0 + 132.0 + 28.0


# ---------------------------------------------------------------------------
# 4. Write-off period-length filter
# ---------------------------------------------------------------------------

def test_writeoff_12_month_aggregate() -> None:
    """CBA full-year + WBC 12-month sum cleanly."""
    rows = [
        _make_canonical_row("cba", "Manufacturing", "write_offs_aud_m", 38.0,
                            as_of=date(2025, 6, 30), period_length_months=12),
        _make_canonical_row("wbc", "Manufacturing", "write_offs_aud_m", 10.0,
                            as_of=date(2025, 9, 30), period_length_months=12),
    ]
    out = inter_bank_aggregate(
        _df(rows), metric="write_offs_aud_m", period_length_months=12,
    )
    assert out["value_aud_m"].iloc[0] == 48.0
    assert out["aggregate_period_length_months"].iloc[0] == 12
    assert tuple(out["contributing_banks"].iloc[0]) == ("cba", "wbc")


def test_writeoff_6_month_only_cba() -> None:
    rows = [
        _make_canonical_row("cba", "Manufacturing", "write_offs_aud_m", 0.0,
                            as_of=date(2024, 12, 31), period_length_months=6),
    ]
    out = inter_bank_aggregate(
        _df(rows), metric="write_offs_aud_m", period_length_months=6,
    )
    assert out["value_aud_m"].iloc[0] == 0.0
    assert tuple(out["contributing_banks"].iloc[0]) == ("cba",)
    assert out["aggregate_period_length_months"].iloc[0] == 6


def test_writeoff_period_filter_excludes_mismatched_rows() -> None:
    """Asking for 12-month aggregation should ignore CBA half-year rows."""
    rows = [
        _make_canonical_row("cba", "Manufacturing", "write_offs_aud_m", 38.0,
                            as_of=date(2025, 6, 30), period_length_months=12),
        _make_canonical_row("cba", "Manufacturing", "write_offs_aud_m", 0.0,
                            as_of=date(2024, 12, 31), period_length_months=6),
    ]
    out = inter_bank_aggregate(
        _df(rows), metric="write_offs_aud_m", period_length_months=12,
    )
    # Only CBA full-year contributes.
    assert out["value_aud_m"].iloc[0] == 38.0


# ---------------------------------------------------------------------------
# 5. Consumer bucket separation
# ---------------------------------------------------------------------------

def test_consumer_personal_view_isolated() -> None:
    rows = [
        _make_canonical_row("nab", "Personal", "exposure_aud_m", 20790.0,
                            as_of=date(2025, 9, 30)),
        _make_canonical_row("anz", "Personal Lending", "exposure_aud_m", 20788.0,
                            as_of=date(2025, 9, 30)),
        _make_canonical_row("nab", "Residential mortgages", "exposure_aud_m", 496085.0,
                            as_of=date(2025, 9, 30)),
        _make_canonical_row("nab", "Manufacturing", "exposure_aud_m", 21932.0,
                            as_of=date(2025, 9, 30)),
    ]
    out = inter_bank_aggregate(
        _df(rows), metric="exposure_aud_m", consumer_view="personal",
    )
    assert len(out) == 1
    assert out["canonical_bucket"].iloc[0] == "consumer_lending_personal"
    assert out["value_aud_m"].iloc[0] == 20790.0 + 20788.0


def test_consumer_residential_mortgage_view_isolated() -> None:
    rows = [
        _make_canonical_row("nab", "Residential mortgages", "exposure_aud_m", 496085.0,
                            as_of=date(2025, 9, 30)),
        _make_canonical_row("anz", "Residential Mortgage", "exposure_aud_m", 554118.0,
                            as_of=date(2025, 9, 30)),
        _make_canonical_row("anz", "Personal Lending", "exposure_aud_m", 20788.0,
                            as_of=date(2025, 9, 30)),
    ]
    out = inter_bank_aggregate(
        _df(rows), metric="exposure_aud_m",
        consumer_view="residential_mortgage",
    )
    assert len(out) == 1
    assert out["canonical_bucket"].iloc[0] == "consumer_lending_residential_mortgage"
    assert out["value_aud_m"].iloc[0] == 496085.0 + 554118.0


def test_consumer_combined_view_isolated() -> None:
    rows = [
        _make_canonical_row("cba", "Consumer", "exposure_aud_m", 828841.0,
                            as_of=date(2025, 6, 30)),
        _make_canonical_row("wbc", "Retail lending", "exposure_aud_m", 599287.0,
                            as_of=date(2025, 9, 30)),
        _make_canonical_row("nab", "Personal", "exposure_aud_m", 20790.0,
                            as_of=date(2025, 9, 30)),
    ]
    out = inter_bank_aggregate(
        _df(rows), metric="exposure_aud_m", consumer_view="combined",
    )
    assert len(out) == 1
    assert out["canonical_bucket"].iloc[0] == "consumer_combined"
    assert out["value_aud_m"].iloc[0] == 828841.0 + 599287.0


def test_consumer_invalid_view_raises() -> None:
    rows = [
        _make_canonical_row("nab", "Personal", "exposure_aud_m", 100.0,
                            as_of=date(2025, 9, 30)),
    ]
    with pytest.raises(ValueError, match="consumer_view"):
        inter_bank_aggregate(
            _df(rows), metric="exposure_aud_m", consumer_view="all",
        )


# ---------------------------------------------------------------------------
# 6. As-of-date manifest — no synthetic dates
# ---------------------------------------------------------------------------

def test_manifest_carries_per_bank_dates() -> None:
    rows = [
        _make_canonical_row("cba", "Manufacturing", "exposure_aud_m", 13977.0,
                            as_of=date(2025, 6, 30)),
        _make_canonical_row("nab", "Manufacturing", "exposure_aud_m", 21932.0,
                            as_of=date(2025, 9, 30)),
    ]
    out = inter_bank_aggregate(_df(rows), metric="exposure_aud_m")
    manifest = out["bank_as_of_dates"].iloc[0]
    assert manifest == {"cba": date(2025, 6, 30), "nab": date(2025, 9, 30)}
    assert out["aggregate_as_of_date_strategy"].iloc[0] == "most_recent_per_bank"
    assert out["as_of_date"].iloc[0] == date(2025, 9, 30)  # most recent


def test_no_synthetic_dates_introduced() -> None:
    """Aggregator must never emit a date that doesn't appear in inputs."""
    in_dates = {date(2025, 6, 30), date(2025, 9, 30), date(2024, 12, 31)}
    rows = [
        _make_canonical_row("cba", "Manufacturing", "exposure_aud_m", 100.0,
                            as_of=date(2025, 6, 30)),
        _make_canonical_row("cba", "Manufacturing", "exposure_aud_m", 95.0,
                            as_of=date(2024, 12, 31)),
        _make_canonical_row("nab", "Manufacturing", "exposure_aud_m", 200.0,
                            as_of=date(2025, 9, 30)),
    ]
    out = inter_bank_aggregate(_df(rows), metric="exposure_aud_m")
    assert set(out["as_of_date"]).issubset(in_dates)


# ---------------------------------------------------------------------------
# 7. Coverage ratios
# ---------------------------------------------------------------------------

def _make_d14_1_row(industry: str, value: float | None, *,
                    business_size: str = "small",
                    as_of: date = date(2026, 3, 31),
                    series_break_flag: str = "from_jun2024",
                    redaction_reason: str | None = None) -> dict:
    return {
        "data_source": "rba_d14_1",
        "metric": "lending_stock_aud_m",
        "as_of_date": as_of,
        "business_size": business_size,
        "industry_published": industry,
        "value_aud_m": value,
        "series_id": "synth",
        "series_break_flag": series_break_flag,
        "redaction_reason": redaction_reason,
        "synthesised": False,
        "source_publication_date": date(2026, 4, 9),
    }


def test_coverage_ratio_low_flag_fires_below_threshold() -> None:
    """Big-4 EAD = 30, D14.1 = 100 → ratio 0.30 → low_coverage_flag."""
    big4 = pd.DataFrame([{
        "canonical_bucket": "manufacturing",
        "metric": "exposure_aud_m",
        "value_aud_m": 30.0,
        "contributing_banks": ("nab",),
        "bank_as_of_dates": {"nab": date(2025, 9, 30)},
        "aggregate_as_of_date_strategy": "most_recent_per_bank",
        "as_of_date": date(2025, 9, 30),
    }])
    d14 = pd.DataFrame([_make_d14_1_row("Manufacturing", 100.0)])
    cov = compute_coverage_ratios(big4, d14)
    assert cov["coverage_ratio"].iloc[0] == 0.30
    assert bool(cov["low_coverage_flag"].iloc[0]) is True
    assert "ead_vs_outstanding_soft_proxy" in cov["coverage_ratio_caveat"].iloc[0]
    assert "d14_1_lender_scope" in cov["coverage_ratio_caveat"].iloc[0]


def test_coverage_ratio_high_no_low_flag() -> None:
    """Ratio above the 60% threshold → low_coverage_flag is False."""
    big4 = pd.DataFrame([{
        "canonical_bucket": "manufacturing",
        "metric": "exposure_aud_m",
        "value_aud_m": 80.0,
        "contributing_banks": ("nab",),
        "bank_as_of_dates": {"nab": date(2025, 9, 30)},
        "aggregate_as_of_date_strategy": "most_recent_per_bank",
        "as_of_date": date(2025, 9, 30),
    }])
    d14 = pd.DataFrame([_make_d14_1_row("Manufacturing", 100.0)])
    cov = compute_coverage_ratios(big4, d14)
    assert cov["coverage_ratio"].iloc[0] == 0.80
    assert bool(cov["low_coverage_flag"].iloc[0]) is False


def test_coverage_d14_suppression_propagates_caveat() -> None:
    """If D14.1 cell is suppressed (null), coverage is null and the
    suppressed caveat appears."""
    big4 = pd.DataFrame([{
        "canonical_bucket": "manufacturing",
        "metric": "exposure_aud_m",
        "value_aud_m": 50.0,
        "contributing_banks": ("nab",),
        "bank_as_of_dates": {"nab": date(2025, 9, 30)},
        "aggregate_as_of_date_strategy": "most_recent_per_bank",
        "as_of_date": date(2025, 9, 30),
    }])
    d14 = pd.DataFrame([_make_d14_1_row(
        "Manufacturing", None,
        redaction_reason="APRA_publication_suppressed",
    )])
    cov = compute_coverage_ratios(big4, d14)
    assert pd.isna(cov["coverage_ratio"].iloc[0])
    assert "d14_1_suppressed" in cov["coverage_ratio_caveat"].iloc[0]


def test_coverage_threshold_exact_value() -> None:
    """At exactly the threshold (60%), low flag should not fire."""
    assert LOW_COVERAGE_THRESHOLD == 0.60


# ---------------------------------------------------------------------------
# 8. APS 120 caveat propagation
# ---------------------------------------------------------------------------

def test_aps120_caveat_appended_to_npe_rows_with_cba() -> None:
    rows = pd.DataFrame([
        {"metric": "npe_aud_m", "contributing_banks": ("cba", "nab"),
         "coverage_ratio_caveat": "ead_vs_outstanding_soft_proxy"},
        {"metric": "npe_aud_m", "contributing_banks": ("nab",),
         "coverage_ratio_caveat": "ead_vs_outstanding_soft_proxy"},
        {"metric": "exposure_aud_m", "contributing_banks": ("cba",),
         "coverage_ratio_caveat": "ead_vs_outstanding_soft_proxy"},
    ])
    out = flag_aps120_caveat_for_npe_aggregates(rows)
    assert "cba_aps120_scope_mismatch" in out.iloc[0]["coverage_ratio_caveat"]
    # Row without CBA: no caveat added
    assert "cba_aps120_scope_mismatch" not in out.iloc[1]["coverage_ratio_caveat"]
    # EAD-based row: no caveat even with CBA present
    assert "cba_aps120_scope_mismatch" not in out.iloc[2]["coverage_ratio_caveat"]


# ---------------------------------------------------------------------------
# 9. Real-data integration
# ---------------------------------------------------------------------------

PDF_DIR = Path(__file__).resolve().parents[3] / "data" / "raw" / "pillar3"


@pytest.mark.skipif(
    not all((PDF_DIR / f).exists() for f in [
        "CBA_FY2025_Pillar3_Annual.pdf", "NAB_FY2025_Pillar3_Annual.pdf",
        "WBC_FY2025_Pillar3_Annual.pdf", "ANZ_FY2025_Pillar3_Annual.pdf",
    ]),
    reason="Real-data fixtures not all present",
)
def test_full_pipeline_real_data_with_golden_aggregates() -> None:
    from ingestion.adapters.cba_pillar3_industry import extract_cba_industry_rows
    from ingestion.adapters.nab_pillar3_industry import extract_nab_industry_rows
    from ingestion.adapters.wbc_pillar3_industry import extract_wbc_industry_rows
    from ingestion.adapters.anz_pillar3_industry import extract_anz_industry_rows

    all_per_bank = pd.concat([
        intra_bank_industry_totals(extract_cba_industry_rows(PDF_DIR / "CBA_FY2025_Pillar3_Annual.pdf")),
        intra_bank_industry_totals(extract_nab_industry_rows(PDF_DIR / "NAB_FY2025_Pillar3_Annual.pdf")),
        intra_bank_industry_totals(extract_wbc_industry_rows(PDF_DIR / "WBC_FY2025_Pillar3_Annual.pdf")),
        intra_bank_industry_totals(extract_anz_industry_rows(PDF_DIR / "ANZ_FY2025_Pillar3_Annual.pdf")),
    ], ignore_index=True)

    big4_ead = inter_bank_aggregate(all_per_bank, metric="exposure_aud_m")
    by_bucket = {r.canonical_bucket: r for r in big4_ead.itertuples()}

    # All four banks contribute to manufacturing (no consumer split there).
    assert tuple(by_bucket["manufacturing"].contributing_banks) == ("anz", "cba", "nab", "wbc")

    # Pinned aggregate values from the FY2025 PDFs.
    # CBA Manufacturing Jun-25 column total (sum of 10 portfolios in
    # CRB(e)(ii)) = 16717, per the published "Total credit exposures"
    # row on p36. WBC publishes per-geography exposure (Aus 14763 +
    # NZ 2718 + Other Overseas 2521 = 20002). ANZ publishes 3
    # gross-carrying-component splits (loans 26053 + off_bs 23205 +
    # other 1573 = 50831). NAB single value = 21932.
    assert by_bucket["manufacturing"].value_aud_m == (
        16_717.0 + 21_932.0 + 20_002.0 + 50_831.0
    )

    # Construction = CBA(11623) + NAB(15140) + WBC(11766+882+70) + ANZ(6508+6657+46)
    assert by_bucket["construction"].value_aud_m == 11_623.0 + 15_140.0 + 12_718.0 + 13_211.0

    # Trade (wholesale + retail combined; ANZ split → summed)
    # ANZ Trade = Retail Trade(11480+6418+71) + Wholesale Trade(12706+11439+1107)
    #           = 17969 + 25252
    # CBA = 29785, NAB = 36531, WBC = 22042+4403+2693 = 29138
    assert by_bucket["trade"].value_aud_m == (
        29_785.0 + 36_531.0 + 29_138.0 + 17_969.0 + 25_252.0
    )

    # Provisions aggregable across NAB+WBC+ANZ on AASB 9 basis.
    aasb9 = aggregate_provisions_same_basis(
        all_per_bank[all_per_bank["bank_code"].isin({"nab", "wbc", "anz"})],
        provision_basis="aasb9_stage3_ecl",
    )
    assert not aasb9.empty
    assert "manufacturing" in aasb9["canonical_bucket"].values


@pytest.mark.skipif(
    not all((PDF_DIR / f).exists() for f in [
        "CBA_FY2025_Pillar3_Annual.pdf", "NAB_FY2025_Pillar3_Annual.pdf",
        "WBC_FY2025_Pillar3_Annual.pdf", "ANZ_FY2025_Pillar3_Annual.pdf",
    ]),
    reason="Real-data fixtures not all present",
)
def test_writeoff_aggregate_real_data_12_month_only_cba_and_wbc() -> None:
    from ingestion.adapters.cba_pillar3_industry import extract_cba_industry_rows
    from ingestion.adapters.nab_pillar3_industry import extract_nab_industry_rows
    from ingestion.adapters.wbc_pillar3_industry import extract_wbc_industry_rows
    from ingestion.adapters.anz_pillar3_industry import extract_anz_industry_rows

    all_per_bank = pd.concat([
        intra_bank_industry_totals(extract_cba_industry_rows(PDF_DIR / "CBA_FY2025_Pillar3_Annual.pdf")),
        intra_bank_industry_totals(extract_nab_industry_rows(PDF_DIR / "NAB_FY2025_Pillar3_Annual.pdf")),
        intra_bank_industry_totals(extract_wbc_industry_rows(PDF_DIR / "WBC_FY2025_Pillar3_Annual.pdf")),
        intra_bank_industry_totals(extract_anz_industry_rows(PDF_DIR / "ANZ_FY2025_Pillar3_Annual.pdf")),
    ], ignore_index=True)
    wo = inter_bank_aggregate(
        all_per_bank, metric="write_offs_aud_m", period_length_months=12,
    )
    # Only CBA + WBC publish write-offs by industry.
    contributing = set(wo["contributing_banks"].iloc[0])
    assert contributing.issubset({"cba", "wbc"})
    # Half-year aggregate: only CBA contributes.
    wo6 = inter_bank_aggregate(
        all_per_bank, metric="write_offs_aud_m", period_length_months=6,
    )
    assert tuple(wo6["contributing_banks"].iloc[0]) == ("cba",)


# ---------------------------------------------------------------------------
# 10. Cross-bank invariant — every bucket routes to a known canonical key
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not all((PDF_DIR / f).exists() for f in [
        "CBA_FY2025_Pillar3_Annual.pdf", "NAB_FY2025_Pillar3_Annual.pdf",
        "WBC_FY2025_Pillar3_Annual.pdf", "ANZ_FY2025_Pillar3_Annual.pdf",
    ]),
    reason="Real-data fixtures not all present",
)
def test_cross_bank_invariant_after_aggregation_holds() -> None:
    from ingestion.adapters.cba_pillar3_industry import extract_cba_industry_rows
    from ingestion.adapters.nab_pillar3_industry import extract_nab_industry_rows
    from ingestion.adapters.wbc_pillar3_industry import extract_wbc_industry_rows
    from ingestion.adapters.anz_pillar3_industry import extract_anz_industry_rows
    from ingestion.adapters.anzsic_harmonisation import canonical_buckets

    all_per_bank = pd.concat([
        intra_bank_industry_totals(extract_cba_industry_rows(PDF_DIR / "CBA_FY2025_Pillar3_Annual.pdf")),
        intra_bank_industry_totals(extract_nab_industry_rows(PDF_DIR / "NAB_FY2025_Pillar3_Annual.pdf")),
        intra_bank_industry_totals(extract_wbc_industry_rows(PDF_DIR / "WBC_FY2025_Pillar3_Annual.pdf")),
        intra_bank_industry_totals(extract_anz_industry_rows(PDF_DIR / "ANZ_FY2025_Pillar3_Annual.pdf")),
    ], ignore_index=True)
    out = inter_bank_aggregate(all_per_bank, metric="exposure_aud_m")
    known = set(canonical_buckets().keys())
    for canon in out["canonical_bucket"]:
        assert canon in known


# ---------------------------------------------------------------------------
# 9. Refusal-event log (Phase 3.D recovery Step 5.1 / §B.5)
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=False)
def _refusal_log_fixture():
    """Opt-in fixture: reset before, reset after, isolate from other tests."""
    reset_refusal_log()
    yield
    reset_refusal_log()


def test_refusal_log_pd_aggregation_emits_event(_refusal_log_fixture) -> None:
    df = _df([
        _make_canonical_row("nab", "Manufacturing", "pd_value", 0.012,
                            as_of=date(2025, 9, 30)),
    ])
    with pytest.raises(CrossBankAggregationRefusedError):
        inter_bank_aggregate(df, metric="pd_value")
    log = get_refusal_log()
    assert len(log) == 1
    event = log[0]
    assert isinstance(event, RefusalEvent)
    assert event.refusal_type == "CrossBankAggregationRefusedError"
    assert "pd_value" in event.requested_aggregation
    assert "Phase 2" in event.phase_ruling_reference
    assert event.rule_violated  # populated
    assert event.as_of_date  # populated


def test_refusal_log_provision_basis_mismatch_emits_event(_refusal_log_fixture) -> None:
    df = _df([
        _make_canonical_row("cba", "Manufacturing",
                            "individually_assessed_provision_aud_m", 100.0,
                            as_of=date(2025, 6, 30),
                            provision_basis="aps220_specific"),
        _make_canonical_row("nab", "Manufacturing",
                            "individually_assessed_provision_aud_m", 200.0,
                            as_of=date(2025, 9, 30),
                            provision_basis="aasb9_stage3_ecl"),
    ])
    with pytest.raises(CrossBankAggregationRefusedError):
        aggregate_provisions_same_basis(df, provision_basis="aasb9_stage3_ecl")
    log = get_refusal_log()
    assert len(log) == 1
    assert log[0].refusal_type == "CrossBankAggregationRefusedError"
    assert "Phase 3.B.3" in log[0].phase_ruling_reference
    assert "provision_basis" in log[0].rule_violated


def test_refusal_log_period_length_mismatch_emits_event(_refusal_log_fixture) -> None:
    df = _df([
        _make_canonical_row("cba", "Manufacturing", "write_offs_aud_m", 38.0,
                            as_of=date(2025, 6, 30), period_length_months=12),
    ])
    with pytest.raises(IncompatiblePeriodLengthError):
        inter_bank_aggregate(df, metric="write_offs_aud_m")
    log = get_refusal_log()
    assert len(log) == 1
    assert log[0].refusal_type == "IncompatiblePeriodLengthError"
    assert "Phase 2 Issue 4" in log[0].phase_ruling_reference


def test_refusal_log_missing_provenance_emits_event(_refusal_log_fixture) -> None:
    bad = pd.DataFrame([{"metric": "exposure_aud_m", "value_aud_m": 100.0}])
    with pytest.raises(MissingProvenanceError):
        intra_bank_industry_totals(bad)
    log = get_refusal_log()
    assert len(log) == 1
    assert log[0].refusal_type == "MissingProvenanceError"
    assert "aggregator contract" in log[0].phase_ruling_reference.lower()


def test_refusal_log_total_consumer_is_refusal_by_omission(_refusal_log_fixture) -> None:
    """The total-consumer refusal is structural: no API path produces a
    single combined consumer aggregate, so no runtime event is emitted.
    Verify the absence of such an API rather than catching an event."""
    import ingestion.aggregation.pillar3_big4_aggregator as agg_mod
    public_names = [n for n in dir(agg_mod) if not n.startswith("_")]
    forbidden = ("aggregate_total_consumer", "consumer_total", "big4_total_consumer")
    for name in forbidden:
        assert name not in public_names, (
            f"{name} should not exist — total-consumer view is refused "
            f"by architecture (Phase 3.B.3 Q4)"
        )
    # consumer_view='combined' isolates one bucket only; it does not sum
    # the three. Verify by checking that combined output never carries
    # the other two buckets.
    df_personal = _df([
        _make_canonical_row("nab", "Personal", "exposure_aud_m", 10.0,
                            as_of=date(2025, 9, 30)),
    ])
    df_combined = _df([
        _make_canonical_row("cba", "Consumer", "exposure_aud_m", 50.0,
                            as_of=date(2025, 6, 30)),
    ])
    # Either consumer_view yields a single-bucket frame; never a combined sum.
    out_personal = inter_bank_aggregate(df_personal, metric="exposure_aud_m",
                                        consumer_view="personal")
    out_combined = inter_bank_aggregate(df_combined, metric="exposure_aud_m",
                                        consumer_view="combined")
    assert set(out_personal["canonical_bucket"]) <= {"consumer_lending_personal"}
    assert set(out_combined["canonical_bucket"]) <= {"consumer_combined"}


def test_refusal_log_chronological_order(_refusal_log_fixture) -> None:
    """Multiple refusals append in call order."""
    df_pd = _df([_make_canonical_row("nab", "Manufacturing", "pd_value",
                                     0.01, as_of=date(2025, 9, 30))])
    df_wo = _df([_make_canonical_row("cba", "Manufacturing", "write_offs_aud_m",
                                     38.0, as_of=date(2025, 6, 30),
                                     period_length_months=12)])
    with pytest.raises(CrossBankAggregationRefusedError):
        inter_bank_aggregate(df_pd, metric="pd_value")
    with pytest.raises(IncompatiblePeriodLengthError):
        inter_bank_aggregate(df_wo, metric="write_offs_aud_m")
    log = get_refusal_log()
    assert len(log) == 2
    assert log[0].refusal_type == "CrossBankAggregationRefusedError"
    assert log[1].refusal_type == "IncompatiblePeriodLengthError"


def test_refusal_log_reset_clears(_refusal_log_fixture) -> None:
    df = _df([_make_canonical_row("nab", "Manufacturing", "pd_value",
                                  0.01, as_of=date(2025, 9, 30))])
    with pytest.raises(CrossBankAggregationRefusedError):
        inter_bank_aggregate(df, metric="pd_value")
    assert len(get_refusal_log()) == 1
    reset_refusal_log()
    assert get_refusal_log() == ()
