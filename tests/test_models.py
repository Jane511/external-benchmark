"""Tests for src/models.py — BenchmarkEntry validators.

Target: 12 tests covering every branch of the component-aware value
validator plus the date-ordering rule.
"""
from __future__ import annotations

from datetime import date
from typing import Any

import pytest
from pydantic import ValidationError

from datetime import datetime

from src.models import (
    AdjustmentResult,
    AdjustmentStep,
    BenchmarkEntry,
    CentralTendencyOutput,
    Component,
    Condition,
    DataType,
    DownturnResult,
    GovernanceReport,
    InstitutionType,
    QualityScore,
    SourceType,
    TriangulationResult,
)


def _valid_kwargs(**overrides: Any) -> dict[str, Any]:
    """Return a kwargs dict for a minimally valid BenchmarkEntry.

    Defaults describe a Pillar 3 residential-mortgage PD; override any
    subset to flex the specific validator branch under test.
    """
    base: dict[str, Any] = {
        "source_id": "CBA_PILLAR3_RES_2024H2",
        "publisher": "Commonwealth Bank of Australia",
        "source_type": SourceType.PILLAR3,
        "data_type": DataType.PD,
        "asset_class": "residential_mortgage",
        "value": 0.0072,
        "value_date": date(2024, 12, 31),
        "period_years": 5,
        "geography": "AU",
        "url": "https://www.commbank.com.au/pillar3",
        "retrieval_date": date(2025, 3, 1),
        "quality_score": QualityScore.HIGH,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Aggregate (component=None) value-bound rules
# ---------------------------------------------------------------------------

def test_pd_in_bounds_accepted() -> None:
    entry = BenchmarkEntry(**_valid_kwargs(data_type=DataType.PD, value=0.025))
    assert entry.value == 0.025


def test_pd_above_one_rejected() -> None:
    with pytest.raises(ValidationError, match=r"value=1.5 must be in \[0, 1\]"):
        BenchmarkEntry(**_valid_kwargs(data_type=DataType.PD, value=1.5))


def test_lgd_negative_rejected() -> None:
    with pytest.raises(ValidationError, match=r"must be in \[0, 1\]"):
        BenchmarkEntry(**_valid_kwargs(data_type=DataType.LGD, value=-0.01))


# ---------------------------------------------------------------------------
# Component-specific value rules
# ---------------------------------------------------------------------------

def test_haircut_in_bounds_accepted() -> None:
    entry = BenchmarkEntry(
        **_valid_kwargs(
            data_type=DataType.LGD,
            component=Component.HAIRCUT,
            condition=Condition.DOWNTURN,
            value=0.25,
        )
    )
    assert entry.component == Component.HAIRCUT
    assert entry.value == 0.25


def test_haircut_above_one_rejected() -> None:
    with pytest.raises(ValidationError, match=r"component='haircut'"):
        BenchmarkEntry(
            **_valid_kwargs(
                data_type=DataType.LGD,
                component=Component.HAIRCUT,
                value=1.5,
            )
        )


def test_workout_costs_in_bounds_accepted() -> None:
    entry = BenchmarkEntry(
        **_valid_kwargs(
            data_type=DataType.LGD,
            component=Component.WORKOUT_COSTS,
            value=0.05,
        )
    )
    assert entry.value == 0.05


def test_time_to_recovery_positive_accepted() -> None:
    entry = BenchmarkEntry(
        **_valid_kwargs(
            data_type=DataType.LGD,
            component=Component.TIME_TO_RECOVERY,
            value=12,
        )
    )
    assert entry.value == 12.0


def test_time_to_recovery_zero_rejected() -> None:
    with pytest.raises(ValidationError, match=r"must be > 0"):
        BenchmarkEntry(
            **_valid_kwargs(
                data_type=DataType.LGD,
                component=Component.TIME_TO_RECOVERY,
                value=0,
            )
        )


def test_discount_rate_positive_accepted() -> None:
    entry = BenchmarkEntry(
        **_valid_kwargs(
            data_type=DataType.LGD,
            component=Component.DISCOUNT_RATE,
            value=0.12,
        )
    )
    assert entry.value == 0.12


# ---------------------------------------------------------------------------
# Component-gating rule: only allowed for lgd / recovery_rate
# (explicitly called out — easy to miss)
# ---------------------------------------------------------------------------

def test_component_rejected_for_pd_data_type() -> None:
    """A component cannot be attached to a PD benchmark — only lgd / recovery_rate."""
    with pytest.raises(ValidationError, match=r"only allowed when data_type in"):
        BenchmarkEntry(
            **_valid_kwargs(
                data_type=DataType.PD,
                component=Component.HAIRCUT,
                value=0.20,
            )
        )


def test_component_allowed_for_recovery_rate() -> None:
    """recovery_rate is the second data_type that admits components."""
    entry = BenchmarkEntry(
        **_valid_kwargs(
            data_type=DataType.RECOVERY_RATE,
            component=Component.HAIRCUT,
            value=0.15,
        )
    )
    assert entry.data_type == DataType.RECOVERY_RATE
    assert entry.component == Component.HAIRCUT


# ---------------------------------------------------------------------------
# Date ordering
# ---------------------------------------------------------------------------

def test_value_date_after_retrieval_date_rejected() -> None:
    with pytest.raises(ValidationError, match=r"must be on or before"):
        BenchmarkEntry(
            **_valid_kwargs(
                value_date=date(2025, 6, 1),
                retrieval_date=date(2025, 3, 1),
            )
        )


# ===========================================================================
# Supporting models
# ===========================================================================

def test_adjustment_step_constructs() -> None:
    step = AdjustmentStep(
        name="selection_bias", multiplier=1.7, rationale="PC borrowers rejected by banks"
    )
    assert step.multiplier == 1.7
    assert step.rationale.startswith("PC")


def test_adjustment_result_scenario_label_defaults_to_none() -> None:
    """Persisted adjustments have scenario_label=None; what-if sets it explicitly."""
    step = AdjustmentStep(name="selection_bias", multiplier=1.7)
    result = AdjustmentResult(
        raw_value=0.025,
        adjusted_value=0.0425,
        institution_type=InstitutionType.PRIVATE_CREDIT,
        product="bridging_commercial",
        asset_class="commercial_property_investment",
        steps=[step],
        final_multiplier=1.7,
    )
    assert result.scenario_label is None


def test_adjustment_result_scenario_label_accepts_what_if() -> None:
    step = AdjustmentStep(name="selection_bias", multiplier=2.0)
    result = AdjustmentResult(
        raw_value=0.025,
        adjusted_value=0.05,
        institution_type=InstitutionType.PRIVATE_CREDIT,
        product="bridging_commercial",
        asset_class="commercial_property_investment",
        steps=[step],
        final_multiplier=2.0,
        scenario_label="what_if",
    )
    assert result.scenario_label == "what_if"


def test_triangulation_result_constructs() -> None:
    result = TriangulationResult(
        segment="residential_mortgage",
        benchmark_value=0.0083,
        confidence_n=300,
        source_count=4,
        method="weighted_by_years",
    )
    assert result.confidence_n == 300
    assert result.method == "weighted_by_years"


def test_triangulation_result_confidence_n_capped_at_500() -> None:
    """Anything above 500 is silently clamped — the cap lives in the model."""
    result = TriangulationResult(
        segment="residential_mortgage",
        benchmark_value=0.0083,
        confidence_n=750,
        source_count=4,
        method="weighted_by_years",
    )
    assert result.confidence_n == 500


def test_calibration_feed_central_tendency_constructs() -> None:
    out = CentralTendencyOutput(
        segment="residential_mortgage",
        external_lra=0.0083,
        floor_triggered=False,
    )
    assert out.method == "central_tendency"
    assert out.external_lra == 0.0083


def test_downturn_result_has_both_capital_and_ecl_fields() -> None:
    """lgd_for_capital (downturn) and lgd_for_ecl (long-run) are both present."""
    result = DownturnResult(
        long_run_lgd=0.22,
        uplift=1.45,
        downturn_lgd=0.319,
        product_type="residential_property",
        lgd_for_capital=0.319,
        lgd_for_ecl=0.22,
    )
    assert result.lgd_for_capital == 0.319
    assert result.lgd_for_ecl == 0.22
    assert result.lgd_for_capital != result.lgd_for_ecl  # regulatory purpose differs


def test_governance_report_constructs() -> None:
    report = GovernanceReport(
        report_type="stale_benchmarks",
        generated_at=datetime(2026, 4, 20, 12, 0, 0),
        institution_type=InstitutionType.BANK,
        findings=[{"source_id": "CBA_PILLAR3_RES_2024H2", "days_old": 150}],
        flags=["stale"],
    )
    assert report.report_type == "stale_benchmarks"
    assert report.institution_type == InstitutionType.BANK
    assert report.findings[0]["days_old"] == 150
