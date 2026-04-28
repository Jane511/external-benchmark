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
    BenchmarkEntry,
    Component,
    Condition,
    DataDefinitionClass,
    DataType,
    GovernanceReport,
    InstitutionType,
    QualityScore,
    RawObservation,
    SourceType,
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


# ===========================================================================
# RawObservation — DataDefinitionClass + parameter cross-validation
# ===========================================================================

def _valid_obs_kwargs(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "source_id": "cba",
        "source_type": SourceType.BANK_PILLAR3,
        "segment": "commercial_property",
        "parameter": "pd",
        "data_definition_class": DataDefinitionClass.BASEL_PD_ONE_YEAR,
        "value": 0.025,
        "as_of_date": date(2026, 3, 31),
        "reporting_basis": "Pillar 3 quarterly",
        "methodology_note": "Average PD CR6",
    }
    base.update(overrides)
    return base


def test_raw_observation_basel_pd_constructs() -> None:
    obs = RawObservation(**_valid_obs_kwargs())
    assert obs.parameter == "pd"
    assert obs.data_definition_class is DataDefinitionClass.BASEL_PD_ONE_YEAR


def test_raw_observation_arrears_definition_class_accepted() -> None:
    obs = RawObservation(
        **_valid_obs_kwargs(
            parameter="arrears",
            data_definition_class=DataDefinitionClass.ARREARS_30_PLUS_DAYS,
            value=0.0093,
        )
    )
    assert obs.parameter == "arrears"
    assert obs.data_definition_class is DataDefinitionClass.ARREARS_30_PLUS_DAYS


def test_raw_observation_impaired_npl_accepted() -> None:
    impaired = RawObservation(
        **_valid_obs_kwargs(
            parameter="impaired",
            data_definition_class=DataDefinitionClass.IMPAIRED_LOANS_RATIO,
            value=0.012,
        )
    )
    npl = RawObservation(
        **_valid_obs_kwargs(
            parameter="npl",
            data_definition_class=DataDefinitionClass.NPL_RATIO,
            value=0.013,
        )
    )
    assert impaired.parameter == "impaired"
    assert npl.parameter == "npl"


def test_raw_observation_qualitative_commentary_zero_value_allowed() -> None:
    """parameter='commentary' relaxes the [0,1] value check (value=0.0 by convention)."""
    obs = RawObservation(
        **_valid_obs_kwargs(
            parameter="commentary",
            data_definition_class=DataDefinitionClass.QUALITATIVE_COMMENTARY,
            value=0.0,
            methodology_note="QUALITATIVE: office sector under pressure",
        )
    )
    assert obs.value == 0.0


def test_raw_observation_unknown_parameter_rejected() -> None:
    with pytest.raises(ValidationError, match=r"parameter must be one of"):
        RawObservation(**_valid_obs_kwargs(parameter="unknown_metric"))


def test_raw_observation_definition_class_inconsistent_with_parameter_rejected() -> None:
    """parameter='pd' with data_definition_class=ARREARS_30_PLUS_DAYS must fail."""
    with pytest.raises(
        ValidationError,
        match=r"data_definition_class=.* is not valid for parameter='pd'",
    ):
        RawObservation(
            **_valid_obs_kwargs(
                parameter="pd",
                data_definition_class=DataDefinitionClass.ARREARS_30_PLUS_DAYS,
            )
        )


def test_raw_observation_loss_rate_accepts_two_definition_classes() -> None:
    """loss_rate covers both forward-looking expense and backward-looking realised."""
    fwd = RawObservation(
        **_valid_obs_kwargs(
            parameter="loss_rate",
            data_definition_class=DataDefinitionClass.LOSS_EXPENSE_RATE,
            value=0.0035,
        )
    )
    back = RawObservation(
        **_valid_obs_kwargs(
            parameter="loss_rate",
            data_definition_class=DataDefinitionClass.REALISED_LOSS_RATE,
            value=0.008,
        )
    )
    assert fwd.value == pytest.approx(0.0035)
    assert back.value == pytest.approx(0.008)


def test_raw_observation_lgd_with_regulatory_floor_accepted() -> None:
    """APS 113 LGD floor migrates as parameter='lgd' + REGULATORY_FLOOR_PD."""
    obs = RawObservation(
        **_valid_obs_kwargs(
            parameter="lgd",
            data_definition_class=DataDefinitionClass.REGULATORY_FLOOR_PD,
            value=0.075,
        )
    )
    assert obs.parameter == "lgd"
