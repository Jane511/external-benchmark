"""Tests for src.validation — cross-source validation flags (no averaging)."""
from __future__ import annotations

from datetime import date, timedelta

import pytest

from src.models import RawObservation, SourceType
from src.validation import compute_validation_flags


def _obs(
    source_id: str, value: float, *,
    days_old: int = 0,
    source_type: SourceType = SourceType.BANK_PILLAR3,
    segment: str = "commercial_property",
    today: date = date(2026, 4, 27),
) -> RawObservation:
    return RawObservation(
        source_id=source_id,
        source_type=source_type,
        segment=segment,
        product=None,
        parameter="pd",
        value=value,
        as_of_date=today - timedelta(days=days_old),
        reporting_basis="Pillar 3 quarterly",
        methodology_note="Average PD from CR6 EAD-weighted",
    )


def test_empty_observations_returns_zero_flags():
    flags = compute_validation_flags([], today=date(2026, 4, 27))
    assert flags.n_sources == 0
    assert flags.spread_pct is None
    assert flags.outlier_sources == []
    assert flags.bank_vs_nonbank_ratio is None


def test_spread_computed_from_max_min_over_median():
    obs = [_obs("cba", 0.020), _obs("nab", 0.025), _obs("wbc", 0.030)]
    flags = compute_validation_flags(obs, today=date(2026, 4, 27))
    assert flags.n_sources == 3
    # (0.030 - 0.020) / 0.025 = 0.40
    assert flags.spread_pct == pytest.approx(0.40)


def test_outlier_threshold_flags_sources_more_than_2x_median():
    obs = [
        _obs("cba", 0.020),
        _obs("nab", 0.025),
        _obs("wbc", 0.030),
        _obs("anz", 0.090),  # > 2 × 0.025 = 0.050
    ]
    flags = compute_validation_flags(obs, today=date(2026, 4, 27))
    assert "anz" in flags.outlier_sources


def test_stale_sources_flagged_after_threshold():
    obs = [
        _obs("cba", 0.025, days_old=30),
        _obs("judo", 0.040, days_old=400, source_type=SourceType.NON_BANK_LISTED),
    ]
    flags = compute_validation_flags(
        obs, today=date(2026, 4, 27), staleness_days=180,
    )
    assert flags.stale_sources == ["judo"]


def test_bank_vs_nonbank_ratio_only_when_both_present():
    obs = [
        _obs("cba", 0.025),
        _obs("nab", 0.030),
        _obs("judo", 0.050, source_type=SourceType.NON_BANK_LISTED),
        _obs("liberty", 0.060, source_type=SourceType.NON_BANK_LISTED),
    ]
    flags = compute_validation_flags(obs, today=date(2026, 4, 27))
    # big4 median = 0.0275, nonbank median = 0.055, ratio = 2.0
    assert flags.bank_vs_nonbank_ratio == pytest.approx(2.0)


def test_no_consensus_value_in_flags():
    """Critical guarantee: ValidationFlags has no 'benchmark_value' field."""
    obs = [_obs("cba", 0.025), _obs("nab", 0.030)]
    flags = compute_validation_flags(obs, today=date(2026, 4, 27))
    assert not hasattr(flags, "benchmark_value")
    assert not hasattr(flags, "consensus")
