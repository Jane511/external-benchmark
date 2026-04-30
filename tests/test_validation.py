"""Tests for src.validation — cross-source validation flags (no averaging)."""
from __future__ import annotations

from datetime import date, timedelta

import pytest

from src.models import DataDefinitionClass, RawObservation, SourceType
from src.validation import compute_validation_flags, PEER_RATIO_DEFINITION  # noqa: F401


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
        data_definition_class=DataDefinitionClass.BASEL_PD_ONE_YEAR,
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


# ---------------------------------------------------------------------------
# P1.1: commentary rows (value=None) must not leak into spread/median/outlier
# ---------------------------------------------------------------------------

def _commentary_obs(source_id: str, *, today: date = date(2026, 4, 27)):
    return RawObservation(
        source_id=source_id,
        source_type=SourceType.NON_BANK_LISTED,
        segment="commercial_property",
        product=None,
        parameter="commentary",
        data_definition_class=DataDefinitionClass.QUALITATIVE_COMMENTARY,
        value=None,
        as_of_date=today,
        reporting_basis="Half-yearly results commentary",
        methodology_note="QUALITATIVE: office sector under pressure",
    )


def test_commentary_rows_excluded_from_spread() -> None:
    """Two commentary rows with value=None must NOT poison spread/median."""
    obs = [
        _obs("cba", 0.025),
        _obs("nab", 0.022),
        _obs("anz", 0.026),
        _obs("wbc", 0.021),
        _commentary_obs("QUALITAS_CRE_COMMENTARY"),
        _commentary_obs("METRICS_CRE_COMMENTARY"),
    ]
    flags = compute_validation_flags(obs, today=date(2026, 4, 27))
    # Spread is (0.026 - 0.021) / 0.0235 ≈ 0.213, NOT 1+ from value=0 leak.
    assert flags.spread_pct is not None
    assert 0.20 < flags.spread_pct < 0.25
    # Commentary rows are not flagged as outliers.
    assert "QUALITAS_CRE_COMMENTARY" not in flags.outlier_sources
    assert "METRICS_CRE_COMMENTARY" not in flags.outlier_sources


# ---------------------------------------------------------------------------
# P2.1: regulatory floors and rating-agency indices excluded from outliers
# ---------------------------------------------------------------------------

def test_regulatory_floors_not_outliers() -> None:
    """APS 113 slotting / floor rows must NOT trip outlier detection."""
    obs = [
        _obs("cba", 0.025),
        _obs("nab", 0.022),
        _obs("anz", 0.026),
        _obs("wbc", 0.021),
        # Slotting Strong=0.4%, Weak=8% — would be 'outliers' under
        # the old all-rows logic, but they're not peers.
        RawObservation(
            source_id="APS113_SLOTTING_STRONG_PD",
            source_type=SourceType.APRA_PERFORMANCE,
            segment="commercial_property",
            product=None, parameter="pd",
            data_definition_class=DataDefinitionClass.REGULATORY_FLOOR_PD,
            value=0.004, as_of_date=date(2026, 1, 31),
            reporting_basis="APRA APS 113",
            methodology_note="Specialised lending Strong",
        ),
        RawObservation(
            source_id="APS113_SLOTTING_WEAK_PD",
            source_type=SourceType.APRA_PERFORMANCE,
            segment="commercial_property",
            product=None, parameter="pd",
            data_definition_class=DataDefinitionClass.REGULATORY_FLOOR_PD,
            value=0.080, as_of_date=date(2026, 1, 31),
            reporting_basis="APRA APS 113",
            methodology_note="Specialised lending Weak",
        ),
    ]
    flags = compute_validation_flags(obs, today=date(2026, 4, 27))
    assert "APS113_SLOTTING_STRONG_PD" not in flags.outlier_sources
    assert "APS113_SLOTTING_WEAK_PD" not in flags.outlier_sources
    # Both should appear as reference anchors instead.
    anchor_ids = {a["source_id"] for a in flags.reference_anchors}
    assert "APS113_SLOTTING_STRONG_PD" in anchor_ids
    assert "APS113_SLOTTING_WEAK_PD" in anchor_ids


# ---------------------------------------------------------------------------
# P2.1: peer ratio excludes regulators and Macquarie
# ---------------------------------------------------------------------------

def test_peer_ratio_excludes_macquarie_and_regulators() -> None:
    """Macquarie + APRA aggregates must NOT enter the Big4-vs-non-bank ratio."""
    obs = [
        _obs("cba", 0.020),
        _obs("nab", 0.024),
        _obs("judo", 0.060, source_type=SourceType.NON_BANK_LISTED),
        _obs("liberty", 0.080, source_type=SourceType.NON_BANK_LISTED),
        # MQG must NOT count toward Big 4.
        _obs("mqg", 0.010, source_type=SourceType.BANK_PILLAR3),
        # APRA aggregate must not count toward non-bank.
        _obs(
            "APRA_QPEX_CRE", 0.012,
            source_type=SourceType.APRA_PERFORMANCE,
        ),
    ]
    flags = compute_validation_flags(obs, today=date(2026, 4, 27))
    # Peer Big 4 median = median(0.020, 0.024) = 0.022.
    # Peer non-bank median = median(0.060, 0.080) = 0.070.
    # Ratio = 0.070 / 0.022 ≈ 3.18.
    assert flags.peer_big4_vs_non_bank_ratio == pytest.approx(0.070 / 0.022, rel=0.01)
    # MQG and APRA appear under reference_anchors.
    anchor_ids = {a["source_id"] for a in flags.reference_anchors}
    assert "mqg" in anchor_ids
    assert "APRA_QPEX_CRE" in anchor_ids


# ---------------------------------------------------------------------------
# P1.4: stale logic respects refresh_schedules + frozen-dataset banner
# ---------------------------------------------------------------------------

def test_stale_only_when_newer_release_known() -> None:
    """When refresh pipeline is quiet, stale list is suppressed and a
    single banner string is emitted."""
    obs = [
        _obs("cba", 0.025, days_old=400),  # would be stale at 210d
    ]
    flags = compute_validation_flags(
        obs, today=date(2026, 4, 27),
        refresh_schedules={"bank_pillar3": 210},
        refresh_pipeline_quiet=True,
    )
    assert flags.stale_sources == []
    assert flags.frozen_dataset_banner is not None
    assert "Refresh pipeline" in flags.frozen_dataset_banner


def test_stale_uses_per_source_type_cadence() -> None:
    """Pillar 3 cadence (120d) should fire on a 200-day-old row, but the
    legacy default (210d) wouldn't."""
    obs = [
        _obs("cba", 0.025, days_old=200, source_type=SourceType.BANK_PILLAR3),
        _obs("rba_aggregate", 0.014, days_old=200, source_type=SourceType.RBA_AGGREGATE),
    ]
    flags = compute_validation_flags(
        obs, today=date(2026, 4, 27),
        refresh_schedules={"bank_pillar3": 120, "rba_aggregate": 210},
    )
    assert "cba" in flags.stale_sources
    assert "rba_aggregate" not in flags.stale_sources


def test_stale_sources_are_deduplicated() -> None:
    """When a source repeats across vintages, stale_sources must list it once."""
    obs = [
        _obs("cba", 0.025, days_old=400),
        _obs("cba", 0.024, days_old=580),
        _obs("cba", 0.022, days_old=760),
    ]
    flags = compute_validation_flags(
        obs, today=date(2026, 4, 27),
        refresh_schedules={"bank_pillar3": 210},
    )
    assert flags.stale_sources == ["cba"]


# ---------------------------------------------------------------------------
# P2.1: PEER_RATIO_DEFINITION constant single-source-of-truth
# ---------------------------------------------------------------------------

def test_peer_ratio_definition_is_documented() -> None:
    from src.validation import PEER_RATIO_DEFINITION
    assert "peer_big4_vs_non_bank_ratio" in PEER_RATIO_DEFINITION
    assert "median(peer_non_bank values)" in PEER_RATIO_DEFINITION
    assert "median(peer_big4 values)" in PEER_RATIO_DEFINITION
