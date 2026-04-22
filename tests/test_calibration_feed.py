"""Tests for src/calibration_feed.py — PD-only, regulatory floor, 5 methods."""
from __future__ import annotations

from datetime import date

import pytest

from src.adjustments import AdjustmentEngine
from src.calibration_feed import CalibrationFeed, _internal_weight_for_years
from src.db import create_engine_and_schema
from src.models import (
    BenchmarkEntry,
    DataType,
    InstitutionType,
    QualityScore,
    SourceType,
)
from src.registry import BenchmarkRegistry
from src.seed_data import load_seed_data
from src.triangulation import BenchmarkTriangulator


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def bank_feed():
    engine = create_engine_and_schema(":memory:")
    registry = BenchmarkRegistry(engine, actor="test")
    load_seed_data(registry)
    adjuster = AdjustmentEngine(InstitutionType.BANK, engine)
    tri = BenchmarkTriangulator(InstitutionType.BANK)
    return CalibrationFeed(registry, adjuster, tri), registry


def _pd_entry(**overrides) -> BenchmarkEntry:
    base = {
        "source_id": "X", "publisher": "P",
        "source_type": SourceType.PILLAR3, "data_type": DataType.PD,
        "asset_class": "residential_mortgage", "value": 0.008,
        "value_date": date(2024, 12, 31), "period_years": 5,
        "geography": "AU", "url": "https://example.com",
        "retrieval_date": date(2025, 3, 1),
        "quality_score": QualityScore.HIGH,
    }
    base.update(overrides)
    return BenchmarkEntry(**base)


# ---------------------------------------------------------------------------
# Five methods return correct shapes
# ---------------------------------------------------------------------------

def test_for_central_tendency_returns_shape(bank_feed) -> None:
    feed, _ = bank_feed
    out = feed.for_central_tendency("residential_mortgage")
    assert out.method == "central_tendency"
    assert out.segment == "residential_mortgage"
    assert out.external_lra > 0
    assert out.floor_triggered is False  # seed values are well above 0.0003


def test_for_logistic_recalibration_has_target_lra_and_confidence_n(bank_feed) -> None:
    feed, _ = bank_feed
    out = feed.for_logistic_recalibration("residential_mortgage")
    assert out.method == "logistic_recalibration"
    assert out.target_lra > 0
    assert out.confidence_n > 0


def test_for_bayesian_blending_returns_external_pd_and_n(bank_feed) -> None:
    feed, _ = bank_feed
    out = feed.for_bayesian_blending("residential_mortgage")
    assert out.method == "bayesian_blending"
    assert out.external_pd > 0
    assert 0 < out.confidence_n <= 500


def test_for_pluto_tasche_marks_role_comparison_only(bank_feed) -> None:
    feed, _ = bank_feed
    out = feed.for_pluto_tasche("residential_mortgage")
    assert out.method == "pluto_tasche"
    assert out.role == "comparison_only"
    assert out.external_pd > 0


# ---------------------------------------------------------------------------
# Regulatory floor (0.03%) — PD only
# ---------------------------------------------------------------------------

def test_pd_below_floor_is_clamped_and_flag_set(bank_feed) -> None:
    feed, registry = bank_feed
    registry.add(_pd_entry(
        source_id="TINY_TRADE_PD", asset_class="trade_finance",
        source_type=SourceType.ICC_TRADE, value=0.0001,
    ))
    # Replace the seed trade finance entries via a fresh low-PD segment
    out = feed.for_central_tendency("trade_finance")
    # Seed has ICC values 0.0003 / 0.00015 / 0.004. The segment average was
    # already below floor; confirm clamp + flag.
    assert out.external_lra == 0.0003
    assert out.floor_triggered is True


def test_pd_above_floor_returned_as_is(bank_feed) -> None:
    feed, _ = bank_feed
    out = feed.for_central_tendency("residential_mortgage")
    assert out.external_lra > 0.0003  # Big 4 avg ~0.008
    assert out.floor_triggered is False


# ---------------------------------------------------------------------------
# External blending weight schedule
# ---------------------------------------------------------------------------

def test_external_blending_weight_schedule(bank_feed) -> None:
    feed, _ = bank_feed
    cases = [
        (2, 0.30),
        (2.9, 0.30),
        (3, 0.50),
        (3.5, 0.50),
        (4, 0.70),
        (4.9, 0.70),
        (5, 0.90),
        (10, 0.90),
    ]
    for years, expected in cases:
        out = feed.for_external_blending("residential_mortgage", internal_years=years)
        assert out.internal_weight == expected, f"years={years} expected {expected}"


def test_internal_weight_helper_direct() -> None:
    assert _internal_weight_for_years(2) == 0.30
    assert _internal_weight_for_years(3) == 0.50
    assert _internal_weight_for_years(4) == 0.70
    assert _internal_weight_for_years(5) == 0.90


# ---------------------------------------------------------------------------
# PD-only gatekeeping — LGD segments rejected
# ---------------------------------------------------------------------------

def test_calibration_feed_rejects_segment_with_no_pd_entries() -> None:
    """LGD-only segment must not accidentally produce a PD calibration output."""
    engine = create_engine_and_schema(":memory:")
    registry = BenchmarkRegistry(engine, actor="test")
    # Seed only an LGD entry, no PDs
    registry.add(_pd_entry(
        source_id="LGD_ONLY", data_type=DataType.LGD,
        asset_class="lgd_only_segment", value=0.22,
    ))
    feed = CalibrationFeed(
        registry,
        AdjustmentEngine(InstitutionType.BANK, engine),
        BenchmarkTriangulator(InstitutionType.BANK),
    )
    with pytest.raises(ValueError, match="PD-only by design"):
        feed.for_central_tendency("lgd_only_segment")


# ---------------------------------------------------------------------------
# Custom floor override
# ---------------------------------------------------------------------------

def test_custom_regulatory_floor_can_be_tightened() -> None:
    engine = create_engine_and_schema(":memory:")
    registry = BenchmarkRegistry(engine, actor="test")
    registry.add(_pd_entry(value=0.001))  # above default floor, below 0.005 tighter floor

    feed = CalibrationFeed(
        registry,
        AdjustmentEngine(InstitutionType.BANK, engine),
        BenchmarkTriangulator(InstitutionType.BANK),
        regulatory_floor=0.005,
    )
    out = feed.for_central_tendency("residential_mortgage")
    assert out.external_lra == 0.005
    assert out.floor_triggered is True
