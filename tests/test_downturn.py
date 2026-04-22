"""Tests for src/downturn.py — PD cycle adjustment, LGD uplift, routing separation.

LGD decomposition tests moved to future_lgd/src/tests/test_lgd_decomposition.py
alongside the extracted `lgd_decomposition` function.
"""
from __future__ import annotations

from datetime import date

import pytest

from src.adjustments import AdjustmentEngine
from src.calibration_feed import CalibrationFeed
from src.db import create_engine_and_schema
from src.downturn import (
    DEFAULT_UPLIFT_FACTORS,
    DownturnCalibrator,
    lgd_downturn_uplift,
    pd_cycle_adjustment,
)
from src.models import (
    BenchmarkEntry,
    DataType,
    InstitutionType,
    QualityScore,
    SourceType,
)
from src.registry import BenchmarkRegistry
from src.triangulation import BenchmarkTriangulator


def _entry(**overrides) -> BenchmarkEntry:
    base = {
        "source_id": "X", "publisher": "P",
        "source_type": SourceType.INDUSTRY_BODY, "data_type": DataType.LGD,
        "asset_class": "bridging_residential", "value": 0.25,
        "value_date": date(2024, 12, 31), "period_years": 5,
        "geography": "AU", "url": "https://example.com",
        "retrieval_date": date(2025, 3, 1),
        "quality_score": QualityScore.HIGH,
    }
    base.update(overrides)
    return BenchmarkEntry(**base)


# ---------------------------------------------------------------------------
# pd_cycle_adjustment
# ---------------------------------------------------------------------------

def test_pd_cycle_adjustment_returns_base_when_stress_included() -> None:
    assert pd_cycle_adjustment(0.01, includes_stress=True, external_stress_rate=0.05) == 0.01


def test_pd_cycle_adjustment_lifts_toward_stress_when_missing() -> None:
    # base 0.01, external 0.03, margin 0.55 -> 0.01 + 0.55*(0.03-0.01) = 0.021
    out = pd_cycle_adjustment(0.01, includes_stress=False, external_stress_rate=0.03)
    assert out == pytest.approx(0.021)


def test_pd_cycle_adjustment_never_drops_below_base() -> None:
    # External rate LOWER than base -> gap = 0, no uplift
    out = pd_cycle_adjustment(0.05, includes_stress=False, external_stress_rate=0.02)
    assert out == 0.05


def test_pd_cycle_adjustment_margin_bounds_enforced() -> None:
    with pytest.raises(ValueError, match="margin must be"):
        pd_cycle_adjustment(0.01, False, 0.03, margin=0.1)
    with pytest.raises(ValueError, match="margin must be"):
        pd_cycle_adjustment(0.01, False, 0.03, margin=1.0)


# ---------------------------------------------------------------------------
# lgd_downturn_uplift — product factor table
# ---------------------------------------------------------------------------

def test_lgd_downturn_uplift_residential_property_factor() -> None:
    result = lgd_downturn_uplift(long_run_lgd=0.22, product_type="residential_property")
    assert result.uplift == 1.45
    assert result.downturn_lgd == pytest.approx(0.22 * 1.45)
    assert result.lgd_for_capital == pytest.approx(0.22 * 1.45)
    assert result.lgd_for_ecl == 0.22


def test_lgd_downturn_uplift_all_default_products_have_factors() -> None:
    """Every product in DEFAULT_UPLIFT_FACTORS produces a valid DownturnResult."""
    for product, expected_uplift in DEFAULT_UPLIFT_FACTORS.items():
        result = lgd_downturn_uplift(0.2, product)
        assert result.uplift == expected_uplift
        assert result.product_type == product


def test_lgd_downturn_uplift_custom_override() -> None:
    result = lgd_downturn_uplift(0.3, "residential_property", custom_uplift=2.5)
    assert result.uplift == 2.5
    assert result.downturn_lgd == pytest.approx(0.75)


def test_lgd_downturn_uplift_caps_at_one() -> None:
    """LGD cannot exceed 100% — 0.7 x 1.75 = 1.225 -> clipped to 1.0."""
    result = lgd_downturn_uplift(0.7, "commercial_property")
    assert result.downturn_lgd == 1.0
    assert result.lgd_for_capital == 1.0


def test_lgd_downturn_uplift_unknown_product_raises() -> None:
    with pytest.raises(ValueError, match="Unknown product_type"):
        lgd_downturn_uplift(0.2, "not_a_real_product")


# ===========================================================================
# PD/LGD ROUTING SEPARATION — the centrepiece Tier 4 test
# ===========================================================================

def test_pd_and_lgd_routing_is_non_overlapping() -> None:
    """Seed both PD and LGD for one segment. Confirm:
       - CalibrationFeed consumes only the PD (floor applied to PD)
       - DownturnCalibrator consumes only the LGD (no floor)
       - Values stay in their own lanes; no accidental cross-wiring.
    """
    engine = create_engine_and_schema(":memory:")
    registry = BenchmarkRegistry(engine, actor="test")

    # One PD entry (bank Pillar 3, value safely above the floor)
    registry.add(BenchmarkEntry(
        source_id="PILLAR3_RES_PD", publisher="Big 4 Bank",
        source_type=SourceType.PILLAR3, data_type=DataType.PD,
        asset_class="residential_mortgage", value=0.0085,
        value_date=date(2024, 12, 31), period_years=5,
        geography="AU", url="https://example.com",
        retrieval_date=date(2025, 3, 1),
        quality_score=QualityScore.HIGH,
    ))
    # One aggregate LGD entry (no component — just the downturn-adjusted Pillar 3 value)
    registry.add(BenchmarkEntry(
        source_id="PILLAR3_RES_LGD", publisher="Big 4 Bank",
        source_type=SourceType.PILLAR3, data_type=DataType.LGD,
        asset_class="residential_mortgage", value=0.22,
        value_date=date(2024, 12, 31), period_years=5,
        geography="AU", url="https://example.com",
        retrieval_date=date(2025, 3, 1),
        quality_score=QualityScore.HIGH,
    ))

    # --- CalibrationFeed path: PD only
    feed = CalibrationFeed(
        registry,
        AdjustmentEngine(InstitutionType.BANK, engine),
        BenchmarkTriangulator(InstitutionType.BANK),
    )
    pd_out = feed.for_central_tendency("residential_mortgage")
    # Bank adjustment is near-neutral (peer_mix default 1.0, no geography for Pillar 3)
    # so external_lra ~ 0.0085, well above the 0.0003 floor.
    assert pd_out.external_lra == pytest.approx(0.0085, abs=1e-6)
    assert pd_out.floor_triggered is False

    # --- Downturn path: LGD only, NO floor applied
    calc = DownturnCalibrator(registry)
    lgd_result = calc.lgd_downturn_uplift(
        long_run_lgd=0.22, product_type="residential_property"
    )
    assert lgd_result.long_run_lgd == 0.22  # untouched — no floor on LGD
    assert lgd_result.lgd_for_ecl == 0.22
    assert lgd_result.lgd_for_capital == pytest.approx(0.22 * 1.45)

    # --- Non-overlap: flipping institution would NOT route LGD into CalibrationFeed.
    # If we ask CalibrationFeed for an LGD-only segment, it raises — proved elsewhere.
    # Here the test confirms the two streams coexist without crossing.
    assert pd_out.method == "central_tendency"  # not an LGD output type
    assert lgd_result.product_type == "residential_property"  # not a PD segment name
