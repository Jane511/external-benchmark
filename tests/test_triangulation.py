"""Tests for src/triangulation.py — four methods, TypeError gate, confidence-N."""
from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from src.models import (
    AdjustmentResult,
    AdjustmentStep,
    BenchmarkEntry,
    DataType,
    InstitutionType,
    QualityScore,
    SourceType,
)
from src.triangulation import BenchmarkTriangulator


def _adj(raw: float, adjusted: float, *, product: str = "bridging_commercial") -> AdjustmentResult:
    return AdjustmentResult(
        raw_value=raw,
        adjusted_value=adjusted,
        institution_type=InstitutionType.PRIVATE_CREDIT,
        product=product,
        asset_class="commercial_property_investment",
        steps=[AdjustmentStep(name="noop", multiplier=1.0)],
        final_multiplier=adjusted / raw if raw else 1.0,
    )


def _entry(
    value: float,
    *,
    source_type: SourceType = SourceType.PILLAR3,
    period_years: int = 5,
    quality: QualityScore = QualityScore.HIGH,
    source_id: str = "X",
) -> BenchmarkEntry:
    return BenchmarkEntry(
        source_id=source_id,
        publisher="Test",
        source_type=source_type,
        data_type=DataType.PD,
        asset_class="commercial_property_investment",
        value=value,
        value_date=date(2024, 12, 31),
        period_years=period_years,
        geography="AU",
        url="https://example.com",
        retrieval_date=date(2025, 3, 1),
        quality_score=quality,
    )


# ---------------------------------------------------------------------------
# Non-negotiable: TypeError on raw BenchmarkEntry input
# ---------------------------------------------------------------------------

def test_triangulate_rejects_list_of_benchmark_entry() -> None:
    entries = [_entry(0.025)]
    t = BenchmarkTriangulator()
    with pytest.raises(TypeError, match="BenchmarkEntry"):
        t.triangulate(entries, method="simple_average")


def test_triangulate_rejects_non_list() -> None:
    t = BenchmarkTriangulator()
    with pytest.raises(TypeError, match="must be list"):
        t.triangulate("not a list", method="simple_average")  # type: ignore[arg-type]


def test_triangulate_rejects_mixed_list() -> None:
    t = BenchmarkTriangulator()
    with pytest.raises(TypeError):
        t.triangulate(
            [_adj(0.02, 0.04), _entry(0.02)],  # second item is wrong type
            method="simple_average",
        )


# ---------------------------------------------------------------------------
# Four methods — known inputs -> known outputs
# ---------------------------------------------------------------------------

def test_simple_average_three_sources() -> None:
    t = BenchmarkTriangulator()
    r = t.triangulate(
        [_adj(0.02, 0.02), _adj(0.03, 0.03), _adj(0.04, 0.04)],
        method="simple_average",
    )
    assert r.benchmark_value == pytest.approx(0.03)
    assert r.source_count == 3
    assert r.method == "simple_average"


def test_weighted_by_years() -> None:
    """Weight = period_years (uncapped for weighting; the 10-year cap applies only to confidence_n)."""
    t = BenchmarkTriangulator()
    adjs = [_adj(0.01, 0.01), _adj(0.02, 0.02), _adj(0.03, 0.03)]
    raws = [_entry(0.01, period_years=5),
            _entry(0.02, period_years=10),
            _entry(0.03, period_years=15)]
    r = t.triangulate(adjs, method="weighted_by_years", raw_entries=raws)
    # weights 5, 10, 15 -> (0.01*5 + 0.02*10 + 0.03*15)/(5+10+15) = 0.70/30
    assert r.benchmark_value == pytest.approx(0.70 / 30)


def test_quality_weighted() -> None:
    """HIGH=3, MEDIUM=2, LOW=1. Values 0.01/0.02/0.03 with H/M/L."""
    t = BenchmarkTriangulator()
    adjs = [_adj(0.01, 0.01), _adj(0.02, 0.02), _adj(0.03, 0.03)]
    raws = [_entry(0.01, quality=QualityScore.HIGH),
            _entry(0.02, quality=QualityScore.MEDIUM),
            _entry(0.03, quality=QualityScore.LOW)]
    r = t.triangulate(adjs, method="quality_weighted", raw_entries=raws)
    # weights 3, 2, 1 -> (0.01*3 + 0.02*2 + 0.03*1)/6 = 0.10/6 = 0.01666...
    assert r.benchmark_value == pytest.approx(0.10 / 6)


def test_trimmed_mean_drops_min_and_max() -> None:
    """Sorted 0.01, 0.02, 0.03, 0.10 -> trim -> mean(0.02, 0.03) = 0.025."""
    t = BenchmarkTriangulator()
    r = t.triangulate(
        [_adj(0.01, 0.01), _adj(0.02, 0.02), _adj(0.03, 0.03), _adj(0.10, 0.10)],
        method="trimmed_mean",
    )
    assert r.benchmark_value == pytest.approx(0.025)


def test_trimmed_mean_requires_four_sources() -> None:
    t = BenchmarkTriangulator()
    with pytest.raises(ValueError, match="requires >= 4"):
        t.triangulate(
            [_adj(0.01, 0.01), _adj(0.02, 0.02), _adj(0.03, 0.03)],
            method="trimmed_mean",
        )


# ---------------------------------------------------------------------------
# Confidence N: formula, cap, PC discount
# ---------------------------------------------------------------------------

def test_confidence_n_single_pillar3_source_bank() -> None:
    """Pillar3 base=100; period 5; bank: 100 * 5/5 = 100."""
    t = BenchmarkTriangulator(InstitutionType.BANK)
    adjs = [_adj(0.01, 0.01)]
    raws = [_entry(0.01, source_type=SourceType.PILLAR3, period_years=5)]
    r = t.triangulate(adjs, method="simple_average", raw_entries=raws)
    assert r.confidence_n == 100


def test_confidence_n_private_credit_discount_applied() -> None:
    """Same inputs as bank test; PC * 0.7 -> 70."""
    t = BenchmarkTriangulator(InstitutionType.PRIVATE_CREDIT)
    adjs = [_adj(0.01, 0.01)]
    raws = [_entry(0.01, source_type=SourceType.PILLAR3, period_years=5)]
    r = t.triangulate(adjs, method="simple_average", raw_entries=raws)
    assert r.confidence_n == 70


def test_confidence_n_capped_at_500() -> None:
    """Many rating-agency sources -> uncapped would be 6000; clamped to 500."""
    t = BenchmarkTriangulator()
    adjs = [_adj(0.02, 0.02) for _ in range(10)]
    raws = [
        _entry(0.02, source_type=SourceType.RATING_AGENCY, period_years=10,
               source_id=f"R{i}")
        for i in range(10)
    ]
    # Each contributes 300 * 10/5 = 600; total 6000; clamped to 500.
    r = t.triangulate(adjs, method="simple_average", raw_entries=raws)
    assert r.confidence_n == 500


def test_confidence_n_period_years_capped_at_10() -> None:
    """period_years=15 contributes as min(15,10)=10 -> base * 10/5 = 2*base."""
    t = BenchmarkTriangulator()
    adjs = [_adj(0.02, 0.02)]
    raws = [_entry(0.02, source_type=SourceType.PILLAR3, period_years=15)]
    r = t.triangulate(adjs, method="simple_average", raw_entries=raws)
    assert r.confidence_n == 200  # 100 * 10/5


# ---------------------------------------------------------------------------
# Breakdown / metadata
# ---------------------------------------------------------------------------

def test_per_source_breakdown_includes_source_ids_when_raw_entries_given() -> None:
    t = BenchmarkTriangulator()
    adjs = [_adj(0.02, 0.04), _adj(0.03, 0.06)]
    raws = [_entry(0.02, source_id="A"), _entry(0.03, source_id="B")]
    r = t.triangulate(adjs, method="simple_average", raw_entries=raws, segment="cre")
    assert r.segment == "cre"
    ids = {b["source_id"] for b in r.per_source_breakdown}
    assert ids == {"A", "B"}


def test_empty_input_raises() -> None:
    t = BenchmarkTriangulator()
    with pytest.raises(ValueError, match="empty"):
        t.triangulate([], method="simple_average")
