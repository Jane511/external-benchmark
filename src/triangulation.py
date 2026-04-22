"""Triangulation — combine adjusted sources into one segment-level benchmark.

Design constraint (enforced, non-negotiable):
    BenchmarkTriangulator.triangulate() accepts ONLY list[AdjustmentResult].
    Passing a raw BenchmarkEntry raises TypeError. The registry -> adjust ->
    triangulate pipeline order is not optional — it's what guarantees the
    audit trail matches the governance framework.

Four methods:
    simple_average, weighted_by_years (default), quality_weighted, trimmed_mean.
    trimmed_mean requires >=4 sources.

Confidence N (Bayesian blending weight downstream):
    per_source_n = base_n[source_type] * min(period_years, 10) / 5.0
    total_n = sum(per_source_n)
    private_credit: total_n *= 0.7
    cap at 500 (enforced by TriangulationResult.confidence_n field validator).
"""
from __future__ import annotations

from typing import Any, Literal, Optional

from src.models import (
    AdjustmentResult,
    BenchmarkEntry,
    InstitutionType,
    QualityScore,
    SourceType,
    TriangulationResult,
)


TriangulationMethod = Literal[
    "simple_average", "weighted_by_years", "quality_weighted", "trimmed_mean"
]


# Per-source confidence base N (plan §5).
_BASE_N: dict[SourceType, int] = {
    SourceType.PILLAR3: 100,
    SourceType.APRA_ADI: 150,
    SourceType.RATING_AGENCY: 300,
    SourceType.ICC_TRADE: 400,
    SourceType.INDUSTRY_BODY: 100,
    SourceType.LISTED_PEER: 100,
    SourceType.BUREAU: 80,
    SourceType.REGULATORY: 200,
    SourceType.RBA: 100,
    SourceType.INSOLVENCY: 100,
}

_QUALITY_WEIGHTS: dict[QualityScore, int] = {
    QualityScore.HIGH: 3,
    QualityScore.MEDIUM: 2,
    QualityScore.LOW: 1,
}


class BenchmarkTriangulator:
    """Combine adjusted sources into one segment benchmark with a confidence N."""

    def __init__(
        self, institution_type: InstitutionType = InstitutionType.BANK,
    ) -> None:
        self._inst = institution_type

    def triangulate(
        self,
        adjusted_sources: list[AdjustmentResult],
        method: TriangulationMethod = "weighted_by_years",
        *,
        segment: str = "unspecified",
        raw_entries: Optional[list[BenchmarkEntry]] = None,
    ) -> TriangulationResult:
        """Combine adjusted sources per `method`.

        `raw_entries` is an optional parallel list (same length / order as
        `adjusted_sources`) carrying period_years / source_type / quality
        for weighting and confidence-N computation. Without it, equal
        weights and a default base N of 100 are used.
        """
        self._validate_input(adjusted_sources)
        if not adjusted_sources:
            raise ValueError("adjusted_sources is empty")

        if raw_entries is not None and len(raw_entries) != len(adjusted_sources):
            raise ValueError(
                f"raw_entries length {len(raw_entries)} does not match "
                f"adjusted_sources length {len(adjusted_sources)}"
            )

        if method == "trimmed_mean" and len(adjusted_sources) < 4:
            raise ValueError(
                f"trimmed_mean requires >= 4 sources, got {len(adjusted_sources)}"
            )

        values = [a.adjusted_value for a in adjusted_sources]

        if method == "simple_average":
            benchmark_value = sum(values) / len(values)
        elif method == "weighted_by_years":
            weights = (
                [max(e.period_years, 1) for e in raw_entries]
                if raw_entries else [1] * len(values)
            )
            benchmark_value = _weighted_avg(values, weights)
        elif method == "quality_weighted":
            weights = (
                [_QUALITY_WEIGHTS[e.quality_score] for e in raw_entries]
                if raw_entries else [_QUALITY_WEIGHTS[QualityScore.MEDIUM]] * len(values)
            )
            benchmark_value = _weighted_avg(values, weights)
        elif method == "trimmed_mean":
            sorted_vals = sorted(values)
            trimmed = sorted_vals[1:-1]  # drop min and max
            benchmark_value = sum(trimmed) / len(trimmed)
        else:
            raise ValueError(f"Unknown triangulation method: {method!r}")

        confidence_n = self._compute_confidence_n(adjusted_sources, raw_entries)
        breakdown = _per_source_breakdown(adjusted_sources, raw_entries)

        return TriangulationResult(
            segment=segment,
            benchmark_value=benchmark_value,
            confidence_n=confidence_n,  # clamped at 500 by the model's field_validator
            source_count=len(adjusted_sources),
            method=method,
            per_source_breakdown=breakdown,
        )

    @staticmethod
    def _validate_input(adjusted_sources: Any) -> None:
        """Enforce AdjustmentResult-only input. Raises TypeError otherwise."""
        if not isinstance(adjusted_sources, list):
            raise TypeError(
                f"adjusted_sources must be list[AdjustmentResult], "
                f"got {type(adjusted_sources).__name__}"
            )
        for i, item in enumerate(adjusted_sources):
            if isinstance(item, BenchmarkEntry):
                raise TypeError(
                    f"adjusted_sources[{i}] is a BenchmarkEntry. Triangulation "
                    "accepts only AdjustmentResult — call AdjustmentEngine.adjust() "
                    "on each entry first, then pass the results here."
                )
            if not isinstance(item, AdjustmentResult):
                raise TypeError(
                    f"adjusted_sources[{i}] has type {type(item).__name__}; "
                    "expected AdjustmentResult"
                )

    def _compute_confidence_n(
        self,
        adjusted_sources: list[AdjustmentResult],
        raw_entries: Optional[list[BenchmarkEntry]],
    ) -> int:
        total = 0.0
        for i, _ in enumerate(adjusted_sources):
            if raw_entries:
                e = raw_entries[i]
                base = _BASE_N.get(e.source_type, 100)
                years = min(e.period_years, 10)
            else:
                base = 100
                years = 5
            total += base * years / 5.0
        if self._inst == InstitutionType.PRIVATE_CREDIT:
            total *= 0.7
        return int(round(total))


def _weighted_avg(values: list[float], weights: list[float]) -> float:
    total_w = sum(weights)
    if total_w <= 0:
        raise ValueError("weights must sum to > 0")
    return sum(v * w for v, w in zip(values, weights)) / total_w


def _per_source_breakdown(
    adjusted: list[AdjustmentResult],
    raw: Optional[list[BenchmarkEntry]],
) -> list[dict[str, Any]]:
    breakdown: list[dict[str, Any]] = []
    for i, a in enumerate(adjusted):
        entry_info: dict[str, Any] = {
            "raw_value": a.raw_value,
            "adjusted_value": a.adjusted_value,
            "institution_type": a.institution_type.value,
            "product": a.product,
            "final_multiplier": a.final_multiplier,
        }
        if raw:
            entry_info["source_id"] = raw[i].source_id
            entry_info["source_type"] = raw[i].source_type.value
            entry_info["period_years"] = raw[i].period_years
        breakdown.append(entry_info)
    return breakdown
