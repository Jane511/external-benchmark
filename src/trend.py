"""Trend helpers for raw observations.

Trend rows compare the latest observation for a (segment, parameter,
source_id) tuple with that same source's immediately prior vintage.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable

from src.models import RawObservation
from src.registry import BenchmarkRegistry


@dataclass(frozen=True)
class SegmentTrendRow:
    segment: str
    parameter: str
    source_id: str
    current_value: float | None
    current_as_of: date
    prior_value: float | None
    prior_as_of: date
    delta: float | None
    pct_change: float | None


def build_segment_trends(registry: BenchmarkRegistry) -> list[SegmentTrendRow]:
    """Return source-level current-vs-prior trend rows from raw observations."""
    return build_segment_trends_from_observations(registry.query_observations())


def build_segment_trends_from_observations(
    observations: Iterable[RawObservation],
) -> list[SegmentTrendRow]:
    grouped: dict[tuple[str, str, str], list[RawObservation]] = {}
    for obs in observations:
        # Commentary rows (value=None) carry no numeric trend.
        if obs.value is None:
            continue
        grouped.setdefault((obs.segment, obs.parameter, obs.source_id), []).append(obs)

    rows: list[SegmentTrendRow] = []
    for (segment, parameter, source_id), obs_rows in grouped.items():
        latest_by_date: dict[date, RawObservation] = {}
        for obs in obs_rows:
            latest_by_date.setdefault(obs.as_of_date, obs)
        vintages = sorted(latest_by_date.values(), key=lambda o: o.as_of_date, reverse=True)
        if len(vintages) < 2:
            continue
        current = vintages[0]
        prior = vintages[1]
        if current.value is None or prior.value is None:
            continue
        delta = current.value - prior.value
        pct_change = delta / prior.value if prior.value else None
        rows.append(
            SegmentTrendRow(
                segment=segment,
                parameter=parameter,
                source_id=source_id,
                current_value=current.value,
                current_as_of=current.as_of_date,
                prior_value=prior.value,
                prior_as_of=prior.as_of_date,
                delta=delta,
                pct_change=pct_change,
            )
        )
    return sorted(rows, key=lambda r: (r.segment, r.parameter, r.source_id))

