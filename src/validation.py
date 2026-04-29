"""Cross-source validation — flags anomalies without computing a consensus.

The engine deliberately does NOT triangulate (compute a weighted average
across sources). Triangulation is a use-case-specific decision and belongs
in the consuming project. What the engine DOES do is flag obvious
data-quality issues that any consumer would want to know about:

  - Spread: max - min across sources for the same segment
  - Outlier: any single source more than `outlier_threshold` × the
    median of the others
  - Vintage staleness: any source older than `staleness_days`
  - Bank-vs-non-bank: ratio of non-bank median to Big-4 median (info only)

These flags are surfaced on each ObservationSet and in the per-segment
validation summary that goes into the engine reports. They do not modify
or filter the underlying observations.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Sequence

from src.models import RawObservation


# Bank Pillar 3 source IDs (lower-case). Used to split bank vs non-bank in flags.
BIG4_SOURCE_IDS: frozenset[str] = frozenset({"cba", "nab", "wbc", "anz", "macquarie", "mqg"})


def is_big4_source_id(source_id: str) -> bool:
    """Return True if ``source_id`` belongs to a Big 4 bank.

    Accepts both short forms (``"cba"`` / ``"mqg"``) used in tests and the long
    Pillar-3 forms produced by the seed / migration pipeline
    (``"CBA_PILLAR3_RES_2024H2"``). The check is case-insensitive and
    matches either an exact short form or a long form whose first
    underscore- or hyphen-separated token is one of the Big 4.
    """
    sid = source_id.lower()
    if sid in BIG4_SOURCE_IDS:
        return True
    normalized = sid.replace("-", "_")
    head = normalized.split("_", 1)[0]
    return head in BIG4_SOURCE_IDS or normalized.startswith("macquarie_bank_")


@dataclass
class ValidationFlags:
    """Per-segment data-quality flags. Informational only — values are not modified."""

    n_sources: int
    spread_pct: float | None             # (max - min) / median across all sources
    outlier_sources: list[str]           # source IDs flagged as outliers
    stale_sources: list[str]             # source IDs older than staleness_days
    bank_vs_nonbank_ratio: float | None  # nonbank median / big4 median
    big4_spread_pct: float | None        # spread within Big 4 only

    def __post_init__(self) -> None:
        if self.spread_pct is not None and self.spread_pct < 0:
            raise ValueError("spread_pct cannot be negative")


def _median(values: Sequence[float]) -> float:
    """Plain median (no numpy dependency). Returns 0.0 for empty input."""
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return s[mid]
    return (s[mid - 1] + s[mid]) / 2.0


def compute_validation_flags(
    observations: Sequence[RawObservation],
    *,
    outlier_threshold: float = 2.0,
    staleness_days: int = 100,
    today: date | None = None,
) -> ValidationFlags:
    """Compute cross-source validation flags WITHOUT averaging.

    The engine never publishes a consensus value. It publishes the raw
    observations and these flags. Consumers decide what to do with the spread.
    """
    today = today or date.today()
    n = len(observations)
    if n == 0:
        return ValidationFlags(
            n_sources=0,
            spread_pct=None,
            outlier_sources=[],
            stale_sources=[],
            bank_vs_nonbank_ratio=None,
            big4_spread_pct=None,
        )

    values = [o.value for o in observations]
    median = _median(values)
    spread = (max(values) - min(values)) / median if median > 0 else None

    outliers = [
        o.source_id for o in observations
        if median > 0 and (
            o.value > outlier_threshold * median
            or o.value < median / outlier_threshold
        )
    ]
    stale = [
        o.source_id for o in observations
        if (today - o.as_of_date).days > staleness_days
    ]

    big4_vals = [o.value for o in observations if is_big4_source_id(o.source_id)]
    nonbank_vals = [o.value for o in observations if not is_big4_source_id(o.source_id)]

    bank_vs_nonbank: float | None = None
    if big4_vals and nonbank_vals:
        big4_med = _median(big4_vals)
        non_med = _median(nonbank_vals)
        if big4_med > 0:
            bank_vs_nonbank = non_med / big4_med

    big4_spread: float | None = None
    if len(big4_vals) >= 2:
        big4_med = _median(big4_vals)
        if big4_med > 0:
            big4_spread = (max(big4_vals) - min(big4_vals)) / big4_med

    return ValidationFlags(
        n_sources=n,
        spread_pct=spread,
        outlier_sources=outliers,
        stale_sources=stale,
        bank_vs_nonbank_ratio=bank_vs_nonbank,
        big4_spread_pct=big4_spread,
    )
