"""Raw observation API — what the engine publishes to downstream consumers.

The engine no longer computes a "central tendency" or "blended PD" value.
It publishes a list of raw observations per segment, each tagged with its
source, vintage, methodology footnote, and a sample-size proxy where
available. Consumers decide how to weight them.

Typical consumer call:

    obs = PeerObservations(registry).for_segment("commercial_property")
    # obs.observations is a list[RawObservation], each with .source_id,
    # .value, .as_of_date, .reporting_basis, .methodology_note,
    # .sample_size_n

Validation flags are available alongside but separate from the values:

    flags = obs.validation_flags          # ValidationFlags object
    # flags.bank_vs_nonbank_ratio = 1.85
    # flags.outlier_sources = []

The PD workbook's load_pillar3_peer_observations() reads this object,
applies its own EBA MoC adjustments (Brief 2), and produces the LRA target.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional, Sequence

from src.models import DataDefinitionClass, RawObservation, SourceType
from src.registry import BenchmarkRegistry
from src.validation import (
    BIG4_SOURCE_IDS,
    canonical_segment,
    ValidationFlags,
    compute_validation_flags,
    is_big4_source_id,
)


@dataclass
class ObservationSet:
    """All raw observations for a single segment, plus validation flags."""

    segment: str
    observations: list[RawObservation]
    validation_flags: ValidationFlags
    queried_at: date

    @property
    def n_sources(self) -> int:
        return len(self.observations)

    def by_source_type(
        self, *, big4_only: bool = False, nonbank_only: bool = False,
    ) -> list[RawObservation]:
        """Filter helper — by Big 4 or non-bank ASX-listed."""
        if big4_only and nonbank_only:
            raise ValueError("big4_only and nonbank_only are mutually exclusive")
        if big4_only:
            return [o for o in self.observations
                    if is_big4_source_id(o.source_id)]
        if nonbank_only:
            return [o for o in self.observations
                    if not is_big4_source_id(o.source_id)]
        return list(self.observations)

    def latest_per_source(self) -> list[RawObservation]:
        """One observation per source — the freshest by as_of_date."""
        latest: dict[str, RawObservation] = {}
        for o in self.observations:
            cur = latest.get(o.source_id)
            if cur is None or o.as_of_date > cur.as_of_date:
                latest[o.source_id] = o
        return list(latest.values())


class PeerObservations:
    """Public read API for raw per-source observations.

    Replaces CalibrationFeed (which returned adjusted values).
    """

    def __init__(
        self,
        registry: BenchmarkRegistry,
        *,
        today: date | None = None,
        refresh_schedules: Optional[dict[str, int]] = None,
        refresh_pipeline_quiet: bool = False,
    ) -> None:
        self._registry = registry
        self._today = today or date.today()
        self._refresh_schedules = refresh_schedules
        self._refresh_pipeline_quiet = refresh_pipeline_quiet

    def for_segment(
        self,
        segment: str,
        *,
        product: Optional[str] = None,
        source_type: Optional[SourceType] = None,
        only_pd: bool = True,
        definition_classes: Optional[Sequence[DataDefinitionClass]] = None,
    ) -> ObservationSet:
        """Return the ObservationSet for this segment.

        Filters: only PD entries by default (use only_pd=False to include
        LGD or other parameter types). Optional product and source_type
        filters narrow further. ``definition_classes`` lets a consumer
        restrict to a specific definition family — e.g. pass
        ``[DataDefinitionClass.BASEL_PD_ONE_YEAR]`` to exclude arrears /
        impaired / commentary observations.

        When ``only_pd=False``, the displayed observation list still
        includes every parameter (PD, LGD, arrears, commentary, ...) but
        the validation arithmetic (spread, median, outlier detection,
        peer ratio) is computed over the PD subset only. Mixing PD with
        LGD in a single spread calc is meaningless: a 0.7% PD next to a
        24% LGD gives a 33x spread that says nothing about either.
        """
        canonical = canonical_segment(segment)
        rows = self._registry.query_observations(
            segment=canonical,
            product=product,
            source_type=source_type,
            parameter="pd" if only_pd else None,
            definition_classes=definition_classes,
        )
        # Pass all rows; ``compute_validation_flags`` itself restricts
        # spread / median / outlier / peer-ratio arithmetic to PD only
        # (mixing PD with LGD in a single spread is meaningless) but
        # uses the full row set for stale detection and source counts.
        flags = compute_validation_flags(
            rows,
            today=self._today,
            refresh_schedules=self._refresh_schedules,
            refresh_pipeline_quiet=self._refresh_pipeline_quiet,
        )
        return ObservationSet(
            segment=canonical,
            observations=rows,
            validation_flags=flags,
            queried_at=self._today,
        )

    def all_segments(self) -> list[str]:
        """Return the list of all segments with at least one observation."""
        return self._registry.list_segments()
