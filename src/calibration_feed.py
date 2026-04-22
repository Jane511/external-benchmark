"""CalibrationFeed — five PD-only calibration methods with regulatory floor.

**PD-only by design.** LGD benchmarks never flow through CalibrationFeed;
they go through `downturn.py` (for downturn-adjusted LGD) or direct registry
reads (for long-run aggregate LGD). If a caller requests a segment with no
PD entries, CalibrationFeed raises ValueError rather than silently returning
an LGD value.

Regulatory floor 0.03% (0.0003) applies at the OUTPUT stage to all five
methods. LGD outputs are never floored — the floor belongs to PD only.

The five methods each return a typed Pydantic output from `models.py`
(tagged union discriminated by `method`); callers can access fields by
attribute or dump to dict via `.model_dump()`.

Internal adjustments run in `what_if={}` mode to avoid persisting each
calibration query as a new row in the `adjustments` table — CalibrationFeed
is a read-heavy consumer, not a write-through adjustment trigger.
"""
from __future__ import annotations

from typing import Optional

from src.adjustments import AdjustmentEngine
from src.models import (
    AdjustmentResult,
    BayesianBlendingOutput,
    BenchmarkEntry,
    CentralTendencyOutput,
    DataType,
    ExternalBlendingOutput,
    LogisticRecalibrationOutput,
    PlutoTascheOutput,
)
from src.registry import BenchmarkRegistry
from src.triangulation import BenchmarkTriangulator, TriangulationMethod


DEFAULT_REGULATORY_FLOOR: float = 0.0003  # APRA APS 113 PD floor (3 bps)


class CalibrationFeed:
    """Five PD segment-level outputs for the downstream PD calibration module."""

    def __init__(
        self,
        registry: BenchmarkRegistry,
        adjustment_engine: AdjustmentEngine,
        triangulator: BenchmarkTriangulator,
        regulatory_floor: float = DEFAULT_REGULATORY_FLOOR,
        triangulation_method: TriangulationMethod = "weighted_by_years",
    ) -> None:
        self._registry = registry
        self._engine = adjustment_engine
        self._triangulator = triangulator
        self._floor = regulatory_floor
        self._method = triangulation_method

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _triangulate_pd_segment(
        self, segment: str, product: Optional[str] = None,
    ) -> tuple[float, int]:
        """Fetch PD entries for segment, adjust each, triangulate. Returns (value, N)."""
        entries: list[BenchmarkEntry] = self._registry.get_by_segment(
            asset_class=segment, data_type=DataType.PD,
        )
        if not entries:
            raise ValueError(
                f"No PD entries for segment {segment!r}. CalibrationFeed is "
                "PD-only by design — LGD segments must be routed through "
                "downturn.py or a direct registry read."
            )

        effective_product = product or segment
        adjusted: list[AdjustmentResult] = []
        for e in entries:
            # what_if={} skips DB persistence for read-path calibration queries.
            # The adjustment layer is still exercised (Stage 1 + Stage 2 multipliers).
            r = self._engine.adjust(
                raw_value=e.value,
                source_type=e.source_type,
                asset_class=e.asset_class,
                product=effective_product,
                source_id=e.source_id,
                what_if={},
            )
            adjusted.append(r)

        result = self._triangulator.triangulate(
            adjusted, method=self._method,
            segment=segment, raw_entries=entries,
        )
        return result.benchmark_value, result.confidence_n

    def _apply_floor(self, value: float) -> tuple[float, bool]:
        """Clamp `value` up to the regulatory floor; return (floored_value, was_triggered)."""
        if value < self._floor:
            return self._floor, True
        return value, False

    # ------------------------------------------------------------------
    # Five methods (PD only)
    # ------------------------------------------------------------------

    def for_central_tendency(
        self, segment: str, *, product: Optional[str] = None,
    ) -> CentralTendencyOutput:
        value, _n = self._triangulate_pd_segment(segment, product)
        lra, triggered = self._apply_floor(value)
        return CentralTendencyOutput(
            segment=segment, external_lra=lra, floor_triggered=triggered,
        )

    def for_logistic_recalibration(
        self, segment: str, *, product: Optional[str] = None,
    ) -> LogisticRecalibrationOutput:
        value, n = self._triangulate_pd_segment(segment, product)
        lra, triggered = self._apply_floor(value)
        return LogisticRecalibrationOutput(
            segment=segment, target_lra=lra, confidence_n=n, floor_triggered=triggered,
        )

    def for_bayesian_blending(
        self, segment: str, *, product: Optional[str] = None,
    ) -> BayesianBlendingOutput:
        value, n = self._triangulate_pd_segment(segment, product)
        pd, triggered = self._apply_floor(value)
        return BayesianBlendingOutput(
            segment=segment, external_pd=pd, confidence_n=n, floor_triggered=triggered,
        )

    def for_external_blending(
        self,
        segment: str,
        internal_years: float,
        *,
        product: Optional[str] = None,
    ) -> ExternalBlendingOutput:
        """Weight schedule: <3yr:0.30, 3-4yr:0.50, 4-5yr:0.70, 5+yr:0.90."""
        value, _n = self._triangulate_pd_segment(segment, product)
        lra, triggered = self._apply_floor(value)
        internal_weight = _internal_weight_for_years(internal_years)
        return ExternalBlendingOutput(
            segment=segment, external_lra=lra,
            internal_weight=internal_weight, floor_triggered=triggered,
        )

    def for_pluto_tasche(
        self, segment: str, *, product: Optional[str] = None,
    ) -> PlutoTascheOutput:
        """Pluto-Tasche low-default-portfolio method — external serves comparison role only."""
        value, _n = self._triangulate_pd_segment(segment, product)
        pd, triggered = self._apply_floor(value)
        return PlutoTascheOutput(
            segment=segment, external_pd=pd,
            role="comparison_only", floor_triggered=triggered,
        )


def _internal_weight_for_years(years: float) -> float:
    """Return the internal-data weight for the external_blending schedule."""
    if years < 3:
        return 0.30
    if years < 4:
        return 0.50
    if years < 5:
        return 0.70
    return 0.90
