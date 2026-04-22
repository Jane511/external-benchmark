"""Pydantic v2 data models for the External Benchmark Engine.

Layering (per plan):
    enums -> BenchmarkEntry (with validators) -> supporting models

This module has no dependencies on db/registry/adjustment layers.
"""
from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Any, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class SourceType(str, Enum):
    """Publisher category for a benchmark source.

    Drives confidence-N weighting in triangulation and refresh-schedule
    lookup in governance. Values align with `refresh_schedules.yaml`.
    """
    PILLAR3 = "pillar3"
    APRA_ADI = "apra_adi"
    RATING_AGENCY = "rating_agency"
    ICC_TRADE = "icc_trade"
    INDUSTRY_BODY = "industry_body"
    LISTED_PEER = "listed_peer"
    REGULATORY = "regulatory"
    RBA = "rba"
    BUREAU = "bureau"
    INSOLVENCY = "insolvency"


class DataType(str, Enum):
    """Type of risk metric the benchmark value represents."""
    PD = "pd"
    LGD = "lgd"
    DEFAULT_RATE = "default_rate"
    IMPAIRED_RATIO = "impaired_ratio"
    RECOVERY_RATE = "recovery_rate"
    FAILURE_RATE = "failure_rate"
    SUPERVISORY_VALUE = "supervisory_value"


class QualityScore(str, Enum):
    """Analyst-assigned quality tier. Feeds quality-weighted triangulation."""
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class InstitutionType(str, Enum):
    """Selects Stage-2 adjustment chain and confidence discount."""
    BANK = "bank"
    PRIVATE_CREDIT = "private_credit"


class Condition(str, Enum):
    """Economic state a component measurement reflects."""
    NORMAL = "normal"
    DOWNTURN = "downturn"


class Component(str, Enum):
    """LGD decomposition component. None = aggregate LGD / recovery rate."""
    HAIRCUT = "haircut"
    TIME_TO_RECOVERY = "time_to_recovery"
    WORKOUT_COSTS = "workout_costs"
    DISCOUNT_RATE = "discount_rate"


# Data types whose aggregate (component=None) value must lie in [0, 1].
BOUNDED_DATA_TYPES: frozenset[DataType] = frozenset({
    DataType.PD,
    DataType.LGD,
    DataType.DEFAULT_RATE,
    DataType.IMPAIRED_RATIO,
    DataType.RECOVERY_RATE,
    DataType.FAILURE_RATE,
    DataType.SUPERVISORY_VALUE,
})

# Data types for which a component decomposition is meaningful.
COMPONENT_ALLOWED_DATA_TYPES: frozenset[DataType] = frozenset({
    DataType.LGD,
    DataType.RECOVERY_RATE,
})


class BenchmarkEntry(BaseModel):
    """An immutable snapshot of one external benchmark observation.

    Registry rows are versioned via the supersede() pattern — content fields
    are never mutated in place. The model supports both aggregate metrics
    (component=None) and LGD decomposition components (haircut, recovery
    time, workout costs, discount rate).
    """

    model_config = ConfigDict(
        frozen=True,
        str_strip_whitespace=True,
        extra="forbid",
    )

    source_id: str = Field(..., min_length=1)
    publisher: str = Field(..., min_length=1)
    source_type: SourceType
    data_type: DataType
    asset_class: str = Field(..., min_length=1)
    value: float
    value_date: date
    period_years: int = Field(..., ge=1)
    geography: str = Field(..., min_length=1)
    url: str = Field(..., min_length=1)
    retrieval_date: date
    quality_score: QualityScore
    notes: str = ""
    version: int = Field(default=1, ge=1)
    superseded_by: Optional[str] = None
    condition: Optional[Condition] = None
    component: Optional[Component] = None

    @model_validator(mode="after")
    def _validate_dates(self) -> "BenchmarkEntry":
        if self.value_date > self.retrieval_date:
            raise ValueError(
                f"value_date ({self.value_date}) must be on or before "
                f"retrieval_date ({self.retrieval_date})"
            )
        return self

    @model_validator(mode="after")
    def _validate_value_against_component(self) -> "BenchmarkEntry":
        comp = self.component
        dt = self.data_type
        v = self.value

        # Gate: component only permitted for LGD / recovery_rate data_types.
        if comp is not None and dt not in COMPONENT_ALLOWED_DATA_TYPES:
            allowed = sorted(d.value for d in COMPONENT_ALLOWED_DATA_TYPES)
            raise ValueError(
                f"component={comp.value!r} is only allowed when "
                f"data_type in {allowed}; got data_type={dt.value!r}"
            )

        # Aggregate metric (no component): bounded data_types clamped to [0,1].
        if comp is None:
            if dt in BOUNDED_DATA_TYPES and not (0.0 <= v <= 1.0):
                raise ValueError(
                    f"value={v} must be in [0, 1] for "
                    f"data_type={dt.value!r} with no component"
                )
            return self

        # Component-specific numeric ranges.
        if comp in (Component.HAIRCUT, Component.WORKOUT_COSTS):
            if not (0.0 <= v <= 1.0):
                raise ValueError(
                    f"value={v} must be in [0, 1] for component={comp.value!r}"
                )
        elif comp in (Component.TIME_TO_RECOVERY, Component.DISCOUNT_RATE):
            if not v > 0:
                raise ValueError(
                    f"value={v} must be > 0 for component={comp.value!r}"
                )

        return self


# ---------------------------------------------------------------------------
# Adjustment-layer models
# ---------------------------------------------------------------------------

class AdjustmentStep(BaseModel):
    """One multiplier applied during adjustment, plus rationale for the audit trail."""
    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str = Field(..., min_length=1)
    multiplier: float = Field(..., gt=0)
    source_reference: str = ""
    rationale: str = ""


class AdjustmentResult(BaseModel):
    """Output of AdjustmentEngine.adjust(). Chain of steps + final value.

    `scenario_label` defaults to None for persisted adjustments; set to
    'what_if' by the engine when the caller passes overrides, which also
    suppresses DB writes. Persistence guarantee: rows in the `adjustments`
    table always have scenario_label=None.
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    raw_value: float
    adjusted_value: float
    institution_type: InstitutionType
    product: str = Field(..., min_length=1)
    asset_class: str = Field(..., min_length=1)
    steps: list[AdjustmentStep] = Field(default_factory=list)
    final_multiplier: float = Field(..., gt=0)
    scenario_label: Optional[str] = None


# ---------------------------------------------------------------------------
# Triangulation
# ---------------------------------------------------------------------------

class TriangulationResult(BaseModel):
    """Combined benchmark across multiple adjusted sources for one segment.

    `confidence_n` is silently capped at 500 on ingest — the cap belongs to
    the model so every code path producing TriangulationResult honours it,
    not just the triangulator.
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    segment: str = Field(..., min_length=1)
    benchmark_value: float
    confidence_n: int = Field(..., ge=0)
    source_count: int = Field(..., ge=1)
    method: str = Field(..., min_length=1)
    per_source_breakdown: list[dict[str, Any]] = Field(default_factory=list)

    @field_validator("confidence_n", mode="before")
    @classmethod
    def _cap_confidence_at_500(cls, v: Any) -> int:
        return min(int(v), 500)


# ---------------------------------------------------------------------------
# Calibration feed: tagged union with five method-specific variants
# ---------------------------------------------------------------------------

class _CalibrationFeedBase(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    segment: str = Field(..., min_length=1)
    floor_triggered: bool


class CentralTendencyOutput(_CalibrationFeedBase):
    method: Literal["central_tendency"] = "central_tendency"
    external_lra: float = Field(..., ge=0.0)


class LogisticRecalibrationOutput(_CalibrationFeedBase):
    method: Literal["logistic_recalibration"] = "logistic_recalibration"
    target_lra: float = Field(..., ge=0.0)
    confidence_n: int = Field(..., ge=0, le=500)


class BayesianBlendingOutput(_CalibrationFeedBase):
    method: Literal["bayesian_blending"] = "bayesian_blending"
    external_pd: float = Field(..., ge=0.0)
    confidence_n: int = Field(..., ge=0, le=500)


class ExternalBlendingOutput(_CalibrationFeedBase):
    method: Literal["external_blending"] = "external_blending"
    external_lra: float = Field(..., ge=0.0)
    internal_weight: float = Field(..., ge=0.0, le=1.0)


class PlutoTascheOutput(_CalibrationFeedBase):
    method: Literal["pluto_tasche"] = "pluto_tasche"
    external_pd: float = Field(..., ge=0.0)
    role: str = "comparison_only"


CalibrationFeedOutput = Union[
    CentralTendencyOutput,
    LogisticRecalibrationOutput,
    BayesianBlendingOutput,
    ExternalBlendingOutput,
    PlutoTascheOutput,
]


# ---------------------------------------------------------------------------
# Downturn and governance
# ---------------------------------------------------------------------------

class DownturnResult(BaseModel):
    """Output of `downturn.lgd_downturn_uplift()` and decomposition paths.

    `lgd_for_capital` (= downturn_lgd) and `lgd_for_ecl` (= long_run_lgd)
    are kept as separate fields so downstream consumers can pick the right
    value for the right regulatory purpose without re-deriving it.
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    long_run_lgd: float = Field(..., ge=0.0, le=1.0)
    uplift: float = Field(..., gt=0.0)
    downturn_lgd: float = Field(..., ge=0.0, le=1.0)
    product_type: str = Field(..., min_length=1)
    lgd_for_capital: float = Field(..., ge=0.0, le=1.0)
    lgd_for_ecl: float = Field(..., ge=0.0, le=1.0)


class GovernanceReport(BaseModel):
    """Container for any of the six governance report variants."""
    model_config = ConfigDict(frozen=True, extra="forbid")

    report_type: str = Field(..., min_length=1)
    generated_at: datetime
    institution_type: InstitutionType
    findings: list[dict[str, Any]] = Field(default_factory=list)
    flags: list[str] = Field(default_factory=list)
