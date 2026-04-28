"""Pydantic v2 data models for the External Benchmark Engine.

Layering (per plan):
    enums -> BenchmarkEntry (with validators) -> supporting models

This module has no dependencies on db/registry/adjustment layers.
"""
from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


class SourceType(str, Enum):
    """Publisher category for a benchmark source.

    Used by the registry / observations API for source-type filtering and
    by the governance refresh-schedule lookup. Legacy values (PILLAR3,
    APRA_ADI, RATING_AGENCY, INDUSTRY_BODY, LISTED_PEER, REGULATORY, RBA,
    BUREAU, INSOLVENCY) are retained for backward compatibility with
    persisted BenchmarkEntry rows; new RawObservation rows use the
    BANK_PILLAR3 / NON_BANK_LISTED / APRA_* / RATING_AGENCY_INDEX /
    RBA_AGGREGATE family below.
    """
    # ---- legacy (BenchmarkEntry rows) --------------------------------
    PILLAR3 = "pillar3"
    APRA_ADI = "apra_adi"
    RATING_AGENCY = "rating_agency"
    INDUSTRY_BODY = "industry_body"
    LISTED_PEER = "listed_peer"
    REGULATORY = "regulatory"
    RBA = "rba"
    BUREAU = "bureau"
    INSOLVENCY = "insolvency"

    # ---- raw-observation taxonomy (Brief 1) --------------------------
    BANK_PILLAR3 = "bank_pillar3"            # CBA, NAB, WBC, ANZ
    NON_BANK_LISTED = "non_bank_listed"      # Judo, Liberty, Pepper, MoneyMe, etc.
    APRA_QPEX = "apra_qpex"
    APRA_PERFORMANCE = "apra_performance"
    APRA_NON_ADI = "apra_non_adi"            # DEPRECATED 2026-04-28: adapter removed (was stub)
    RATING_AGENCY_INDEX = "rating_agency_index"  # S&P SPIN
    RBA_AGGREGATE = "rba_aggregate"          # RBA Bulletin / FSR aggregates


class DataType(str, Enum):
    """Type of risk metric the benchmark value represents."""
    PD = "pd"
    LGD = "lgd"
    DEFAULT_RATE = "default_rate"
    IMPAIRED_RATIO = "impaired_ratio"
    RECOVERY_RATE = "recovery_rate"
    FAILURE_RATE = "failure_rate"
    SUPERVISORY_VALUE = "supervisory_value"


class DataDefinitionClass(str, Enum):
    """Classification of what a published rate actually represents.

    Surfaces definition heterogeneity to consumers so they can decide
    how to align (or whether to use) each observation. The engine never
    aligns or adjusts; it just labels.
    """

    BASEL_PD_ONE_YEAR = "basel_pd_one_year"
    """Basel-aligned 12-month forward probability of default. Big 4
    Pillar 3, Judo Bank Pillar 3 (since 2019)."""

    ARREARS_30_PLUS_DAYS = "arrears_30_plus_days"
    """Loans 30+ days past due as % of book. S&P SPIN, some Resimac
    disclosures."""

    ARREARS_90_PLUS_DAYS = "arrears_90_plus_days"
    """Loans 90+ days past due as % of book. Pepper Money, Resimac,
    APRA QPEX (most-comparable to PD but earlier in the cycle)."""

    IMPAIRED_LOANS_RATIO = "impaired_loans_ratio"
    """Loans classified as impaired (cumulative, includes restructured).
    Liberty Financial, APRA QPEX. Definition closer to default than PD."""

    NPL_RATIO = "npl_ratio"
    """Non-performing loans (typically 90+ DPD plus impaired). APRA
    quarterly ADI performance."""

    LOSS_EXPENSE_RATE = "loss_expense_rate"
    """Loan loss expense / average book (P&L-driven). Pepper asset
    finance, Liberty. Forward-looking; reflects management provisioning."""

    REALISED_LOSS_RATE = "realised_loss_rate"
    """Backward-looking realised charge-offs / book. La Trobe Financial.
    Useful for LGD calibration, not directly PD."""

    REGULATORY_FLOOR_PD = "regulatory_floor_pd"
    """Regulatory-prescribed PD by slot/grade. APS 113 slotting (Strong/
    Good/Satisfactory/Weak), APS 113 minimum floors."""

    QUALITATIVE_COMMENTARY = "qualitative_commentary"
    """Source publishes only qualitative narrative; encoded as a tagged
    text observation with no numeric value. Qualitas, Metrics. Stored
    with value=0.0 and the commentary in methodology_note."""


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
# Raw observation API — what the engine publishes to consumers
# ---------------------------------------------------------------------------

_ALLOWED_PARAMETERS: frozenset[str] = frozenset({
    "pd", "lgd", "arrears", "impaired", "npl", "loss_rate", "commentary",
})

_PARAMETER_TO_DEFINITION_CLASSES: dict[str, frozenset[DataDefinitionClass]] = {
    "pd": frozenset({
        DataDefinitionClass.BASEL_PD_ONE_YEAR,
        DataDefinitionClass.REGULATORY_FLOOR_PD,
    }),
    "lgd": frozenset({
        # LGD parameters can carry realised loss rates (back-looking) or
        # supervisory floors when published as LGD; the engine just labels.
        DataDefinitionClass.REALISED_LOSS_RATE,
        DataDefinitionClass.REGULATORY_FLOOR_PD,  # APS 113 LGD slotting reuses the floor class
    }),
    "arrears": frozenset({
        DataDefinitionClass.ARREARS_30_PLUS_DAYS,
        DataDefinitionClass.ARREARS_90_PLUS_DAYS,
    }),
    "impaired": frozenset({DataDefinitionClass.IMPAIRED_LOANS_RATIO}),
    "npl": frozenset({DataDefinitionClass.NPL_RATIO}),
    "loss_rate": frozenset({
        DataDefinitionClass.LOSS_EXPENSE_RATE,
        DataDefinitionClass.REALISED_LOSS_RATE,
    }),
    "commentary": frozenset({DataDefinitionClass.QUALITATIVE_COMMENTARY}),
}


class RawObservation(BaseModel):
    """A single raw PD or LGD observation from one source for one segment.

    The engine publishes these directly. No adjustment, no blending, no
    triangulation — the source's published value with its full attribution.
    Consumers (PD workbook, LGD project, stress testing) read these via
    src.observations.PeerObservations and apply their own adjustments.

    `data_definition_class` makes source heterogeneity machine-readable:
    Big 4 publish Basel PDs, APRA QPEX publishes impaired ratios, S&P SPIN
    publishes 30+DPD arrears, Qualitas publishes only commentary, etc.
    Consumers filter on this field rather than parsing methodology notes.
    """

    model_config = ConfigDict(frozen=True, str_strip_whitespace=True, extra="forbid")

    source_id: str = Field(..., min_length=1)         # e.g. "cba", "judo", "liberty"
    source_type: SourceType
    segment: str = Field(..., min_length=1)           # canonical segment ID
    product: Optional[str] = None                     # finer granularity if available
    parameter: str = Field(..., min_length=1)         # broad category (see _ALLOWED_PARAMETERS)
    data_definition_class: DataDefinitionClass        # precise definition the source publishes
    value: float = Field(..., ge=0.0)                 # raw published value (decimal)

    # Vintage and methodology
    as_of_date: date
    reporting_basis: str = Field(..., min_length=1)   # e.g. "Pillar 3 quarterly"
    methodology_note: str = Field(..., min_length=1)  # what the source says this means

    # Optional metadata
    sample_size_n: Optional[int] = Field(default=None, ge=0)
    period_start: Optional[date] = None
    period_end: Optional[date] = None
    source_url: Optional[str] = None
    page_or_table_ref: Optional[str] = None           # e.g. "Pillar 3 Table CR6 row 4"

    @model_validator(mode="after")
    def _validate_period_bounds(self) -> "RawObservation":
        if (
            self.period_start is not None
            and self.period_end is not None
            and self.period_start > self.period_end
        ):
            raise ValueError(
                f"period_start ({self.period_start}) must be on or before "
                f"period_end ({self.period_end})"
            )
        return self

    @model_validator(mode="after")
    def _validate_parameter(self) -> "RawObservation":
        if self.parameter not in _ALLOWED_PARAMETERS:
            raise ValueError(
                f"parameter must be one of {sorted(_ALLOWED_PARAMETERS)}; "
                f"got {self.parameter!r}"
            )
        # Numeric values bounded [0, 1]; commentary uses value=0.0 by convention.
        if self.parameter != "commentary":
            if not 0.0 <= self.value <= 1.0:
                raise ValueError(
                    f"value={self.value} must be in [0, 1] for "
                    f"parameter={self.parameter!r}"
                )
        return self

    @model_validator(mode="after")
    def _validate_parameter_definition_consistency(self) -> "RawObservation":
        valid = _PARAMETER_TO_DEFINITION_CLASSES.get(self.parameter)
        if valid is not None and self.data_definition_class not in valid:
            raise ValueError(
                f"data_definition_class={self.data_definition_class.value!r} is "
                f"not valid for parameter={self.parameter!r}; "
                f"valid: {sorted(c.value for c in valid)}"
            )
        return self


# ---------------------------------------------------------------------------
# Governance report container
# ---------------------------------------------------------------------------

class GovernanceReport(BaseModel):
    """Container for any of the six governance report variants."""
    model_config = ConfigDict(frozen=True, extra="forbid")

    report_type: str = Field(..., min_length=1)
    generated_at: datetime
    institution_type: InstitutionType
    findings: list[dict[str, Any]] = Field(default_factory=list)
    flags: list[str] = Field(default_factory=list)
