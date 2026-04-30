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
    BANK_PILLAR3 = "bank_pillar3"            # CBA, NAB, WBC, ANZ, Macquarie
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
    """Regulatory-prescribed PD by slot/grade. APS 113 slotting PD bands
    (Strong/Good/Satisfactory/Weak)."""

    REGULATORY_FLOOR_LGD = "regulatory_floor_lgd"
    """Regulatory-prescribed LGD by slot/grade. APS 113 slotting LGD
    bands and APS 113 minimum LGD floors."""

    QUALITATIVE_COMMENTARY = "qualitative_commentary"
    """Source publishes only qualitative narrative; encoded as a tagged
    text observation with ``value=None`` and the published narrative in
    ``methodology_note``. Qualitas, Metrics."""


class Cohort(str, Enum):
    """Peer-grouping label, derived from ``source_type`` + ``source_id``.

    Used by validation to keep regulatory floors, rating-agency indices,
    and aggregate references out of peer-vs-peer arithmetic. The two
    "peer" cohorts are the only ones that participate in outlier
    detection and the Big-4-vs-non-bank ratio; everything else surfaces
    separately as a reference anchor.
    """

    PEER_BIG4 = "peer_big4"
    """ANZ, CBA, NAB, WBC — IRB-accredited major banks."""

    PEER_OTHER_MAJOR_BANK = "peer_other_major_bank"
    """Macquarie Bank — APRA-classified major bank but not Big 4. Kept
    out of both peer_big4 and peer_non_bank to avoid distorting either
    median; appears as a reference anchor."""

    PEER_NON_BANK = "peer_non_bank"
    """ASX-listed non-bank lenders: Judo, Liberty, Pepper, Resimac,
    Plenti, Wisr, MoneyMe, Qualitas, Metrics, La Trobe."""

    REGULATOR_AGGREGATE = "regulator_aggregate"
    """APRA QPEX / quarterly ADI performance, RBA FSR / SMP / Chart Pack
    aggregates. System-wide, not a peer."""

    RATING_AGENCY = "rating_agency"
    """S&P SPIN / corporate default index, Moody's RMBS performance.
    Composite indices, not a peer."""

    REGULATORY_FLOOR = "regulatory_floor"
    """APS 113 PD / LGD slotting grades and supervisory minima."""

    INDUSTRY_BODY = "industry_body"
    """AFIA, illion BFRI etc. — industry-aggregate references."""


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
        # supervisory LGD floors. APS 113 LGD slotting now uses the
        # dedicated REGULATORY_FLOOR_LGD class — no more PD/LGD
        # collision on REGULATORY_FLOOR_PD.
        DataDefinitionClass.REALISED_LOSS_RATE,
        DataDefinitionClass.REGULATORY_FLOOR_LGD,
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
    # ``value`` is None *only* for parameter='commentary' rows — qualitative
    # observations are stored as tagged narrative with no numeric reading.
    # All other parameters require a value in [0, 1].
    value: Optional[float] = Field(default=None)

    # Reporting basis and methodology
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
        if self.parameter == "commentary":
            if self.value is not None:
                raise ValueError(
                    "value must be None for parameter='commentary' "
                    f"(got {self.value!r}); qualitative observations carry "
                    "no numeric reading"
                )
            if self.data_definition_class is not DataDefinitionClass.QUALITATIVE_COMMENTARY:
                raise ValueError(
                    "parameter='commentary' requires "
                    "data_definition_class=QUALITATIVE_COMMENTARY"
                )
        else:
            if self.value is None:
                raise ValueError(
                    f"value is required for parameter={self.parameter!r}; "
                    "only commentary rows may carry value=None"
                )
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
        if self.reporting_basis == "legacy BenchmarkEntry v1":
            raise ValueError("reporting_basis cannot use the legacy migration placeholder")
        if self.methodology_note.lower().startswith("migrated from"):
            raise ValueError("methodology_note cannot use the migration placeholder")
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


# ---------------------------------------------------------------------------
# Cohort derivation
# ---------------------------------------------------------------------------

_BIG4_HEADS: frozenset[str] = frozenset({"cba", "nab", "wbc", "anz"})
_MACQUARIE_HEADS: frozenset[str] = frozenset({"mqg", "macquarie"})


def cohort_for(source_type: SourceType, source_id: str) -> Cohort:
    """Map a (source_type, source_id) pair to its peer-group cohort.

    Source-id pattern wins over source_type — APS113 floors are filed under
    APRA-family source types but should be treated as regulatory floors;
    Macquarie is filed under BANK_PILLAR3 but should not be Big 4.
    """
    sid = source_id.lower().replace("-", "_")
    head = sid.split("_", 1)[0]

    if "APS113" in source_id.upper() or sid.startswith("aps113_"):
        return Cohort.REGULATORY_FLOOR

    if source_type in (SourceType.BANK_PILLAR3, SourceType.PILLAR3):
        if head in _BIG4_HEADS or sid in _BIG4_HEADS:
            return Cohort.PEER_BIG4
        if head in _MACQUARIE_HEADS or sid.startswith("macquarie_bank_"):
            return Cohort.PEER_OTHER_MAJOR_BANK
        return Cohort.PEER_NON_BANK

    if source_type in (
        SourceType.APRA_PERFORMANCE,
        SourceType.APRA_QPEX,
        SourceType.APRA_ADI,
        SourceType.APRA_NON_ADI,
        SourceType.RBA_AGGREGATE,
        SourceType.RBA,
    ):
        return Cohort.REGULATOR_AGGREGATE

    if source_type in (SourceType.RATING_AGENCY_INDEX, SourceType.RATING_AGENCY):
        return Cohort.RATING_AGENCY

    if source_type == SourceType.REGULATORY:
        return Cohort.REGULATORY_FLOOR

    if source_type in (SourceType.INDUSTRY_BODY, SourceType.BUREAU, SourceType.INSOLVENCY):
        return Cohort.INDUSTRY_BODY

    if source_type in (SourceType.NON_BANK_LISTED, SourceType.LISTED_PEER):
        return Cohort.PEER_NON_BANK

    return Cohort.INDUSTRY_BODY


PEER_COHORTS: frozenset[Cohort] = frozenset({
    Cohort.PEER_BIG4,
    Cohort.PEER_NON_BANK,
})
"""Cohorts that participate in peer-vs-peer arithmetic. Macquarie
(``PEER_OTHER_MAJOR_BANK``) and all reference cohorts are excluded by
design: outlier detection and the Big-4-vs-non-bank ratio compute only
across these two."""
