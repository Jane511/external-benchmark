"""Big-4 Pillar 3 aggregator — Phase 3.C.

Pipeline:

    per-bank rows  --(intra_bank_industry_totals)-->  bank-industry totals
                   --(inter_bank_aggregate)-->        canonical-bucket aggregates
                   --(compute_coverage_ratios)-->     coverage diagnostic rows

The module is a *consumer* of the per-bank parsers in
``ingestion/adapters/`` and the RBA D14.1 adapter — it does not parse
PDFs or XLSXes itself.

Refusal taxonomy (§6 of the Phase 3.C plan):

- :class:`CrossBankAggregationRefusedError` — PD aggregation,
  total-consumer aggregation, mismatched ``provision_basis`` summing
- :class:`IncompatiblePeriodLengthError` — write-off summing across
  rows with different ``period_length_months`` values
- :class:`MissingProvenanceError` — input row lacks required
  provenance metadata; this is a contract violation by an upstream
  adapter, not a runtime data problem

The harmonisation map's
:class:`ingestion.adapters.anzsic_harmonisation.UnknownIndustryLabelError`
may also surface here as defence-in-depth — a label that fails to
resolve is a parser contract violation; the aggregator does not paper
over it.

Output schema for aggregated rows
---------------------------------

Each aggregated row carries:

- ``canonical_bucket`` — one of the 13 ANZSIC canonical buckets or one
  of the three consumer buckets
- ``metric`` — the metric name (e.g. ``exposure_aud_m``)
- ``value_aud_m`` — sum across contributing banks (or null if any
  contributing bank's value is null with ``null_reason``)
- ``contributing_banks`` — sorted tuple of bank codes that contributed
- ``bank_as_of_dates`` — manifest dict ``{bank_code: as_of_date}``
- ``aggregate_as_of_date_strategy`` — currently always
  ``most_recent_per_bank``
- ``as_of_date`` — most-recent contributing bank date (for sorting /
  filtering convenience; the manifest is the source of truth)
- ``aggregate_period_length_months`` — for write-off rows, 6 or 12
- ``derived_from_intra_bank_sum`` — True when intra-bank sum is the
  source of truth (no published bank-industry total to validate against)
- ``null_reason`` — populated when ``value_aud_m`` is null (e.g.
  ``partial_redaction_in_subrows``)
- ``coverage_ratio_caveat`` — populated on coverage diagnostic rows
  only (see :func:`compute_coverage_ratios`)
- ``low_coverage_flag`` — populated on coverage diagnostic rows only

Refusal-event log lifecycle (Phase 3.D recovery Step 5.1 / §B.5)
---------------------------------------------------------------

The module-level ``_REFUSAL_LOG`` accumulates structured events across
the process lifetime; callers needing per-run isolation should call
:func:`reset_refusal_log` between runs.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from ingestion.adapters.anzsic_harmonisation import (
    UnknownIndustryLabelError,
    is_business_lending,
    resolve,
    resolve_rba_d14_1,
)
from ingestion.adapters.pillar3_industry_schema import (
    COL_GROSS_CARRYING_COMPONENT,
    COL_PORTFOLIO_TYPE,
    COL_PROVISION_BASIS,
    METRIC_EXPOSURE,
    METRIC_NPE,
    METRIC_PROVISIONS,
    METRIC_WRITE_OFFS,
    PROVISION_BASIS_VALUES,
)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RefusalEvent:
    """Structured aggregation refusal emitted into the refusal log."""

    refusal_type: str
    requested_aggregation: str
    rule_violated: str
    phase_ruling_reference: str
    as_of_date: str


# ---------------------------------------------------------------------------
# Refusal-event log — Phase 3.D recovery Step 5.1 (§B.5)
# ---------------------------------------------------------------------------
#
# Module-level collector. Each refusal raise-site appends a structured
# RefusalEvent before raising. The reporting layer retrieves the log via
# get_refusal_log() and renders it in the refusal-log section. Callers
# scoping a single report run call reset_refusal_log() first.

_REFUSAL_LOG: list[RefusalEvent] = []


def _emit_refusal(
    *,
    refusal_type: str,
    requested_aggregation: str,
    rule_violated: str,
    phase_ruling_reference: str,
    as_of_date: object = None,
) -> None:
    _REFUSAL_LOG.append(RefusalEvent(
        refusal_type=refusal_type,
        requested_aggregation=requested_aggregation,
        rule_violated=rule_violated,
        phase_ruling_reference=phase_ruling_reference,
        as_of_date=str(as_of_date) if as_of_date is not None else date.today().isoformat(),
    ))


def get_refusal_log() -> tuple[RefusalEvent, ...]:
    """Return refusal events emitted since the last reset, in order."""
    return tuple(_REFUSAL_LOG)


def reset_refusal_log() -> None:
    """Clear the refusal-event collector. Call before scoping a run."""
    _REFUSAL_LOG.clear()


# ---------------------------------------------------------------------------
# Errors — refusal taxonomy
# ---------------------------------------------------------------------------


class CrossBankAggregationRefusedError(ValueError):
    """Refused: a cross-bank aggregation that violates a documented rule.

    Specifically:

    - PD aggregation across banks (Phase 2 §5.4 — banks publish on
      different IRB master scales; cross-bank weighted-average PD is
      definitionally meaningless)
    - "Big-4 total consumer" view across the three consumer buckets
      (Phase 3.B.3 ruling Q4 — the buckets are definitionally distinct
      populations)
    - Provision summing across banks with different
      ``provision_basis`` (Phase 3.B.3 hand-off — APS 220 specific
      provision and AASB 9 Stage 3 ECL are not summable)
    """


class IncompatiblePeriodLengthError(ValueError):
    """Refused: write-off summing across mismatched period lengths.

    Phase 2 Issue 4 ruling — CBA publishes both 6-month and 12-month
    actual losses; WBC publishes 12-month rolling. Aggregation must
    not silently sum a half-year onto a full-year figure.
    """


class MissingProvenanceError(ValueError):
    """Aggregator input row lacks required provenance metadata.

    Contract violation by an upstream adapter, not a runtime data
    issue. Surfaces here as defence-in-depth.
    """


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LOW_COVERAGE_THRESHOLD: float = 0.60  # Phase 2 §5.3 ruling

# Metrics that may be summed across banks.
_AGGREGATABLE_METRICS: frozenset[str] = frozenset({
    METRIC_EXPOSURE, METRIC_NPE, METRIC_WRITE_OFFS,
})

# Canonical consumer-bucket keys (used to enforce the §3.6 refusal).
_CONSUMER_BUCKETS: frozenset[str] = frozenset({
    "consumer_lending_personal",
    "consumer_lending_residential_mortgage",
    "consumer_combined",
})


# Dimension columns that exist on per-bank rows for some banks. Intra-
# bank aggregation drops these (collapsed to the industry level).
_SUBROW_DIMENSIONS: tuple[str, ...] = (
    COL_GROSS_CARRYING_COMPONENT,
    COL_PORTFOLIO_TYPE,
)


# Required provenance columns on every input row to the aggregator.
_REQUIRED_PROVENANCE: frozenset[str] = frozenset({
    "data_source", "bank_code", "as_of_date", "industry_published",
    "metric", "value_aud_m", "source_publication", "source_table_ref",
})


# ---------------------------------------------------------------------------
# Intra-bank aggregation — collapse sub-row dimensions
# ---------------------------------------------------------------------------


def intra_bank_industry_totals(bank_rows: pd.DataFrame) -> pd.DataFrame:
    """Collapse per-bank rows to bank-industry-metric level.

    Sums ``value_aud_m`` across sub-row dimensions (``portfolio_type``
    for CBA, ``gross_carrying_component`` for ANZ exposures).
    NAB / WBC pass through.

    Null propagation: if any sub-row in a group has a null
    ``value_aud_m`` (because the bank published a dash that the parser
    interpreted as redacted), the intra-bank sum is null with
    ``null_reason='partial_redaction_in_subrows'``. This protects
    downstream consumers from a falsely-low sum.

    Output: same schema as input, with sub-row dimension columns
    dropped, plus a new boolean ``derived_from_intra_bank_sum`` column
    that is True iff the row is the result of an intra-bank sum (vs a
    pass-through).
    """
    _validate_provenance(bank_rows)

    # Group keys: everything that uniquely identifies a bank-industry-
    # metric-period row. Geography is preserved (WBC publishes per-geog
    # NPE/provision rows; the aggregator preserves that — inter-bank
    # aggregation collapses by geography later if requested).
    group_keys = [
        "bank_code", "industry_published", "metric", "as_of_date",
        "geography", "period_length_months",
    ]
    # Include provision_basis and any present subrow dimensions in the
    # output but not in grouping (we want to preserve provision_basis
    # value, which is invariant within a group).
    has_subrows = any(c in bank_rows.columns for c in _SUBROW_DIMENSIONS)
    if not has_subrows:
        # Pass-through; just add the derived flag.
        out = bank_rows.copy()
        out["derived_from_intra_bank_sum"] = False
        out["null_reason"] = _null_reason_from_redaction(out)
        return out

    out_rows: list[dict[str, Any]] = []
    grouped = bank_rows.groupby(
        group_keys, dropna=False, as_index=False, sort=False,
    )
    for keys, group in grouped:
        any_null = group["value_aud_m"].isna().any()
        if any_null:
            value: float | None = None
            null_reason: str | None = "partial_redaction_in_subrows"
        else:
            value = float(group["value_aud_m"].sum())
            null_reason = None

        # Carry over the first row's metadata as the representative.
        rep = group.iloc[0].to_dict()
        for col in _SUBROW_DIMENSIONS:
            rep.pop(col, None)
        rep["value_aud_m"] = value
        rep["redaction_reason"] = (
            None if value is not None else _carry_redaction_reason(group)
        )
        rep["derived_from_intra_bank_sum"] = (len(group) > 1)
        rep["null_reason"] = null_reason
        out_rows.append(rep)

    out = pd.DataFrame.from_records(out_rows)
    return out


def _carry_redaction_reason(group: pd.DataFrame) -> str | None:
    """If the whole group is null, surface a representative redaction reason."""
    reasons = group["redaction_reason"].dropna().unique()
    if len(reasons) == 0:
        return None
    return str(reasons[0])


def _null_reason_from_redaction(df: pd.DataFrame) -> pd.Series:
    """For pass-through rows, set null_reason from existing redaction_reason."""
    out = pd.Series([None] * len(df), index=df.index, dtype=object)
    null_mask = df["value_aud_m"].isna()
    if null_mask.any():
        out.loc[null_mask] = df.loc[null_mask, "redaction_reason"]
    return out


def _validate_provenance(df: pd.DataFrame) -> None:
    missing_cols = _REQUIRED_PROVENANCE - set(df.columns)
    if missing_cols:
        _emit_refusal(
            refusal_type="MissingProvenanceError",
            requested_aggregation="intra_bank_industry_totals",
            rule_violated=(
                f"input rows missing required provenance columns: "
                f"{sorted(missing_cols)}"
            ),
            phase_ruling_reference="Phase 3.C aggregator contract",
        )
        raise MissingProvenanceError(
            f"input rows missing required provenance columns: "
            f"{sorted(missing_cols)}; aggregator contract requires "
            f"{sorted(_REQUIRED_PROVENANCE)}"
        )


# ---------------------------------------------------------------------------
# Inter-bank aggregation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _BankContribution:
    bank_code: str
    as_of_date: date
    value: float | None
    provision_basis: str | None


def inter_bank_aggregate(
    bank_industry_totals: pd.DataFrame,
    metric: str,
    *,
    consumer_view: str | None = None,
    refuse_total_consumer: bool = True,
    period_length_months: int | None = None,
) -> pd.DataFrame:
    """Aggregate bank-industry totals to canonical-bucket rows.

    Parameters
    ----------
    bank_industry_totals
        Output of :func:`intra_bank_industry_totals` for one or more
        banks. Multi-bank in is the typical case.
    metric
        One of :data:`_AGGREGATABLE_METRICS`. Other metrics raise
        :class:`CrossBankAggregationRefusedError` (this is the entry
        point for the PD-aggregation refusal).
    consumer_view
        ``None`` (default) — exclude all consumer buckets from output.
        ``"personal"`` — include only ``consumer_lending_personal``.
        ``"residential_mortgage"`` — include only
        ``consumer_lending_residential_mortgage``.
        ``"combined"`` — include only ``consumer_combined``.
        Per Phase 3.B.3 Q4: there is no value of this parameter that
        produces a single "Big-4 total consumer" aggregate. The three
        consumer buckets are emitted separately or not at all.
    refuse_total_consumer
        Safety catch. If True (default) and the input rows include
        more than one consumer bucket and ``consumer_view`` is not set
        to one of the three valid values, raises
        :class:`CrossBankAggregationRefusedError`.
    period_length_months
        For write-off aggregation: filter input rows to a specific
        period length (6 or 12). Mismatched-period summing raises
        :class:`IncompatiblePeriodLengthError`. Required when
        ``metric == 'write_offs_aud_m'``.

    Returns
    -------
    pd.DataFrame
        One row per (canonical_bucket, metric) with
        ``contributing_banks``, ``bank_as_of_dates``,
        ``aggregate_as_of_date_strategy='most_recent_per_bank'``, and
        ``value_aud_m`` as the sum across contributing banks.
    """
    if metric not in _AGGREGATABLE_METRICS:
        _emit_refusal(
            refusal_type="CrossBankAggregationRefusedError",
            requested_aggregation=f"inter_bank_aggregate(metric={metric!r})",
            rule_violated=(
                "metric is not aggregatable across banks; "
                "PD aggregation refused (different IRB master scales) and "
                "provisions must use aggregate_provisions_same_basis()"
            ),
            phase_ruling_reference="Phase 2 §5.4",
        )
        raise CrossBankAggregationRefusedError(
            f"metric {metric!r} is not aggregatable across banks. "
            f"Allowed metrics: {sorted(_AGGREGATABLE_METRICS)}. "
            f"PD aggregation is refused per Phase 2 §5.4 — banks "
            f"publish PDs on different IRB master scales."
        )

    df = bank_industry_totals[bank_industry_totals["metric"] == metric].copy()
    if df.empty:
        return pd.DataFrame()

    # Resolve canonical buckets for every input row.
    df["canonical_bucket"] = [
        resolve(b, lab)
        for b, lab in zip(df["bank_code"], df["industry_published"])
    ]

    # Apply consumer-view filter.
    df = _apply_consumer_view_filter(
        df, consumer_view=consumer_view,
        refuse_total_consumer=refuse_total_consumer,
    )

    if metric == METRIC_WRITE_OFFS:
        if period_length_months is None:
            _emit_refusal(
                refusal_type="IncompatiblePeriodLengthError",
                requested_aggregation=(
                    "inter_bank_aggregate(metric='write_offs_aud_m', "
                    "period_length_months=None)"
                ),
                rule_violated=(
                    "write-off aggregation requires explicit "
                    "period_length_months (6 or 12); silently mixing "
                    "CBA half-year with WBC 12-month rolling is refused"
                ),
                phase_ruling_reference="Phase 2 Issue 4",
            )
            raise IncompatiblePeriodLengthError(
                "write-off aggregation requires period_length_months "
                "(6 or 12) — refuse to silently mix CBA half-year "
                "with WBC 12-month rolling figures (Phase 2 Issue 4)."
            )
        df = _filter_period_length(df, period_length_months)
    else:
        # most_recent_per_bank strategy: each bank contributes its
        # most-recent-available row per (bank, canonical_bucket). For
        # CBA which publishes Jun-25 + Dec-24, this keeps Jun-25 only.
        df = _keep_most_recent_per_bank_bucket(df)

    # Provision-basis check happens upstream of this function (we don't
    # accept METRIC_PROVISIONS in _AGGREGATABLE_METRICS); see
    # aggregate_provisions_same_basis() for the documented entry point.

    out_rows: list[dict[str, Any]] = []
    for canon, group in df.groupby("canonical_bucket", sort=True):
        contributing = _summarise_contributions(group)
        # Sum values; if any contributing bank's value is null, the
        # aggregate is null with the contributing bank's null_reason.
        any_null = group["value_aud_m"].isna().any()
        if any_null:
            value: float | None = None
            null_reason = "contributing_bank_null"
        else:
            value = float(group["value_aud_m"].sum())
            null_reason = None

        bank_dates = {
            bc: max(g["as_of_date"])
            for bc, g in group.groupby("bank_code")
        }
        most_recent = max(bank_dates.values())

        out_row: dict[str, Any] = {
            "canonical_bucket": canon,
            "metric": metric,
            "value_aud_m": value,
            "contributing_banks": tuple(sorted(bank_dates.keys())),
            "bank_as_of_dates": dict(bank_dates),
            "aggregate_as_of_date_strategy": "most_recent_per_bank",
            "as_of_date": most_recent,
            "aggregate_period_length_months": (
                period_length_months if metric == METRIC_WRITE_OFFS else None
            ),
            "derived_from_intra_bank_sum": bool(
                group["derived_from_intra_bank_sum"].any()
                if "derived_from_intra_bank_sum" in group.columns else False
            ),
            "null_reason": null_reason,
            "coverage_ratio_caveat": None,
            "low_coverage_flag": None,
        }
        out_rows.append(out_row)

    return pd.DataFrame.from_records(out_rows)


def _apply_consumer_view_filter(
    df: pd.DataFrame, *, consumer_view: str | None,
    refuse_total_consumer: bool,
) -> pd.DataFrame:
    consumer_present = df["canonical_bucket"].isin(_CONSUMER_BUCKETS)
    if not consumer_present.any():
        return df

    if consumer_view is None:
        # Drop consumer rows; only business lending aggregated.
        return df[~consumer_present].copy()

    valid = {
        "personal":              "consumer_lending_personal",
        "residential_mortgage":  "consumer_lending_residential_mortgage",
        "combined":              "consumer_combined",
    }
    if consumer_view not in valid:
        raise ValueError(
            f"consumer_view must be one of {sorted(valid)}; got "
            f"{consumer_view!r}"
        )
    if refuse_total_consumer:
        # Only one consumer bucket allowed in the output. Any attempt
        # to retain multiple consumer buckets in a single aggregation
        # call is the disallowed total-consumer view.
        kept_bucket = valid[consumer_view]
        keep_mask = (~consumer_present) | (df["canonical_bucket"] == kept_bucket)
        # If consumer_view is set, the user is explicitly working with
        # consumer rows — drop business rows from this call's output to
        # keep the semantics single-purpose.
        return df[df["canonical_bucket"] == kept_bucket].copy()
    return df  # pragma: no cover — refuse_total_consumer=False is unsupported


def _keep_most_recent_per_bank_bucket(df: pd.DataFrame) -> pd.DataFrame:
    """Keep all rows from each ``(bank_code, canonical_bucket)`` whose
    ``as_of_date`` equals that group's maximum.

    Implements the ``most_recent_per_bank`` strategy at the
    inter-bank level. WBC publishes per-geography rows for the same
    (industry, period); ANZ may have multiple sub-row dimensions
    pre-collapse — both should retain ALL rows on the most-recent
    date so the subsequent inter-bank groupby-sum captures every
    contribution. Only earlier-period rows (e.g. CBA Dec-24 when
    Jun-25 also exists) are filtered out.
    """
    if df.empty:
        return df
    max_dates = (
        df.groupby(["bank_code", "canonical_bucket"])["as_of_date"]
          .transform("max")
    )
    return df[df["as_of_date"] == max_dates].copy()


def _filter_period_length(df: pd.DataFrame, period_length: int) -> pd.DataFrame:
    if period_length not in (6, 12):
        _emit_refusal(
            refusal_type="IncompatiblePeriodLengthError",
            requested_aggregation=(
                f"_filter_period_length(period_length={period_length!r})"
            ),
            rule_violated="period_length_months must be 6 or 12",
            phase_ruling_reference="Phase 2 Issue 4",
        )
        raise IncompatiblePeriodLengthError(
            f"period_length_months must be 6 or 12; got {period_length!r}"
        )
    out = df[df["period_length_months"] == period_length].copy()
    return out


def _summarise_contributions(group: pd.DataFrame) -> list[_BankContribution]:
    contributions: list[_BankContribution] = []
    for bc, sub in group.groupby("bank_code"):
        for _, row in sub.iterrows():
            contributions.append(_BankContribution(
                bank_code=str(bc),
                as_of_date=row["as_of_date"],
                value=(
                    None if pd.isna(row["value_aud_m"])
                    else float(row["value_aud_m"])
                ),
                provision_basis=row.get(COL_PROVISION_BASIS),
            ))
    return contributions


def aggregate_provisions_same_basis(
    bank_industry_totals: pd.DataFrame,
    *,
    provision_basis: str,
    consumer_view: str | None = None,
) -> pd.DataFrame:
    """Sum provisions across banks that share the same ``provision_basis``.

    Cross-basis summing raises
    :class:`CrossBankAggregationRefusedError`. APS 220 (CBA-only) and
    AASB 9 Stage 3 ECL (WBC + NAB + ANZ) cannot be aggregated together.

    Use this function rather than :func:`inter_bank_aggregate` for
    provisions — the latter refuses provisions outright to make the
    basis-mismatch check unmissable at the call site.
    """
    if provision_basis not in PROVISION_BASIS_VALUES:
        _emit_refusal(
            refusal_type="CrossBankAggregationRefusedError",
            requested_aggregation=(
                f"aggregate_provisions_same_basis("
                f"provision_basis={provision_basis!r})"
            ),
            rule_violated=(
                f"provision_basis must be one of "
                f"{sorted(PROVISION_BASIS_VALUES)}"
            ),
            phase_ruling_reference="Phase 3.B.3 hand-off",
        )
        raise CrossBankAggregationRefusedError(
            f"provision_basis must be one of "
            f"{sorted(PROVISION_BASIS_VALUES)}; got {provision_basis!r}"
        )

    df = bank_industry_totals[
        bank_industry_totals["metric"] == METRIC_PROVISIONS
    ].copy()
    if df.empty:
        return pd.DataFrame()

    bases = set(df[COL_PROVISION_BASIS].dropna().unique())
    if len(bases) > 1:
        _emit_refusal(
            refusal_type="CrossBankAggregationRefusedError",
            requested_aggregation=(
                "aggregate_provisions_same_basis(...) on mixed-basis input"
            ),
            rule_violated=(
                f"input mixes provision_basis values {sorted(bases)}; "
                f"APS 220 specific and AASB 9 Stage 3 ECL are not summable"
            ),
            phase_ruling_reference="Phase 3.B.3 hand-off",
        )
        raise CrossBankAggregationRefusedError(
            f"provision aggregation refused: input rows mix "
            f"provision_basis values {sorted(bases)} — APS 220 "
            f"specific and AASB 9 Stage 3 ECL are not summable "
            f"(Phase 3.B.3 hand-off). Filter input rows to a single "
            f"basis before calling, or call separately for each basis."
        )
    if bases and provision_basis not in bases:
        _emit_refusal(
            refusal_type="CrossBankAggregationRefusedError",
            requested_aggregation=(
                f"aggregate_provisions_same_basis("
                f"provision_basis={provision_basis!r})"
            ),
            rule_violated=(
                f"requested basis {provision_basis!r} not present in "
                f"input rows (carry {sorted(bases)})"
            ),
            phase_ruling_reference="Phase 3.B.3 hand-off",
        )
        raise CrossBankAggregationRefusedError(
            f"provision aggregation refused: requested basis "
            f"{provision_basis!r} but input rows carry "
            f"{sorted(bases)}"
        )

    df = df[df[COL_PROVISION_BASIS] == provision_basis].copy()
    df["canonical_bucket"] = [
        resolve(b, lab)
        for b, lab in zip(df["bank_code"], df["industry_published"])
    ]
    df = _apply_consumer_view_filter(
        df, consumer_view=consumer_view, refuse_total_consumer=True,
    )
    df = _keep_most_recent_per_bank_bucket(df)

    out_rows: list[dict[str, Any]] = []
    for canon, group in df.groupby("canonical_bucket", sort=True):
        any_null = group["value_aud_m"].isna().any()
        bank_dates = {
            bc: max(g["as_of_date"])
            for bc, g in group.groupby("bank_code")
        }
        most_recent = max(bank_dates.values())
        out_rows.append({
            "canonical_bucket": canon,
            "metric": METRIC_PROVISIONS,
            "provision_basis": provision_basis,
            "value_aud_m": None if any_null else float(group["value_aud_m"].sum()),
            "contributing_banks": tuple(sorted(bank_dates.keys())),
            "bank_as_of_dates": dict(bank_dates),
            "aggregate_as_of_date_strategy": "most_recent_per_bank",
            "as_of_date": most_recent,
            "aggregate_period_length_months": None,
            "derived_from_intra_bank_sum": bool(
                group["derived_from_intra_bank_sum"].any()
                if "derived_from_intra_bank_sum" in group.columns else False
            ),
            "null_reason": "contributing_bank_null" if any_null else None,
            "coverage_ratio_caveat": None,
            "low_coverage_flag": None,
        })
    return pd.DataFrame.from_records(out_rows)


# ---------------------------------------------------------------------------
# Coverage ratios — Big-4 EAD vs RBA D14.1 stocks
# ---------------------------------------------------------------------------


def compute_coverage_ratios(
    big4_aggregates: pd.DataFrame,
    d14_1: pd.DataFrame,
) -> pd.DataFrame:
    """Compute per-canonical-bucket coverage ratios.

    Inputs:

    - ``big4_aggregates``: output of :func:`inter_bank_aggregate` for
      ``METRIC_EXPOSURE`` (i.e. Big-4 EAD per canonical bucket).
    - ``d14_1``: rows from :class:`RbaD14_1Adapter` ``normalise()``.
      Construction is taken from the synthesised "Construction
      (total)" row; ANZSIC sub-rows for Construction are intentionally
      not used (matches Big-4 single-bucket convention from Phase 2
      Issue 7).

    Caveats are surfaced in ``coverage_ratio_caveat`` (semicolon-
    separated):

    - ``ead_vs_outstanding_soft_proxy`` — always (per Phase 2 Issue 9)
    - ``cba_aps120_scope_mismatch`` — when the numerator is NPE-based
      and CBA is a contributing bank (handled separately by
      :func:`flag_aps120_caveat_for_npe_aggregates`)
    - ``d14_1_lender_scope_banks_and_rfcs_only`` — always (per Phase 2
      Issue 9)
    - ``d14_1_series_break_<vintage>`` — when the D14.1 row crosses a
      vintage boundary
    - ``d14_1_suppressed`` — when the D14.1 cell is suppressed (Phase 2
      Issue 10); coverage ratio is null with this caveat.
    """
    # Sum D14.1 to canonical buckets. Use Construction (total) only.
    d14 = _aggregate_d14_1_to_canonical_buckets(d14_1)

    out_rows: list[dict[str, Any]] = []
    for _, big4 in big4_aggregates.iterrows():
        canon = big4["canonical_bucket"]
        d14_match = d14[d14["canonical_bucket"] == canon]
        if d14_match.empty:
            d14_value: float | None = None
            d14_pub_date: date | None = None
            extra_caveats: list[str] = ["d14_1_no_matching_bucket"]
        else:
            row = d14_match.iloc[0]
            d14_value = row["d14_1_outstanding_aud_m"]
            d14_pub_date = row["d14_1_publication_date"]
            extra_caveats = list(row["caveats"]) if row["caveats"] else []

        big4_ead = big4["value_aud_m"]
        if pd.isna(big4_ead) or pd.isna(d14_value) or d14_value in (0.0, None):
            coverage = None
        else:
            coverage = float(big4_ead) / float(d14_value)

        caveats = [
            "ead_vs_outstanding_soft_proxy",
            "d14_1_lender_scope_banks_and_rfcs_only",
        ] + extra_caveats
        if d14_value is None:
            caveats.append("d14_1_suppressed")

        low_flag = (
            coverage is not None and coverage < LOW_COVERAGE_THRESHOLD
        )

        out_rows.append({
            "canonical_bucket": canon,
            "as_of_date_big4": big4["as_of_date"],
            "as_of_date_d14_1": d14_pub_date,
            "big4_ead_aud_m": (
                None if pd.isna(big4_ead) else float(big4_ead)
            ),
            "d14_1_outstanding_aud_m": d14_value,
            "coverage_ratio": coverage,
            "coverage_ratio_caveat": "; ".join(caveats),
            "low_coverage_flag": low_flag,
            "contributing_banks": big4["contributing_banks"],
            "bank_as_of_dates": big4["bank_as_of_dates"],
        })
    return pd.DataFrame.from_records(out_rows)


def _aggregate_d14_1_to_canonical_buckets(d14_1: pd.DataFrame) -> pd.DataFrame:
    """Sum D14.1 outstanding to canonical buckets.

    Uses Construction (total) synthesised row; excludes the three
    Construction sub-rows from the sum (avoids double-counting).
    Suppressed cells produce null ``d14_1_outstanding_aud_m`` with a
    ``d14_1_suppressed`` caveat.
    """
    df = d14_1.copy()

    # Use D14.1's synthesised "Construction (total)" row per Phase 2
    # Issue 7 ruling — drop the 3 sub-rows to avoid double-counting.
    construction_subrows = {
        "Residential building construction",
        "Non-residential building construction",
        "Other construction",
    }
    df = df[~df["industry_published"].isin(construction_subrows)]

    # Drop the universe-total row.
    df = df[df["industry_published"] != "Total, excluding selected financial businesses"]

    # Sum across all business sizes per industry per latest period.
    latest_date = df["as_of_date"].max()
    df = df[df["as_of_date"] == latest_date]

    # Resolve to canonical buckets.
    out: dict[str, dict[str, Any]] = {}
    for _, row in df.iterrows():
        try:
            canon = resolve_rba_d14_1(row["industry_published"])
        except UnknownIndustryLabelError:
            # Universe total / unmapped — skip silently here; the
            # coverage caller will report no-matching-bucket if a Big-4
            # bucket has no D14.1 counterpart.
            continue
        bucket = out.setdefault(canon, {
            "canonical_bucket": canon,
            "values": [],
            "any_suppressed": False,
            "any_break_flag": set(),
            "publication_date": None,
        })
        if pd.isna(row["value_aud_m"]):
            bucket["any_suppressed"] = True
        else:
            bucket["values"].append(float(row["value_aud_m"]))
        bucket["any_break_flag"].add(row["series_break_flag"])
        if bucket["publication_date"] is None:
            bucket["publication_date"] = row["source_publication_date"]

    out_rows: list[dict[str, Any]] = []
    for canon, b in out.items():
        if b["any_suppressed"]:
            value = None
        else:
            value = sum(b["values"]) if b["values"] else None
        caveats: list[str] = []
        for flag in sorted(b["any_break_flag"]):
            caveats.append(f"d14_1_series_break_{flag}")
        out_rows.append({
            "canonical_bucket": canon,
            "d14_1_outstanding_aud_m": value,
            "d14_1_publication_date": b["publication_date"],
            "caveats": caveats,
        })
    if not out_rows:
        return pd.DataFrame(columns=[
            "canonical_bucket", "d14_1_outstanding_aud_m",
            "d14_1_publication_date", "caveats",
        ])
    return pd.DataFrame.from_records(out_rows)


def flag_aps120_caveat_for_npe_aggregates(
    npe_aggregates: pd.DataFrame,
) -> pd.DataFrame:
    """Append ``cba_aps120_scope_mismatch`` to NPE-based rows that
    include CBA in ``contributing_banks``.

    Per Phase 3.B.3 hand-off: CBA NPE excludes APS 120 securitisation
    entities; D14.1 (and other banks' NPE) does not. Any coverage
    ratio with CBA in the numerator therefore has a scope mismatch
    that must be surfaced — never silently elided.
    """
    df = npe_aggregates.copy()
    if "coverage_ratio_caveat" not in df.columns:
        df["coverage_ratio_caveat"] = None

    def _has_cba(banks: Iterable[str] | None) -> bool:
        if banks is None:
            return False
        return "cba" in tuple(banks)

    def _append_caveat(row: pd.Series) -> str | None:
        if row["metric"] != METRIC_NPE:
            return row.get("coverage_ratio_caveat")
        if not _has_cba(row.get("contributing_banks")):
            return row.get("coverage_ratio_caveat")
        existing = row.get("coverage_ratio_caveat")
        new = "cba_aps120_scope_mismatch"
        if not existing or pd.isna(existing):
            return new
        return f"{existing}; {new}"

    df["coverage_ratio_caveat"] = df.apply(_append_caveat, axis=1)
    return df
