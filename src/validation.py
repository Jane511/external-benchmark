"""Cross-source validation — flags anomalies without computing a consensus.

The engine deliberately does NOT triangulate (compute a weighted average
across sources). Triangulation is a use-case-specific decision and belongs
in the consuming project. What the engine DOES do is flag obvious
data-quality issues that any consumer would want to know about:

  - Spread: max - min across PEER sources for the same segment
  - Outlier: any peer source more than ``outlier_threshold`` × peer median
  - Vintage staleness: any source older than its source-type cadence
  - Peer-Big4-vs-non-bank: ratio of peer_non_bank median to peer_big4
    median (informational only)
  - Reference anchors: regulator / rating-agency / regulatory-floor /
    industry-body values surfaced separately from peer arithmetic

These flags are surfaced on each ObservationSet and in the per-segment
validation summary that goes into the engine reports. They do not modify
or filter the underlying observations.
"""
from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import date
from typing import Optional, Sequence

from src.models import Cohort, PEER_COHORTS, RawObservation, cohort_for


SEGMENT_ALIASES: dict[str, str] = {
    "commercial_property_investment": "commercial_property",
}


# Bank Pillar 3 source IDs (lower-case). Big 4 only — Macquarie is in its
# own ``Cohort.PEER_OTHER_MAJOR_BANK`` cohort and is excluded from the
# Big-4 spread / Big-4-vs-non-bank-ratio numerators and denominators.
BIG4_SOURCE_IDS: frozenset[str] = frozenset({"cba", "nab", "wbc", "anz"})


PEER_RATIO_DEFINITION: str = (
    "peer_big4_vs_non_bank_ratio = median(peer_non_bank values) / "
    "median(peer_big4 values), computed only when both cohorts have "
    ">=1 observation. Macquarie (peer_other_major_bank), regulatory "
    "floors, rating-agency indices, regulator aggregates, and industry "
    "bodies are excluded from both medians and listed separately under "
    "reference anchors."
)
"""Single-source-of-truth wording for the peer ratio. Both the rendered
report (Section 4) and ``validation_flags.csv`` (units / definition
header) emit this string verbatim so the two outputs cannot drift."""


def canonical_segment(segment: str) -> str:
    """Return the canonical segment ID after applying any read-side aliases."""
    return SEGMENT_ALIASES.get(segment, segment)


def is_big4_source_id(source_id: str) -> bool:
    """Return True iff ``source_id`` belongs to a Big 4 bank.

    Macquarie is *not* Big 4 — APRA classifies them as a major bank but
    they sit in their own cohort (``PEER_OTHER_MAJOR_BANK``). Use
    :func:`src.models.cohort_for` for the full peer-group classification.
    """
    sid = source_id.lower()
    if sid in BIG4_SOURCE_IDS:
        return True
    normalized = sid.replace("-", "_")
    head = normalized.split("_", 1)[0]
    return head in BIG4_SOURCE_IDS


@dataclass
class ValidationFlags:
    """Per-segment data-quality flags. Informational only — values are not modified."""

    n_sources: int
    spread_pct: float | None             # (max - min) / median across peer cohorts
    outlier_sources: list[str]           # peer source IDs flagged as outliers
    stale_sources: list[str]             # source IDs past their source-type cadence
    bank_vs_nonbank_ratio: float | None  # alias of peer_big4_vs_non_bank_ratio
    big4_spread_pct: float | None        # spread within Big 4 only
    peer_big4_vs_non_bank_ratio: float | None = None
    reference_anchors: list[dict] = field(default_factory=list)
    frozen_dataset_banner: Optional[str] = None
    """When set, the per-row stale_sources column is suppressed in
    rendered output and this string is shown once at the top of the
    validation table. Set when the audit trail has no recent fetches."""

    def __post_init__(self) -> None:
        if self.spread_pct is not None and self.spread_pct < 0:
            raise ValueError("spread_pct cannot be negative")
        # Backwards-compat alias: the legacy field name is kept so older
        # consumers keep working; new code reads peer_big4_vs_non_bank_ratio.
        if self.peer_big4_vs_non_bank_ratio is None:
            self.peer_big4_vs_non_bank_ratio = self.bank_vs_nonbank_ratio
        elif self.bank_vs_nonbank_ratio is None:
            self.bank_vs_nonbank_ratio = self.peer_big4_vs_non_bank_ratio


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


def _dedup(items: Sequence[str]) -> list[str]:
    """Stable de-dup; preserves first-seen order."""
    return list(OrderedDict.fromkeys(items))


_DEFAULT_STALE_THRESHOLD = 210


def _staleness_threshold(
    source_type_value: str,
    refresh_schedules: Optional[dict[str, int]],
) -> int:
    """Resolve the stale-after-N-days threshold for a source type."""
    if not refresh_schedules:
        return _DEFAULT_STALE_THRESHOLD
    if source_type_value in refresh_schedules:
        return int(refresh_schedules[source_type_value])
    # APRA / RBA / Pillar 3 etc. share family-level cadences; map back-compat.
    family_aliases = {
        "bank_pillar3": "pillar3",
        "non_bank_listed": "listed_peer",
        "apra_qpex": "apra_adi",
        "apra_performance": "apra_adi",
        "rba_aggregate": "rba",
        "rating_agency_index": "rating_agency",
    }
    aliased = family_aliases.get(source_type_value)
    if aliased and aliased in refresh_schedules:
        return int(refresh_schedules[aliased])
    return _DEFAULT_STALE_THRESHOLD


def compute_validation_flags(
    observations: Sequence[RawObservation],
    *,
    outlier_threshold: float = 2.0,
    staleness_days: int | None = None,
    today: date | None = None,
    refresh_schedules: Optional[dict[str, int]] = None,
    refresh_pipeline_quiet: bool = False,
    arithmetic_parameter: str = "pd",
) -> ValidationFlags:
    """Compute cross-source validation flags WITHOUT averaging.

    The engine never publishes a consensus value. It publishes the raw
    observations and these flags. Consumers decide what to do with the spread.

    Parameters
    ----------
    refresh_schedules
        Optional ``{source_type: max_days}`` mapping. When provided,
        each row uses its source-type-specific cadence; otherwise a
        single ``staleness_days`` (or the legacy default) applies.
    refresh_pipeline_quiet
        Set by the report when the audit trail shows no fetches in the
        recent past — the per-row stale list is suppressed and a single
        ``frozen_dataset_banner`` string is emitted instead.
    arithmetic_parameter
        The single parameter that spread / median / outlier / peer-ratio
        arithmetic is computed over (default: ``"pd"``). Mixing PD with
        LGD or arrears in one spread is meaningless. ``stale_sources``,
        ``n_sources``, and ``reference_anchors`` use the full input.
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
            peer_big4_vs_non_bank_ratio=None,
            reference_anchors=[],
        )

    # Restrict arithmetic to one parameter (PD by default). Stale, count,
    # reference anchors keep using the full input set.
    arith_obs = [o for o in observations if o.parameter == arithmetic_parameter]

    # Cohort partition over the arithmetic-relevant subset.
    by_cohort: dict[Cohort, list[RawObservation]] = {}
    for obs in arith_obs:
        coh = cohort_for(obs.source_type, obs.source_id)
        by_cohort.setdefault(coh, []).append(obs)

    peer_observations: list[RawObservation] = []
    for coh in PEER_COHORTS:
        peer_observations.extend(by_cohort.get(coh, []))

    peer_values = [o.value for o in peer_observations if o.value is not None]
    median = _median(peer_values)
    spread = (max(peer_values) - min(peer_values)) / median if median > 0 and peer_values else None

    outliers = _dedup(
        o.source_id
        for o in peer_observations
        if o.value is not None
        and median > 0
        and (
            o.value > outlier_threshold * median
            or o.value < median / outlier_threshold
        )
    )

    # Staleness uses the full observation set (every staged source is a
    # candidate for refresh, regardless of parameter).
    legacy_cutoff = staleness_days if staleness_days is not None else _DEFAULT_STALE_THRESHOLD
    if refresh_pipeline_quiet:
        stale: list[str] = []
        banner: Optional[str] = (
            "Refresh pipeline has not fetched recently; per-row staleness "
            "column suppressed (data is current as of last cycle)."
        )
    else:
        raw_stale: list[str] = []
        for o in observations:
            threshold = (
                _staleness_threshold(o.source_type.value, refresh_schedules)
                if refresh_schedules is not None
                else legacy_cutoff
            )
            if (today - o.as_of_date).days > threshold:
                raw_stale.append(o.source_id)
        stale = _dedup(raw_stale)
        banner = None

    # Big-4 cohort: peer_big4 only (Macquarie excluded).
    big4_obs = [o for o in by_cohort.get(Cohort.PEER_BIG4, []) if o.value is not None]
    nonbank_obs = [o for o in by_cohort.get(Cohort.PEER_NON_BANK, []) if o.value is not None]
    big4_vals = [o.value for o in big4_obs]
    nonbank_vals = [o.value for o in nonbank_obs]

    peer_ratio: float | None = None
    if big4_vals and nonbank_vals:
        big4_med = _median(big4_vals)
        non_med = _median(nonbank_vals)
        if big4_med > 0:
            peer_ratio = non_med / big4_med

    big4_spread: float | None = None
    if len(big4_vals) >= 2:
        big4_med = _median(big4_vals)
        if big4_med > 0:
            big4_spread = (max(big4_vals) - min(big4_vals)) / big4_med

    # Reference anchors: every non-peer observation surfaces here so a
    # reader can see them without them poisoning peer arithmetic.
    # Anchors walk the *full* input set (not just the arithmetic
    # subset) — regulator NPLs and rating-agency arrears anchor a
    # segment even when there's no peer PD row to compare to.
    reference_cohorts = (
        Cohort.PEER_OTHER_MAJOR_BANK,
        Cohort.REGULATOR_AGGREGATE,
        Cohort.RATING_AGENCY,
        Cohort.REGULATORY_FLOOR,
        Cohort.INDUSTRY_BODY,
    )
    anchors: list[dict] = []
    seen_anchor_keys: set[tuple[str, str]] = set()
    by_cohort_full: dict[Cohort, list[RawObservation]] = {}
    for obs in observations:
        coh = cohort_for(obs.source_type, obs.source_id)
        by_cohort_full.setdefault(coh, []).append(obs)
    for coh in reference_cohorts:
        for o in by_cohort_full.get(coh, []):
            if o.value is None:
                continue
            key = (o.source_id, coh.value)
            if key in seen_anchor_keys:
                continue
            seen_anchor_keys.add(key)
            anchors.append({
                "source_id": o.source_id,
                "cohort": coh.value,
                "value": o.value,
                "as_of_date": o.as_of_date.isoformat(),
            })

    return ValidationFlags(
        n_sources=n,
        spread_pct=spread,
        outlier_sources=outliers,
        stale_sources=stale,
        bank_vs_nonbank_ratio=peer_ratio,
        big4_spread_pct=big4_spread,
        peer_big4_vs_non_bank_ratio=peer_ratio,
        reference_anchors=anchors,
        frozen_dataset_banner=banner,
    )
