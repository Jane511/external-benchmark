"""Shared canonical schema for per-bank Pillar 3 industry extractions.

Phase 3.B contract. Each bank's industry extension (WBC / NAB / ANZ
in-place; CBA via sibling module per Phase 3 ruling) emits a long-format
DataFrame with the columns declared here. The aggregation layer (Phase
3.C) consumes these per-bank frames and applies the harmonisation map
in ``config/anzsic_harmonisation_map.yaml`` to produce Big-4-aggregated
canonical-bucket rows.

Preservation principle (Phase 3.B.2 §3.2)
-----------------------------------------

Adapters preserve the bank's actual disclosure shape; aggregation
imposes the canonical shape. Per-bank quirks (geography splits,
gross-carrying-amount component splits, missing metric types, "of
which" subsets) are emitted as-is in row form. Aggregation modules —
not adapters — decide how to flatten or harmonise across banks.

Concrete consequences:

- An adapter that sees a published row MUST emit it (after schema
  conformance), even if the bank's row has no analogue at other banks.
- An adapter MUST NOT silently drop a published bucket. Unknown labels
  raise so the harmonisation map can be updated explicitly.
- An adapter MAY emit zero rows of a given metric if the bank does not
  publish it (e.g. NAB and ANZ do not publish write-offs by industry).

Five guardrails (Phase 3.B sign-off)
------------------------------------

1. **Provenance metadata** on every row — ``source_publication``,
   ``source_table_ref``, ``source_page``.
2. **Zero vs redacted** — ``value_aud_m=0.0`` is an honest published
   zero. A null ``value_aud_m`` paired with a non-null
   ``redaction_reason`` is a not-disclosed cell. Adapters MUST NOT
   silently coerce one into the other.
3. **No synthetic alignment** — ``as_of_date`` is the actual reporting
   date as printed on the bank's table. No carry-forward, no
   interpolation. Plan §5.3 Issue-5 invariant scoped to per-bank source.
4. **No ``contributing_banks`` column in adapter output** — that field
   exists only on aggregation-layer rows. Per-bank rows always pertain
   to one bank named in ``bank_code``.
5. **Period-length metadata** — ``period_length_months`` indicates the
   reporting window for flow metrics (write-offs / actual losses).
   Set to ``None`` for stock metrics (exposure, NPE, provisions). Plan
   §5.4 Issue-4 ruling: aggregation must not sum mismatched periods.

Optional fields
---------------

Adapters MAY attach extra columns beyond
:data:`INDUSTRY_ROW_COLUMNS` to capture per-bank metadata. The
invariant function tolerates extras. Two such fields are defined as
constants here so adapters use the same key:

- :data:`COL_PROVISION_BASIS` — captures the definitional basis of the
  provision figure (``aps220_specific`` for CBA APS 220 specific
  provision; ``aasb9_stage3_ecl`` for the AASB 9 Stage 3 ECL provision
  reported by WBC, NAB, ANZ).
- :data:`COL_GROSS_CARRYING_COMPONENT` — for ANZ, which splits gross
  carrying amount three ways (``loans``, ``off_balance_sheet``,
  ``other``). Other banks emit a single ``total`` value.
- :data:`COL_PORTFOLIO_TYPE` — for CBA, which publishes CRB(e)(ii) as
  a portfolio-type × industry matrix. The 10 CBA portfolio-type values
  are declared in :data:`PORTFOLIO_TYPE_VALUES`. Other banks do not
  publish a portfolio split at industry level and therefore omit this
  column from their adapter outputs.

CBA-specific dash convention
----------------------------

CBA's CRB(e)(ii) matrix uses dashes to denote structural zeros (the
portfolio type does not extend to that industry). Per the CBA recon
ruling sign-off in Phase 3.B.3 §C, dashes in this specific table are
treated as honest-zero (``value=0.0``, no ``redaction_reason``) rather
than redacted. The override is encoded as a (bank, table) pair in
:data:`_HONEST_ZERO_TABLES`; it does **not** generalise the §A.2 rule
beyond CBA CRB(e)(ii).

Dash handling for the Government & Official Institutions row
-------------------------------------------------------------

Per recon §1.8 + Phase 3.B.3 §A.2, the Government & Official
Institutions row is a known no-exposure case across all Big-4 banks:
where a metric column is published as a dash for this specific
industry, the cell is honest-zero (no exposure → no NPE / no
provision / no write-off) rather than redacted. The rule applies
**only** to this single industry, only when the bank's footnotes do
not contradict zero, and only to dash cells (numeric publications are
always taken at face value). All other industries with dashes apply
guardrail 2 (null + ``redaction_reason='published_as_dash'``).

The rule is encoded once in
:func:`is_honest_zero_government_row(bank_code, industry_label)` so
adapters inherit harmonised behaviour by construction. The mapping
:data:`_GOVERNMENT_INDUSTRY_PER_BANK` declares the published label
each bank uses for this industry; adding a fifth bank requires only a
new entry there.

Null handling
-------------

:func:`_is_null` is the canonical absent-value check used by
:func:`assert_row_well_formed`. It treats ``None``, ``float('nan')``,
``pd.NA``, and ``numpy.nan`` as equivalent-null. It does **not** treat
empty strings or zero-length collections as null — those are
semantically distinct from absent values and would mask a real parser
bug. Use :func:`_is_null` in any adapter that needs to make the same
distinction.
"""

from __future__ import annotations

import math
from typing import Any, Final


def _is_null(v: Any) -> bool:
    """Treat None / NaN / pd.NA / numpy NaN as equivalent-null.

    Empty strings and zero-length collections are NOT null — they are
    semantically distinct from absent values. Use this helper anywhere
    you need pandas-row iteration to behave like dict iteration for
    missing-ness checks.
    """
    if v is None:
        return True
    if isinstance(v, float) and math.isnan(v):
        return True
    # pandas types — guard against import cost.
    try:
        import pandas as pd
        if v is pd.NA:
            return True
        if pd.api.types.is_scalar(v) and pd.isna(v):
            return True
    except Exception:
        pass
    return False


# Canonical column order for per-bank Pillar 3 industry rows.
INDUSTRY_ROW_COLUMNS: Final[list[str]] = [
    "data_source",            # e.g. "pillar3_wbc"
    "aggregation_level",      # always "single_bank" at adapter layer
    "bank_code",              # "cba" | "nab" | "wbc" | "anz"
    "as_of_date",             # actual published reporting date (date)
    "period_length_months",   # 6 / 12 for flow metrics; None for stocks
    "geography",              # bank's published geography or "Total"
    "industry_published",     # raw label exactly as printed on the table
    "metric",                 # see METRIC_NAMES below
    "value_aud_m",            # float | None — None iff redacted/missing
    "redaction_reason",       # None | str — see REDACTION_REASONS
    "source_publication",     # e.g. "WBC Pillar 3 — September 2025"
    "source_table_ref",       # e.g. "CRB(f)"
    "source_page",            # 1-based PDF page (int) | None
]


# Allowed metric names. Per Phase 2 recon §1.2: PD-by-industry is not
# published anywhere in Big 4 Pillar 3 industry tables; past-due-90+ in
# isolation by industry is not published either (NPE captures both per
# APS 220). The set below reflects what is actually deliverable.
METRIC_EXPOSURE: Final[str] = "exposure_aud_m"
METRIC_NPE: Final[str] = "npe_aud_m"
METRIC_PROVISIONS: Final[str] = "individually_assessed_provision_aud_m"
METRIC_WRITE_OFFS: Final[str] = "write_offs_aud_m"

METRIC_NAMES: Final[frozenset[str]] = frozenset({
    METRIC_EXPOSURE,
    METRIC_NPE,
    METRIC_PROVISIONS,
    METRIC_WRITE_OFFS,
})

# Stock metrics (period_length_months SHOULD be None) vs flow metrics
# (period_length_months SHOULD be 6 or 12).
STOCK_METRICS: Final[frozenset[str]] = frozenset({
    METRIC_EXPOSURE, METRIC_NPE, METRIC_PROVISIONS,
})
FLOW_METRICS: Final[frozenset[str]] = frozenset({METRIC_WRITE_OFFS})


# Optional column constants — adapters that attach these MUST use these
# exact keys so downstream consumers can rely on consistent naming.
COL_PROVISION_BASIS: Final[str] = "provision_basis"
COL_GROSS_CARRYING_COMPONENT: Final[str] = "gross_carrying_component"

# Allowed values for the optional provision_basis column.
PROVISION_BASIS_APS220: Final[str] = "aps220_specific"
PROVISION_BASIS_AASB9_STAGE3: Final[str] = "aasb9_stage3_ecl"
PROVISION_BASIS_VALUES: Final[frozenset[str]] = frozenset({
    PROVISION_BASIS_APS220, PROVISION_BASIS_AASB9_STAGE3,
})

# Allowed values for the optional gross_carrying_component column.
GROSS_COMPONENT_TOTAL: Final[str] = "total"
GROSS_COMPONENT_LOANS: Final[str] = "loans"
GROSS_COMPONENT_OFF_BS: Final[str] = "off_balance_sheet"
GROSS_COMPONENT_OTHER: Final[str] = "other"
GROSS_COMPONENT_VALUES: Final[frozenset[str]] = frozenset({
    GROSS_COMPONENT_TOTAL, GROSS_COMPONENT_LOANS,
    GROSS_COMPONENT_OFF_BS, GROSS_COMPONENT_OTHER,
})

# Phase 3.B.3 §B.2 — CBA-specific portfolio-type column.
COL_PORTFOLIO_TYPE: Final[str] = "portfolio_type"

# CBA's 10 portfolio types as published in CRB(e)(ii). Order matches
# the published table; the parser uses positional ordering rather than
# label matching to handle the multi-line "Corporate (incl. ...)" wrap.
PORTFOLIO_TYPE_CORPORATE: Final[str] = "corporate_incl_large_and_sme"
PORTFOLIO_TYPE_SOVEREIGN: Final[str] = "sovereign"
PORTFOLIO_TYPE_FINANCIAL: Final[str] = "financial_institution"
PORTFOLIO_TYPE_SME_RETAIL: Final[str] = "sme_retail"
PORTFOLIO_TYPE_RES_MORTGAGE: Final[str] = "residential_mortgage"
PORTFOLIO_TYPE_QRR: Final[str] = "qualifying_revolving_retail"
PORTFOLIO_TYPE_OTHER_RETAIL: Final[str] = "other_retail"
PORTFOLIO_TYPE_SPECIALISED: Final[str] = "specialised_lending"
PORTFOLIO_TYPE_OTHER_ASSETS: Final[str] = "other_assets"
PORTFOLIO_TYPE_RBNZ: Final[str] = "rbnz_regulated_entities"

PORTFOLIO_TYPE_VALUES: Final[frozenset[str]] = frozenset({
    PORTFOLIO_TYPE_CORPORATE, PORTFOLIO_TYPE_SOVEREIGN,
    PORTFOLIO_TYPE_FINANCIAL, PORTFOLIO_TYPE_SME_RETAIL,
    PORTFOLIO_TYPE_RES_MORTGAGE, PORTFOLIO_TYPE_QRR,
    PORTFOLIO_TYPE_OTHER_RETAIL, PORTFOLIO_TYPE_SPECIALISED,
    PORTFOLIO_TYPE_OTHER_ASSETS, PORTFOLIO_TYPE_RBNZ,
})


# Phase 3.B.3 §B.1 §8 — table-level honest-zero override. Distinct from
# the row-level Govt rule. Applies dashes-as-zero to every cell within
# the listed (bank, source_table_ref) tables. Provisionally adopted for
# CBA CRB(e)(ii) where dashes are a structural-zero convention; awaiting
# Section C sign-off (one-line revert if (b) or (c) preferred).
_HONEST_ZERO_TABLES: Final[set[tuple[str, str]]] = {
    ("cba", "CRB(e)(ii)"),
}


def is_honest_zero_table(bank_code: str, source_table_ref: str) -> bool:
    """True iff (bank, table) is on the table-level honest-zero list."""
    return (bank_code, source_table_ref) in _HONEST_ZERO_TABLES


def compose_columns(extras: list[str] | None = None) -> list[str]:
    """Canonical columns plus any optional extras the adapter attaches.

    Use this to build the ``columns=`` argument to
    :func:`pandas.DataFrame.from_records` so the canonical column order
    is preserved and the per-bank optional columns appear deterministically
    after the canonical ones (and only the listed extras — never silently
    expanded by row contents).
    """
    extras = extras or []
    seen = set(INDUSTRY_ROW_COLUMNS)
    deduped: list[str] = []
    for e in extras:
        if e in seen:
            continue
        seen.add(e)
        deduped.append(e)
    return list(INDUSTRY_ROW_COLUMNS) + deduped


# Allowed redaction reasons. Adapters that detect new categories of
# missing-ness should add the constant here rather than inventing free
# text — keeps the consumer's filter logic finite.
REDACTION_NOT_DISCLOSED: Final[str] = "not_disclosed_in_published_table"
REDACTION_DASH_OR_HYPHEN: Final[str] = "published_as_dash"

REDACTION_REASONS: Final[frozenset[str]] = frozenset({
    REDACTION_NOT_DISCLOSED,
    REDACTION_DASH_OR_HYPHEN,
})


# Per Phase 3.B.3 §A.2 — Government & Official Institutions is the only
# industry where dashes are interpreted as honest-zero across all banks.
# Each bank publishes this industry under its own label; the mapping is
# the single source of truth for the rule.
_GOVERNMENT_INDUSTRY_PER_BANK: Final[dict[str, str]] = {
    "wbc": "Government, administration and defence",
    "nab": "Government and public authorities",
    "anz": "Government & Official Institutions",
    "cba": "Government Administration & Defence",
}


def is_honest_zero_government_row(bank_code: str, industry_label: str) -> bool:
    """Return True iff (bank, label) is the Government & Official
    Institutions row for which dashes mean honest-zero (recon §1.8).

    The rule applies **only** to this exact (bank, label) pair. Other
    industries with dashes retain the redacted-with-reason treatment
    under guardrail 2. Adding a fifth bank: extend
    :data:`_GOVERNMENT_INDUSTRY_PER_BANK`.
    """
    return _GOVERNMENT_INDUSTRY_PER_BANK.get(bank_code) == industry_label


def coerce_metric_cell(
    bank_code: str,
    industry_label: str,
    raw_token: str,
    *,
    source_table_ref: str | None = None,
) -> tuple[float | None, str | None]:
    """Apply the unified zero-vs-redacted rule to one published metric cell.

    - Numeric token → ``(float, None)``.
    - Parenthesised numeric ``"(123)"`` (provision charges) →
      ``(123.0, None)`` — sign stripped; consumers read absolute amount.
    - Dash ``"-"`` on the Government row →
      ``(0.0, None)`` (honest-zero per :func:`is_honest_zero_government_row`).
    - Dash on any other industry →
      ``(None, REDACTION_DASH_OR_HYPHEN)`` (guardrail 2).
    - Unparseable token → same as dash on a non-Government industry.

    This helper is the single implementation of the rule used by all
    per-bank parsers; the WBC / NAB / ANZ / CBA adapters call it rather
    than re-implementing the dash-vs-zero distinction.
    """
    if raw_token == "-":
        if is_honest_zero_government_row(bank_code, industry_label):
            return 0.0, None
        if source_table_ref is not None and is_honest_zero_table(
            bank_code, source_table_ref,
        ):
            return 0.0, None
        return None, REDACTION_DASH_OR_HYPHEN
    raw = raw_token.replace(",", "")
    if raw.startswith("(") and raw.endswith(")"):
        raw = raw[1:-1]
    try:
        return float(raw), None
    except ValueError:
        return None, REDACTION_DASH_OR_HYPHEN


def assert_row_well_formed(row: dict) -> None:
    """Light-touch invariant checks. Adapters call this per-row in tests.

    Validates the five Phase 3.B guardrails at the row level, plus the
    optional ``provision_basis`` and ``gross_carrying_component``
    columns when present. Does not validate cross-row invariants (those
    belong in adapter-level tests).

    A DataFrame containing zero write-off rows is valid (Phase 3.B.2
    §3.3) — the row-by-row invariant only fires when a row exists, so
    no special handling is needed; the contract is enforced at the row
    level, not the bank level.
    """
    missing = set(INDUSTRY_ROW_COLUMNS) - set(row)
    if missing:
        raise AssertionError(f"row missing columns: {sorted(missing)}")

    if row["aggregation_level"] != "single_bank":
        raise AssertionError(
            "adapter rows must carry aggregation_level='single_bank'; "
            "aggregation-layer rows belong elsewhere"
        )

    if "contributing_banks" in row:
        raise AssertionError(
            "adapter rows MUST NOT carry contributing_banks "
            "(guardrail 4 — that column is aggregation-layer-only)"
        )

    if row["metric"] not in METRIC_NAMES:
        raise AssertionError(
            f"unknown metric {row['metric']!r}; allowed = {sorted(METRIC_NAMES)}"
        )

    val = row["value_aud_m"]
    red = row["redaction_reason"]
    val_null = _is_null(val)
    red_null = _is_null(red)
    if val_null and red_null:
        raise AssertionError(
            "value_aud_m=None requires a redaction_reason "
            "(guardrail 2 — distinguish zero from redacted)"
        )
    if not val_null and not red_null:
        raise AssertionError(
            "value_aud_m present must have redaction_reason=None "
            "(guardrail 2 — a value cannot also be redacted)"
        )
    if not red_null and red not in REDACTION_REASONS:
        raise AssertionError(
            f"unknown redaction_reason {red!r}; "
            f"add the constant to pillar3_industry_schema if intentional"
        )

    period = row["period_length_months"]
    period_null = _is_null(period)
    if row["metric"] in STOCK_METRICS and not period_null:
        raise AssertionError(
            f"stock metric {row['metric']!r} must have "
            f"period_length_months=None (guardrail 5)"
        )
    if row["metric"] in FLOW_METRICS:
        if period_null or int(period) not in (6, 12):
            raise AssertionError(
                f"flow metric {row['metric']!r} must have "
                f"period_length_months in {{6, 12}}, got {period!r}"
            )

    # Optional fields — validated only when present.
    if COL_PROVISION_BASIS in row and not _is_null(row[COL_PROVISION_BASIS]):
        pb = row[COL_PROVISION_BASIS]
        if pb not in PROVISION_BASIS_VALUES:
            raise AssertionError(
                f"unknown provision_basis {pb!r}; allowed = "
                f"{sorted(PROVISION_BASIS_VALUES)}"
            )
    if (
        COL_GROSS_CARRYING_COMPONENT in row
        and not _is_null(row[COL_GROSS_CARRYING_COMPONENT])
    ):
        gc = row[COL_GROSS_CARRYING_COMPONENT]
        if gc not in GROSS_COMPONENT_VALUES:
            raise AssertionError(
                f"unknown gross_carrying_component {gc!r}; allowed = "
                f"{sorted(GROSS_COMPONENT_VALUES)}"
            )
    if COL_PORTFOLIO_TYPE in row and not _is_null(row[COL_PORTFOLIO_TYPE]):
        pt = row[COL_PORTFOLIO_TYPE]
        if pt not in PORTFOLIO_TYPE_VALUES:
            raise AssertionError(
                f"unknown portfolio_type {pt!r}; allowed = "
                f"{sorted(PORTFOLIO_TYPE_VALUES)}"
            )
