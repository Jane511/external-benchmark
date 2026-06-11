"""ANZ Pillar 3 — industry-table extraction.

Implements the per-bank industry emission for ANZ under the Phase 3.B
canonical schema in :mod:`pillar3_industry_schema`. Parses one table:

- "Breakdown of exposures, amounts of non-performing exposures and
  accounting provisions, by industry" — page 43 of the FY2025 release.

Column layout per industry row (after stripping the leading row
number, e.g. ``"1 "``):

    <industry-label> <gross_total> <gross_loans> <gross_off_bs>
    <gross_other> <npe_total> <npe_individually_assessed>
    <prov_total> <prov_individual>

ANZ-specific quirks (Phase 3.B.2 §5.2):

- **Three-way gross carrying split.** Each industry yields three
  exposure rows tagged ``gross_carrying_component`` ∈
  {``loans``, ``off_balance_sheet``, ``other``}. The "Total" gross
  carrying column is intentionally NOT emitted as a separate row —
  it equals the sum of the three components and would duplicate
  information. Aggregation reconstructs the total when needed.
- **NPE row uses the Total column** (not "of which: individually
  assessed for ECL"). One NPE row per industry.
- **Provision row uses "of which: individual provision"** (col 8).
  Per ANZ's table header "Accounting provisions for non-performing
  exposures" — AASB 9 terminology. Tagged
  ``provision_basis = aasb9_stage3_ecl``.
- **Government & Official Institutions zero NPE** is special-cased
  per instruction §5.2 + recon §1.8: dashes here mean honest zero
  (no NPE → no provision), not redacted. Other industries with
  dashes apply guardrail 2 (null + ``published_as_dash``). The
  rationale comes from recon §1.8, not from a literal ANZ footnote
  — this asymmetry is documented in the Phase 3.B.2 checkpoint so
  the inconsistency with WBC/NAB dash handling can be reconciled in
  a follow-up review.
- **Personal Lending and Residential Mortgage** are not business-
  lending and route via the harmonisation map to
  ``consumer_lending_personal`` and
  ``consumer_lending_residential_mortgage`` respectively. Adapter
  emits the rows as published; routing is the harmonisation map's
  job. The integration test (Phase 3.B.2 §6) asserts these do not
  land in any ``business_lending_anzsic_*`` segment.
- **No write-offs by industry** — adapter emits zero write-off rows.
- **Single-geography reporting at industry level** — every row
  carries ``geography = 'Total'``.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pdfplumber

from ingestion.adapters.pillar3_industry_schema import (
    COL_GROSS_CARRYING_COMPONENT,
    COL_PROVISION_BASIS,
    GROSS_COMPONENT_LOANS,
    GROSS_COMPONENT_OFF_BS,
    GROSS_COMPONENT_OTHER,
    METRIC_EXPOSURE,
    METRIC_NPE,
    METRIC_PROVISIONS,
    PROVISION_BASIS_AASB9_STAGE3,
    REDACTION_DASH_OR_HYPHEN,
    coerce_metric_cell,
    compose_columns,
)

logger = logging.getLogger(__name__)


DEFAULT_INDUSTRY_PAGE = 43  # FY2025 release


# Order matches ANZ's published table (rows numbered 1-15 in the PDF;
# row 16 is "Total" — terminator).
ANZ_INDUSTRIES: tuple[str, ...] = (
    "Agriculture, Forestry, Fishing & Mining",
    "Business & Property Services",
    "Commercial Property",
    "Construction",
    "Electricity, Gas & Water Supply",
    "Entertainment, Leisure & Tourism",
    "Financial, Investment & Insurance",
    "Government & Official Institutions",
    "Manufacturing",
    "Personal Lending",
    "Residential Mortgage",
    "Retail Trade",
    "Transport & Storage",
    "Wholesale Trade",
    "Other",
)
_INDUSTRY_LOOKUP = {ind.lower(): ind for ind in ANZ_INDUSTRIES}


# Phase 3.B.3 §A.2: honest-zero rule for the Government row is now
# centralised in :func:`pillar3_industry_schema.coerce_metric_cell` —
# this constant is retained only for documentation / cross-reference.
# Adapters call ``coerce_metric_cell("anz", industry, token)`` and the
# helper applies the rule by consulting
# :data:`pillar3_industry_schema._GOVERNMENT_INDUSTRY_PER_BANK`.

# Strip the 1- or 2-digit leading row index (e.g. "1 ", "16 ") that
# ANZ prefixes to every CRB-family table line.
_ROW_INDEX_PREFIX_RE = re.compile(r"^\d{1,2}\s+")

_NUMERIC_TOKEN_RE = re.compile(r"\(\d[\d,]*\)|\d[\d,]*|-")
_AS_AT_RE = re.compile(r"^\s*Sep\s+(\d{2})\s*$")  # ANZ uses "Sep 25"


def _parse_as_at(line: str) -> date | None:
    m = _AS_AT_RE.match(line)
    if not m:
        return None
    yy = int(m.group(1))
    year = 2000 + yy if yy < 80 else 1900 + yy
    return date(year, 9, 30)


def _strip_row_index(line: str) -> str:
    return _ROW_INDEX_PREFIX_RE.sub("", line)


def _peel_numeric_tokens(line: str, n: int) -> tuple[str, list[str]] | None:
    line = line.rstrip()
    tokens: list[str] = []
    cursor = line
    for _ in range(n):
        m = None
        for candidate in _NUMERIC_TOKEN_RE.finditer(cursor):
            m = candidate
        if m is None or m.end() != len(cursor):
            return None
        tokens.append(m.group(0))
        cursor = cursor[: m.start()].rstrip()
    tokens.reverse()
    return cursor, tokens


def _coerce_value(industry: str, token: str) -> tuple[float | None, str | None]:
    """Delegates to the shared :func:`coerce_metric_cell` (Phase 3.B.3 §A.2)."""
    return coerce_metric_cell("anz", industry, token)


def _coerce_with_honest_zero_rule(
    industry: str, token: str,
) -> tuple[float | None, str | None]:
    """Backwards-compatible alias — the honest-zero rule is now built
    into :func:`coerce_metric_cell` via the bank-aware Government-row
    mapping in :data:`_GOVERNMENT_INDUSTRY_PER_BANK`. Retained as a
    name to avoid churn at call sites."""
    return _coerce_value(industry, token)


def _match_industry(remaining_label: str) -> str | None:
    return _INDUSTRY_LOOKUP.get(remaining_label.strip().lower())


def parse_industry_text(
    page_text: str,
    *,
    source_publication: str,
    source_page: int,
) -> list[dict[str, Any]]:
    """Parse the ANZ industry table page text into canonical rows.

    Per industry: 3 exposure rows (loans / off_balance_sheet / other)
    + 1 NPE row + 1 provision row = 5 rows.
    """
    lines = page_text.splitlines()
    as_of = _find_as_at(lines)
    if as_of is None:
        raise ValueError(
            "ANZ industry table: no 'Sep <yy>' marker found — "
            "refuse to fabricate a date (guardrail 3)"
        )

    rows: list[dict[str, Any]] = []
    for raw_line in lines:
        stripped = raw_line.strip()
        if not stripped:
            continue
        # ANZ prefixes data rows with "1 " through "16 ".
        cleaned = _strip_row_index(stripped)
        if cleaned == stripped:
            # No row-index prefix — narrative, headers (e.g. "Total of
            # which: loans..."), footnotes. Skip silently.
            continue
        # End-of-table marker is the numbered "16 Total ..." row.
        if cleaned.lower().startswith("total"):
            break

        peeled = _peel_numeric_tokens(cleaned, 8)
        if peeled is None:
            continue
        label_part, tokens = peeled
        industry = _match_industry(label_part)
        if industry is None:
            raise ValueError(
                f"ANZ industry table: unmatched industry label "
                f"{label_part!r} on line {stripped!r} — adapter must "
                f"raise rather than silently drop"
            )

        # Three exposure rows from gross carrying split (loans, off_bs,
        # other = tokens[1], tokens[2], tokens[3]). Token[0] is the
        # Total gross carrying — intentionally NOT emitted (it's the
        # sum of the three).
        components = (
            (GROSS_COMPONENT_LOANS,        tokens[1]),
            (GROSS_COMPONENT_OFF_BS,       tokens[2]),
            (GROSS_COMPONENT_OTHER,        tokens[3]),
        )
        for component, tok in components:
            value, redaction = _coerce_value(industry, tok)
            rows.append(_make_row(
                as_of=as_of, industry=industry,
                metric=METRIC_EXPOSURE, value=value, redaction=redaction,
                period_length_months=None,
                source_publication=source_publication, source_page=source_page,
                gross_carrying_component=component,
            ))

        # Total NPE = tokens[4]. One row per industry.
        npe_value, npe_red = _coerce_with_honest_zero_rule(industry, tokens[4])
        rows.append(_make_row(
            as_of=as_of, industry=industry,
            metric=METRIC_NPE, value=npe_value, redaction=npe_red,
            period_length_months=None,
            source_publication=source_publication, source_page=source_page,
        ))

        # Of which: individual provision = tokens[7]. One row per industry.
        prov_value, prov_red = _coerce_with_honest_zero_rule(industry, tokens[7])
        rows.append(_make_row(
            as_of=as_of, industry=industry,
            metric=METRIC_PROVISIONS, value=prov_value, redaction=prov_red,
            period_length_months=None,
            source_publication=source_publication, source_page=source_page,
        ))

    return rows


def _make_row(
    *,
    as_of: date,
    industry: str,
    metric: str,
    value: float | None,
    redaction: str | None,
    period_length_months: int | None,
    source_publication: str,
    source_page: int,
    gross_carrying_component: str | None = None,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "data_source": "pillar3_anz",
        "aggregation_level": "single_bank",
        "bank_code": "anz",
        "as_of_date": as_of,
        "period_length_months": period_length_months,
        "geography": "Total",
        "industry_published": industry,
        "metric": metric,
        "value_aud_m": value,
        "redaction_reason": redaction,
        "source_publication": source_publication,
        "source_table_ref": "Exposures/NPE/Provisions by industry",
        "source_page": source_page,
    }
    if metric == METRIC_PROVISIONS:
        row[COL_PROVISION_BASIS] = PROVISION_BASIS_AASB9_STAGE3
    if gross_carrying_component is not None:
        row[COL_GROSS_CARRYING_COMPONENT] = gross_carrying_component
    return row


def _find_as_at(lines: list[str]) -> date | None:
    for line in lines:
        d = _parse_as_at(line.strip())
        if d is not None:
            return d
    return None


def extract_anz_industry_rows(
    pdf_path: Path,
    *,
    industry_page: int = DEFAULT_INDUSTRY_PAGE,
    source_publication_override: str | None = None,
) -> pd.DataFrame:
    """Top-level entry point for ANZ industry extraction."""
    pdf_path = Path(pdf_path)
    with pdfplumber.open(pdf_path) as pdf:
        text = pdf.pages[industry_page - 1].extract_text() or ""

    publication = source_publication_override or _infer_publication(pdf_path)
    rows = parse_industry_text(
        text,
        source_publication=publication,
        source_page=industry_page,
    )
    df = pd.DataFrame.from_records(
        rows,
        columns=compose_columns([
            COL_PROVISION_BASIS, COL_GROSS_CARRYING_COMPONENT,
        ]),
    )
    return df


def _infer_publication(pdf_path: Path) -> str:
    return f"ANZ Pillar 3 — {pdf_path.stem}"
