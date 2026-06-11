"""NAB Pillar 3 — industry-table extraction.

Implements the per-bank industry emission for NAB under the Phase 3.B
canonical schema in :mod:`pillar3_industry_schema`. Parses one table:

- "Exposure at default, non-performing exposures and related provisions
  by industry" — page 37 of the FY2025 release. Source values are
  "Credit and CCR EaD post-CCF post-CRM", "Non-performing exposures",
  and "Of which: individually assessed provision for credit
  impairment". Header text refers to the latter as Stage 3 ECL
  individually-assessed; the column is therefore tagged
  ``provision_basis = aasb9_stage3_ecl``.

NAB-specific quirks (Phase 3.B.2 §4.2):

- Provision basis is **AASB 9 Stage 3 ECL**, not APS 220 specific
  provision. Captured via ``provision_basis`` on every provision row.
- "Other" bundles education, health & community services. Emitted
  as-is; the harmonisation map handles parity with CBA's finer
  breakdown later.
- **No write-offs by industry** — adapter emits zero write-off rows.
  Schema accepts this (§3.3 — row-level invariant fires only when a
  row exists).
- Single-geography reporting at industry level — every row carries
  ``geography = 'Total'`` (matching WBC's write-off-row convention).
- Footnote markers on labels — "Utilities(3)" and "Other(4)" — are
  stripped during label matching against the harmonisation map.
- "Government and public authorities" is published as all-dashes (no
  exposure → no NPE / no provision). Treated per guardrail 2:
  ``value=None`` + ``redaction_reason='published_as_dash'`` on the NPE
  and provision rows. The exposure row carries the published numeric.

Phase 3.B guardrails honoured: identical to WBC sibling — provenance
on every row, dash → null + reason, no synthetic dates, no
``contributing_banks`` field, no ``period_length_months`` on stocks.
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
    COL_PROVISION_BASIS,
    METRIC_EXPOSURE,
    METRIC_NPE,
    METRIC_PROVISIONS,
    PROVISION_BASIS_AASB9_STAGE3,
    REDACTION_DASH_OR_HYPHEN,
    coerce_metric_cell,
    compose_columns,
)

logger = logging.getLogger(__name__)


DEFAULT_INDUSTRY_PAGE = 37  # FY2025 release


# Order matches NAB's published table. Used both as the recognised set
# and as a single source-of-truth for label normalisation. Footnote
# markers (e.g. "(3)", "(4)") are stripped before comparison.
NAB_INDUSTRIES: tuple[str, ...] = (
    "Accommodation and hospitality",
    "Agriculture, forestry, fishing and mining",
    "Business services and property services",
    "Commercial property",
    "Construction",
    "Finance and insurance",
    "Government and public authorities",
    "Manufacturing",
    "Personal",
    "Residential mortgages",
    "Retail and wholesale trade",
    "Transport and storage",
    "Utilities",
    "Other",
)
_INDUSTRY_LOOKUP = {ind.lower(): ind for ind in NAB_INDUSTRIES}


# Strip "(\d+)" footnote markers attached to a label (e.g. "Utilities(3)").
_FOOTNOTE_MARKER_RE = re.compile(r"\(\d+\)\s*$")

# Numeric token: integer with optional thousand-comma separators, or
# "(123)" parenthesised, or a literal dash "-".
_NUMERIC_TOKEN_RE = re.compile(r"\(\d[\d,]*\)|\d[\d,]*|-")

# "As at 30 Sep 25" / "As at 30 September 2025" header.
_AS_AT_RE = re.compile(r"^\s*As at\s+(\d{1,2}\s+\w+\s+\d{2,4})\s*$")


def _parse_as_at(line: str) -> date | None:
    m = _AS_AT_RE.match(line)
    if not m:
        return None
    raw = m.group(1)
    for fmt in ("%d %B %Y", "%d %b %Y", "%d %B %y", "%d %b %y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _peel_numeric_tokens(line: str, n: int) -> tuple[str, list[str]] | None:
    """Peel exactly ``n`` numeric tokens from the right of ``line``."""
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
    """Delegates to shared :func:`coerce_metric_cell` (Phase 3.B.3 §A.2)."""
    return coerce_metric_cell("nab", industry, token)


def _strip_footnote(label: str) -> str:
    return _FOOTNOTE_MARKER_RE.sub("", label).strip()


def _match_industry(remaining_label: str) -> str | None:
    cleaned = _strip_footnote(remaining_label)
    return _INDUSTRY_LOOKUP.get(cleaned.lower())


def parse_industry_text(
    page_text: str,
    *,
    source_publication: str,
    source_page: int,
) -> list[dict[str, Any]]:
    """Parse the NAB industry table page text.

    Emits 3 rows per industry: exposure_aud_m, npe_aud_m,
    individually_assessed_provision_aud_m.
    """
    lines = page_text.splitlines()
    as_of = _find_as_at(lines)
    if as_of is None:
        raise ValueError(
            "NAB industry table: no 'As at <date>' line found — "
            "refuse to fabricate a date (guardrail 3)"
        )

    rows: list[dict[str, Any]] = []
    started = False
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if not started:
            if _AS_AT_RE.match(stripped):
                started = True
            continue

        # End-of-block markers.
        if stripped.startswith("Total ") or stripped.startswith("Total\t"):
            break
        # NAB's column header "Industry sector $m $m $m $m $m" has 5
        # "$m" tokens — skip header rows that have non-industry text
        # leading them.
        peeled = _peel_numeric_tokens(stripped, 5)
        if peeled is None:
            continue
        label_part, tokens = peeled
        industry = _match_industry(label_part)
        if industry is None:
            # Reject only if label looks like an industry; tolerate
            # column-header / footnote rows that happen to have 5 tokens.
            if any(c.isalpha() for c in label_part):
                # If label_part contains "$m" header tokens, it's not an
                # industry row — skip silently.
                if "$m" in label_part or label_part.lower() in {
                    "industry sector",
                }:
                    continue
                raise ValueError(
                    f"NAB industry table: unmatched industry label "
                    f"{label_part!r} on line {stripped!r} — adapter "
                    f"must raise rather than silently drop"
                )
            continue

        exposure_value, exposure_red = _coerce_value(industry, tokens[0])
        npe_value, npe_red = _coerce_value(industry, tokens[2])
        prov_value, prov_red = _coerce_value(industry, tokens[4])

        rows.append(_make_row(
            as_of=as_of, industry=industry,
            metric=METRIC_EXPOSURE, value=exposure_value, redaction=exposure_red,
            period_length_months=None,
            source_publication=source_publication, source_page=source_page,
        ))
        rows.append(_make_row(
            as_of=as_of, industry=industry,
            metric=METRIC_NPE, value=npe_value, redaction=npe_red,
            period_length_months=None,
            source_publication=source_publication, source_page=source_page,
        ))
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
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "data_source": "pillar3_nab",
        "aggregation_level": "single_bank",
        "bank_code": "nab",
        "as_of_date": as_of,
        "period_length_months": period_length_months,
        "geography": "Total",
        "industry_published": industry,
        "metric": metric,
        "value_aud_m": value,
        "redaction_reason": redaction,
        "source_publication": source_publication,
        # NAB's industry table is a single combined exposure/NPE/provision
        # table with no APS 330 letter code. Use a stable descriptive ref.
        "source_table_ref": "EaD/NPE/Provisions by industry",
        "source_page": source_page,
    }
    if metric == METRIC_PROVISIONS:
        row[COL_PROVISION_BASIS] = PROVISION_BASIS_AASB9_STAGE3
    return row


def _find_as_at(lines: list[str]) -> date | None:
    for line in lines:
        d = _parse_as_at(line.strip())
        if d is not None:
            return d
    return None


def extract_nab_industry_rows(
    pdf_path: Path,
    *,
    industry_page: int = DEFAULT_INDUSTRY_PAGE,
    source_publication_override: str | None = None,
) -> pd.DataFrame:
    """Top-level entry point for NAB industry extraction."""
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
        rows, columns=compose_columns([COL_PROVISION_BASIS])
    )
    return df


def _infer_publication(pdf_path: Path) -> str:
    return f"NAB Pillar 3 — {pdf_path.stem}"
