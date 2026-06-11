"""Westpac Pillar 3 — industry-table extraction.

Implements the per-bank industry emission for WBC under the Phase 3.B
canonical schema in :mod:`pillar3_industry_schema`. Parses two tables
from the Westpac annual / half-year Pillar 3 PDF:

- **CRB(e)** — Exposures by geographical areas, industry and residual
  maturity. Source-of-truth for ``exposure_aud_m`` per
  (industry × geography). Page-37 in the FY2025 release.
- **CRB(f)** — Non-performing exposures by geographical areas and
  industry. Source-of-truth for ``npe_aud_m`` and
  ``individually_assessed_provision_aud_m`` per
  (industry × geography), plus ``write_offs_aud_m`` (rolling 12-month,
  industry only — not split by geography). Page-38 in FY2025.

Why this is a sibling module rather than added to
:mod:`wbc_pillar3_pdf_adapter`: the existing WBC adapter is a thin
subclass of CBA's CR6/CR10 PDF parser. Industry-table extraction is a
different specialism (different table family, different page block,
different schema). Keeping the parser here preserves the existing
adapter's behaviour exactly — the new ``extract_industry_rows`` method
on :class:`WbcPillar3PdfAdapter` is a one-line delegation.

Phase 3.B guardrails honoured:

1. Every emitted row carries provenance — ``source_publication``,
   ``source_table_ref``, ``source_page``.
2. Published "-" cells emit ``value_aud_m=None`` with
   ``redaction_reason='published_as_dash'`` — never zero.
3. ``as_of_date`` is the date printed on the table block ("As at
   30 September 2025"). No carry-forward.
4. No ``contributing_banks`` column — single-bank rows only.
5. ``period_length_months=12`` for write-offs (WBC publishes a rolling
   12-month figure); ``None`` for stocks.
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
    INDUSTRY_ROW_COLUMNS,
    METRIC_EXPOSURE,
    METRIC_NPE,
    METRIC_PROVISIONS,
    METRIC_WRITE_OFFS,
    PROVISION_BASIS_AASB9_STAGE3,
    REDACTION_DASH_OR_HYPHEN,
    coerce_metric_cell,
    compose_columns,
)

logger = logging.getLogger(__name__)


# Page numbers in WBC FY2025 release. Tunable if WBC repaginates.
DEFAULT_CRB_E_PAGE = 37
DEFAULT_CRB_F_PAGE = 38


# Industries WBC publishes in CRB(e)/(f). Order matches the table.
# Used both as the matcher set and as the assertion that no published
# label is silently dropped — any line that starts with a different
# label raises rather than being skipped.
WBC_INDUSTRIES: tuple[str, ...] = (
    "Accommodation, cafes and restaurants",
    "Agriculture, forestry and fishing",
    "Construction",
    "Finance and insurance",
    "Government, administration and defence",
    "Manufacturing",
    "Mining",
    "Property",
    "Property services and business services",
    "Services",
    "Trade",
    "Transport and storage",
    "Utilities",
    "Retail lending",
    "Other",
)
_INDUSTRY_LOOKUP = {ind.lower(): ind for ind in WBC_INDUSTRIES}


# Geography blocks in CRB(e), in order.
_CRB_E_GEOGRAPHIES: tuple[tuple[str, str], ...] = (
    ("Australia",         "Total Australia"),
    ("New Zealand",       "Total New Zealand"),
    ("Other overseas",    "Total other overseas"),
)


# "As at <date>" line marker.
_AS_AT_RE = re.compile(
    r"^\s*As at\s+(\d{1,2}\s+\w+\s+\d{4})\s*$"
)


# Numeric token: integer with optional thousand-comma separators, or
# "(123)" parenthesised negative, or a literal dash "-".
_NUMERIC_TOKEN_RE = re.compile(
    r"\(\d[\d,]*\)|\d[\d,]*|-"
)


def _parse_as_at(line: str) -> date | None:
    m = _AS_AT_RE.match(line)
    if not m:
        return None
    raw = m.group(1)
    for fmt in ("%d %B %Y", "%d %b %Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _peel_numeric_tokens(line: str, n: int) -> tuple[str, list[str]] | None:
    """Peel exactly ``n`` numeric tokens from the right of ``line``.

    Returns ``(remaining_label, tokens_left_to_right)`` or ``None`` if
    fewer than ``n`` tokens are found at the right edge.
    """
    line = line.rstrip()
    tokens: list[str] = []
    cursor = line
    for _ in range(n):
        m = None
        # Find the right-most numeric token.
        for candidate in _NUMERIC_TOKEN_RE.finditer(cursor):
            m = candidate
        if m is None or m.end() != len(cursor):
            return None
        tokens.append(m.group(0))
        cursor = cursor[: m.start()].rstrip()
    tokens.reverse()
    return cursor, tokens


def _coerce_value(industry: str, token: str) -> tuple[float | None, str | None]:
    """Delegates to the shared :func:`coerce_metric_cell`.

    Phase 3.B.3 §A.2: dashes for the Government & Official Institutions
    row emit honest-zero; all other dashes emit
    ``None + REDACTION_DASH_OR_HYPHEN``. Industry context is required
    so the helper can apply the Government-row override.
    """
    return coerce_metric_cell("wbc", industry, token)


def _match_industry(remaining_label: str) -> str | None:
    """Case-insensitive exact match against the published label set."""
    return _INDUSTRY_LOOKUP.get(remaining_label.strip().lower())


def parse_crb_e_text(
    page_text: str,
    *,
    source_publication: str,
    source_page: int,
) -> list[dict[str, Any]]:
    """Parse CRB(e) page text into per-(industry × geography) rows.

    Emits :data:`METRIC_EXPOSURE` rows only.
    """
    lines = page_text.splitlines()
    as_of = _find_as_at(lines)
    if as_of is None:
        raise ValueError(
            "CRB(e): no 'As at <date>' line found — refuse to fabricate "
            "a date (guardrail 3)"
        )

    rows: list[dict[str, Any]] = []
    geo_idx = 0
    in_geo_block = False
    geographies = list(_CRB_E_GEOGRAPHIES)

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        # Geography header transitions
        if geo_idx < len(geographies) and stripped == geographies[geo_idx][0]:
            in_geo_block = True
            continue

        # End-of-geography total — close out the block, advance geo_idx
        if (geo_idx < len(geographies)
                and stripped.startswith(geographies[geo_idx][1])):
            in_geo_block = False
            geo_idx += 1
            continue

        if not in_geo_block:
            continue

        peeled = _peel_numeric_tokens(stripped, 5)
        if peeled is None:
            continue
        label_part, tokens = peeled
        industry = _match_industry(label_part)
        if industry is None:
            raise ValueError(
                f"CRB(e): unmatched industry label "
                f"{label_part!r} on line {stripped!r} — adapter must "
                f"raise rather than silently drop a published row"
            )

        # Total Exposure is the 5th (last) token.
        value, redaction = _coerce_value(industry, tokens[-1])
        rows.append({
            "data_source": "pillar3_wbc",
            "aggregation_level": "single_bank",
            "bank_code": "wbc",
            "as_of_date": as_of,
            "period_length_months": None,  # stock metric
            "geography": geographies[geo_idx][0],
            "industry_published": industry,
            "metric": METRIC_EXPOSURE,
            "value_aud_m": value,
            "redaction_reason": redaction,
            "source_publication": source_publication,
            "source_table_ref": "CRB(e)",
            "source_page": source_page,
        })

    return rows


def parse_crb_f_text(
    page_text: str,
    *,
    source_publication: str,
    source_page: int,
) -> list[dict[str, Any]]:
    """Parse CRB(f) page text into NPE / provisions / write-offs rows.

    Each industry yields 9 rows: NPE × 4 geographies + provisions × 4
    geographies + write_offs × 1 (Total only — WBC does not split
    write-offs by geography).
    """
    lines = page_text.splitlines()
    as_of = _find_as_at(lines)
    if as_of is None:
        raise ValueError(
            "CRB(f): no 'As at <date>' line found — refuse to fabricate "
            "a date (guardrail 3)"
        )

    rows: list[dict[str, Any]] = []
    geo_order = ("Australia", "New Zealand", "Other Overseas", "Total")

    started = False
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        # Skip everything before the data block.
        if not started:
            if _AS_AT_RE.match(stripped):
                started = True
            continue

        # Data block ends at the first "Total" or footnote (a-z. ).
        if stripped.startswith("Total ") or re.match(r"^[a-z]\.\s", stripped):
            break

        peeled = _peel_numeric_tokens(stripped, 9)
        if peeled is None:
            continue
        label_part, tokens = peeled
        industry = _match_industry(label_part)
        if industry is None:
            raise ValueError(
                f"CRB(f): unmatched industry label "
                f"{label_part!r} on line {stripped!r} — adapter must "
                f"raise rather than silently drop a published row"
            )

        npe_tokens = tokens[0:4]
        prov_tokens = tokens[4:8]
        write_off_token = tokens[8]

        for geo, tok in zip(geo_order, npe_tokens):
            value, redaction = _coerce_value(industry, tok)
            rows.append(_make_row(
                as_of=as_of, geography=geo, industry=industry,
                metric=METRIC_NPE, value=value, redaction=redaction,
                period_length_months=None,
                source_publication=source_publication, source_page=source_page,
            ))
        for geo, tok in zip(geo_order, prov_tokens):
            value, redaction = _coerce_value(industry, tok)
            rows.append(_make_row(
                as_of=as_of, geography=geo, industry=industry,
                metric=METRIC_PROVISIONS, value=value, redaction=redaction,
                period_length_months=None,
                source_publication=source_publication, source_page=source_page,
            ))

        # Write-offs: WBC publishes only a Total column (rolling 12-month).
        value, redaction = _coerce_value(industry, write_off_token)
        rows.append(_make_row(
            as_of=as_of, geography="Total", industry=industry,
            metric=METRIC_WRITE_OFFS, value=value, redaction=redaction,
            period_length_months=12,
            source_publication=source_publication, source_page=source_page,
        ))

    return rows


def _make_row(
    *,
    as_of: date,
    geography: str,
    industry: str,
    metric: str,
    value: float | None,
    redaction: str | None,
    period_length_months: int | None,
    source_publication: str,
    source_page: int,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "data_source": "pillar3_wbc",
        "aggregation_level": "single_bank",
        "bank_code": "wbc",
        "as_of_date": as_of,
        "period_length_months": period_length_months,
        "geography": geography,
        "industry_published": industry,
        "metric": metric,
        "value_aud_m": value,
        "redaction_reason": redaction,
        "source_publication": source_publication,
        "source_table_ref": "CRB(f)",
        "source_page": source_page,
    }
    # Phase 3.B.2 §4.2 backfill: WBC publishes "Provision for ECL" — AASB 9
    # Stage 3 ECL terminology, NOT APS 220 specific provision. Tag accordingly.
    # Finding overrides the §4.2 hypothesis that WBC = aps220_specific.
    if metric == METRIC_PROVISIONS:
        row[COL_PROVISION_BASIS] = PROVISION_BASIS_AASB9_STAGE3
    return row


def _find_as_at(lines: list[str]) -> date | None:
    for line in lines:
        d = _parse_as_at(line.strip())
        if d is not None:
            return d
    return None


def extract_wbc_industry_rows(
    pdf_path: Path,
    *,
    crb_e_page: int = DEFAULT_CRB_E_PAGE,
    crb_f_page: int = DEFAULT_CRB_F_PAGE,
    source_publication_override: str | None = None,
) -> pd.DataFrame:
    """Top-level entry point for WBC industry extraction."""
    pdf_path = Path(pdf_path)
    with pdfplumber.open(pdf_path) as pdf:
        crb_e_text = pdf.pages[crb_e_page - 1].extract_text() or ""
        crb_f_text = pdf.pages[crb_f_page - 1].extract_text() or ""

    publication = source_publication_override or _infer_publication(pdf_path)

    rows: list[dict[str, Any]] = []
    rows.extend(parse_crb_e_text(
        crb_e_text,
        source_publication=publication,
        source_page=crb_e_page,
    ))
    rows.extend(parse_crb_f_text(
        crb_f_text,
        source_publication=publication,
        source_page=crb_f_page,
    ))

    df = pd.DataFrame.from_records(
        rows, columns=compose_columns([COL_PROVISION_BASIS])
    )
    return df


def _infer_publication(pdf_path: Path) -> str:
    """Best-effort label like 'WBC Pillar 3 — September 2025' from filename.

    Falls back to the filename itself when the pattern doesn't match.
    """
    name = pdf_path.stem
    return f"WBC Pillar 3 — {name}"
