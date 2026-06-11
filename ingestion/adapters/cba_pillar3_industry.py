"""CBA Pillar 3 — industry-table extraction (sibling helper).

Implements the CBA-specific parser for the canonical industry schema.
Sibling to :mod:`wbc_pillar3_industry`, :mod:`nab_pillar3_industry`,
and :mod:`anz_pillar3_industry`. CBA needs its own module because it
publishes industry data in a structurally distinct shape (see below).

CBA publishes industry data across two structurally distinct tables:

- **CRB(e)(ii)** — Credit exposures by portfolio × industry. Wide
  matrix (10 portfolios × 15 industries × 2 periods). Pages 36–37 in
  the FY2025 release.
- **CRB(f)(i)** — Non-performing exposures, specific provision balance,
  and actual losses by industry. Flat industry list (15 industries × 3
  metrics × 2 periods). Pages 39–40.

CBA-specific quirks:

- **Portfolio-type × industry matrix shape (CRB(e)(ii))** — emits one
  exposure row per (portfolio_type × industry × period) with the
  ``portfolio_type`` optional column populated from
  :data:`pillar3_industry_schema.PORTFOLIO_TYPE_VALUES`.
- **Multi-line portfolio label "Corporate (incl. Large and SME
  corporate)"** — wraps across 3 PDF lines. Parser uses positional
  ordering (10 data rows per half, in the order declared in
  :data:`_CBA_PORTFOLIO_ORDER`) rather than label matching.
- **Half-year vs full-year actual losses** — CBA publishes both:
  Jun-25 = full year (`period_length_months=12`), Dec-24 = half year
  (`period_length_months=6`). The aggregation layer (3.C) must filter
  to comparable periods before summing alongside WBC's rolling
  12-month write-offs.
- **Provision basis** — `aps220_specific` (CBA's CRB(f)(i) header
  reads "Specific provision balance"; APS 220 prudential terminology).
  CBA is the only Big-4 bank with this basis.
- **APS 120 securitisation exclusion** — CRB(f)(i) footnote 1 excludes
  NPE in securitisation entities meeting APS 120 capital-relief
  requirements. Captured by appending ``"(APS 120 securitisation
  excluded)"`` to ``source_publication`` for every CBA NPE row.
- **CRB(e)(ii) dashes are structural zeros** — per recon §8 ruling
  (provisional, awaiting Section C sign-off), the entire CRB(e)(ii)
  matrix is on the table-level honest-zero override list in
  :data:`pillar3_industry_schema._HONEST_ZERO_TABLES`. The
  :func:`coerce_metric_cell` helper applies the override automatically
  when called with ``source_table_ref="CRB(e)(ii)"``. CRB(f)(i)
  follows the row-level Govt-only honest-zero rule (Section A
  harmonisation).
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
    COL_PORTFOLIO_TYPE,
    COL_PROVISION_BASIS,
    INDUSTRY_ROW_COLUMNS,
    METRIC_EXPOSURE,
    METRIC_NPE,
    METRIC_PROVISIONS,
    METRIC_WRITE_OFFS,
    PORTFOLIO_TYPE_CORPORATE,
    PORTFOLIO_TYPE_FINANCIAL,
    PORTFOLIO_TYPE_OTHER_ASSETS,
    PORTFOLIO_TYPE_OTHER_RETAIL,
    PORTFOLIO_TYPE_QRR,
    PORTFOLIO_TYPE_RBNZ,
    PORTFOLIO_TYPE_RES_MORTGAGE,
    PORTFOLIO_TYPE_SME_RETAIL,
    PORTFOLIO_TYPE_SOVEREIGN,
    PORTFOLIO_TYPE_SPECIALISED,
    PROVISION_BASIS_APS220,
    coerce_metric_cell,
    compose_columns,
)

logger = logging.getLogger(__name__)


DEFAULT_CRB_E_PAGES = (36, 37)  # left/right halves split by period across pages
DEFAULT_CRB_F_PAGES = (39, 40)  # Jun-25, Dec-24 respectively


# Portfolio order in CRB(e)(ii) — positional matching (10 portfolios per half).
_CBA_PORTFOLIO_ORDER: tuple[str, ...] = (
    PORTFOLIO_TYPE_CORPORATE,
    PORTFOLIO_TYPE_SOVEREIGN,
    PORTFOLIO_TYPE_FINANCIAL,
    PORTFOLIO_TYPE_SME_RETAIL,
    PORTFOLIO_TYPE_RES_MORTGAGE,
    PORTFOLIO_TYPE_QRR,
    PORTFOLIO_TYPE_OTHER_RETAIL,
    PORTFOLIO_TYPE_SPECIALISED,
    PORTFOLIO_TYPE_OTHER_ASSETS,
    PORTFOLIO_TYPE_RBNZ,
)

# Industry order in CRB(e)(ii) — positional matching, 8 industries left half,
# 7 industries + Total right half. The Total column is intentionally NOT
# emitted (it is the row sum).
_CBA_INDUSTRIES_LEFT: tuple[str, ...] = (
    "Consumer",
    "Finance & Insurance",
    "Business Services",
    "Agriculture & Forestry",
    "Construction",
    "Mining, Oil & Gas",
    "Wholesale & Retail Trade",
    "Transport & Storage",
)
_CBA_INDUSTRIES_RIGHT: tuple[str, ...] = (
    "Manufacturing",
    "Commercial Property",
    "Government Administration & Defence",
    "Health & Community Services",
    "Entertainment, Leisure & Tourism",
    "Electricity, Gas & Water",
    "Other",
)

# CRB(f)(i) industry order (slightly different from CRB(e)(ii)).
_CBA_CRB_F_INDUSTRIES: tuple[str, ...] = (
    "Consumer",
    "Government Administration & Defence",
    "Finance & Insurance",
    "Business Services",
    "Agriculture & Forestry",
    "Mining, Oil & Gas",
    "Manufacturing",
    "Electricity, Gas & Water",
    "Construction",
    "Wholesale & Retail Trade",
    "Transport & Storage",
    "Commercial Property",
    "Entertainment, Leisure & Tourism",
    "Health & Community Services",
    "Other",
)
_CRB_F_INDUSTRY_LOOKUP = {ind.lower(): ind for ind in _CBA_CRB_F_INDUSTRIES}


# CBA fiscal year convention: full year ends 30 June, half year ends 31 Dec.
_PERIOD_HEADER_RE = re.compile(
    r"^\s*(\d{1,2}\s+(?:June|December|Jun|Dec)\s+\d{4})\s*$"
)
_NUMERIC_TOKEN_RE = re.compile(r"\(\d[\d,]*\)|\d[\d,]*|–|-")


def _parse_period_header(line: str) -> date | None:
    m = _PERIOD_HEADER_RE.match(line)
    if not m:
        return None
    raw = m.group(1)
    for fmt in ("%d %B %Y", "%d %b %Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _normalise_dash(token: str) -> str:
    """CBA uses en-dash "–" (U+2013) for missing/zero. Normalise to "-"
    so the shared coerce helper handles it."""
    return "-" if token in {"-", "–", "—"} else token


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
        tokens.append(_normalise_dash(m.group(0)))
        cursor = cursor[: m.start()].rstrip()
    tokens.reverse()
    return cursor, tokens


# ---------------------------------------------------------------------------
# CRB(e)(ii) — exposures by portfolio × industry
# ---------------------------------------------------------------------------

# Period the table refers to is established by the period-header line
# ("30 June 2025" or "31 December 2024") that precedes the matrix.

def parse_crb_e_text(
    page_text: str,
    *,
    source_publication: str,
    source_page: int,
    period_override: date | None = None,
) -> list[dict[str, Any]]:
    """Parse one CBA CRB(e)(ii) page.

    Each page contains a single period's matrix split into two halves
    (left = 8 industries, right = 7 industries + Total). 10 portfolio
    rows per half. We collect 10 data rows per half via positional
    counting: any line whose right edge is exactly 8 numeric tokens
    is a data row; the i-th such row in each half maps to the i-th
    portfolio in :data:`_CBA_PORTFOLIO_ORDER`.

    The "Total credit exposures" row terminates each half.
    """
    lines = page_text.splitlines()
    period = period_override or _find_period(lines)
    if period is None:
        raise ValueError(
            "CBA CRB(e)(ii): no period header (e.g. '30 June 2025') "
            "found — refuse to fabricate a date (guardrail 3)"
        )

    halves = _split_into_halves(lines)
    rows: list[dict[str, Any]] = []
    for half_idx, half_lines in enumerate(halves):
        industries = _CBA_INDUSTRIES_LEFT if half_idx == 0 else _CBA_INDUSTRIES_RIGHT
        # Right half publishes "Total" as the 8th (last) column — not emitted.
        emit_count = len(industries)
        data_rows = _collect_data_rows(half_lines, n_tokens=8, max_rows=len(_CBA_PORTFOLIO_ORDER))
        for portfolio_idx, tokens in enumerate(data_rows):
            if portfolio_idx >= len(_CBA_PORTFOLIO_ORDER):
                break
            portfolio = _CBA_PORTFOLIO_ORDER[portfolio_idx]
            for col_idx in range(emit_count):
                token = tokens[col_idx]
                value, redaction = coerce_metric_cell(
                    "cba", "<matrix-cell>", token,
                    source_table_ref="CRB(e)(ii)",
                )
                rows.append(_make_exposure_row(
                    period=period,
                    portfolio=portfolio,
                    industry=industries[col_idx],
                    value=value,
                    redaction=redaction,
                    source_publication=source_publication,
                    source_page=source_page,
                ))
    return rows


def _split_into_halves(lines: list[str]) -> tuple[list[str], list[str]]:
    """Divide a CRB(e)(ii) page into left-half and right-half line groups.

    The right half is preceded by the "Industry Sector (continued)"
    marker. The left half precedes that marker.
    """
    # The right-half marker is the standalone column-header line
    # exactly "Industry Sector (continued)". The CRB(e)(ii) page-2
    # section header contains the same phrase but with leading text
    # ("CRB(e)(ii): Credit exposures by portfolio type and industry
    # sector (continued)") — must NOT be matched. Use exact-equality
    # (case-insensitive) on the stripped line.
    split_idx = None
    for i, line in enumerate(lines):
        if line.strip().lower() == "industry sector (continued)":
            split_idx = i
            break
    if split_idx is None:
        return (lines, [])
    return (lines[:split_idx], lines[split_idx:])


def _collect_data_rows(
    lines: list[str], *, n_tokens: int, max_rows: int,
) -> list[list[str]]:
    """Return the first ``max_rows`` data rows (lines whose right edge
    is exactly ``n_tokens`` numeric tokens). Stops at the "Total
    credit exposures" terminator."""
    out: list[list[str]] = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.lower().startswith("total credit exposures"):
            break
        peeled = _peel_numeric_tokens(stripped, n_tokens)
        if peeled is None:
            continue
        _, tokens = peeled
        out.append(tokens)
        if len(out) >= max_rows:
            break
    return out


def _find_period(lines: list[str]) -> date | None:
    for line in lines:
        d = _parse_period_header(line.strip())
        if d is not None:
            return d
    return None


def _make_exposure_row(
    *,
    period: date,
    portfolio: str,
    industry: str,
    value: float | None,
    redaction: str | None,
    source_publication: str,
    source_page: int,
) -> dict[str, Any]:
    return {
        "data_source": "pillar3_cba",
        "aggregation_level": "single_bank",
        "bank_code": "cba",
        "as_of_date": period,
        "period_length_months": None,  # stock metric
        "geography": "Total",
        "industry_published": industry,
        "metric": METRIC_EXPOSURE,
        "value_aud_m": value,
        "redaction_reason": redaction,
        "source_publication": source_publication,
        "source_table_ref": "CRB(e)(ii)",
        "source_page": source_page,
        COL_PORTFOLIO_TYPE: portfolio,
    }


# ---------------------------------------------------------------------------
# CRB(f)(i) — NPE / specific provision / actual losses by industry
# ---------------------------------------------------------------------------

# Determines whether actual losses for a given period header is "Full
# year" (Jun-25 → 12) or "Half year" (Dec-24 → 6).
_FULL_YEAR_MONTHS = 12
_HALF_YEAR_MONTHS = 6


def parse_crb_f_text(
    page_text: str,
    *,
    source_publication: str,
    source_page: int,
    period_length_months: int,
) -> list[dict[str, Any]]:
    """Parse one CBA CRB(f)(i) page (one period).

    The page covers a single as-at date; ``period_length_months`` is
    set by the caller based on whether this is a full-year (12) or
    half-year (6) page.

    Per industry: 3 rows (NPE, specific provision, actual losses).
    NPE rows append "(APS 120 securitisation excluded)" to
    ``source_publication`` per CBA footnote 1.
    """
    lines = page_text.splitlines()
    period = _find_period(lines)
    if period is None:
        raise ValueError(
            "CBA CRB(f)(i): no period header found — refuse to "
            "fabricate a date (guardrail 3)"
        )

    rows: list[dict[str, Any]] = []
    started = False
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if not started:
            if _parse_period_header(stripped):
                started = True
            continue
        if stripped.lower().startswith("total "):
            break
        peeled = _peel_numeric_tokens(stripped, 3)
        if peeled is None:
            continue
        label_part, tokens = peeled
        industry = _CRB_F_INDUSTRY_LOOKUP.get(label_part.strip().lower())
        if industry is None:
            # Skip header / footnote rows (e.g. "Industry Sector $M $M $M")
            if any(c.isalpha() for c in label_part):
                if "$m" in label_part.lower() or "industry sector" in label_part.lower():
                    continue
                raise ValueError(
                    f"CBA CRB(f)(i): unmatched industry label "
                    f"{label_part!r} on line {stripped!r}"
                )
            continue

        npe_v, npe_r = coerce_metric_cell("cba", industry, tokens[0])
        prov_v, prov_r = coerce_metric_cell("cba", industry, tokens[1])
        loss_v, loss_r = coerce_metric_cell("cba", industry, tokens[2])

        # NPE rows carry the APS 120 exclusion note.
        npe_publication = (
            f"{source_publication} (APS 120 securitisation excluded)"
        )
        rows.append(_make_flat_row(
            period=period, industry=industry, metric=METRIC_NPE,
            value=npe_v, redaction=npe_r,
            period_length_months=None,
            source_publication=npe_publication,
            source_page=source_page,
        ))
        rows.append(_make_flat_row(
            period=period, industry=industry, metric=METRIC_PROVISIONS,
            value=prov_v, redaction=prov_r,
            period_length_months=None,
            source_publication=source_publication,
            source_page=source_page,
        ))
        rows.append(_make_flat_row(
            period=period, industry=industry, metric=METRIC_WRITE_OFFS,
            value=loss_v, redaction=loss_r,
            period_length_months=period_length_months,
            source_publication=source_publication,
            source_page=source_page,
        ))
    return rows


def _make_flat_row(
    *,
    period: date,
    industry: str,
    metric: str,
    value: float | None,
    redaction: str | None,
    period_length_months: int | None,
    source_publication: str,
    source_page: int,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "data_source": "pillar3_cba",
        "aggregation_level": "single_bank",
        "bank_code": "cba",
        "as_of_date": period,
        "period_length_months": period_length_months,
        "geography": "Total",
        "industry_published": industry,
        "metric": metric,
        "value_aud_m": value,
        "redaction_reason": redaction,
        "source_publication": source_publication,
        "source_table_ref": "CRB(f)(i)",
        "source_page": source_page,
    }
    if metric == METRIC_PROVISIONS:
        row[COL_PROVISION_BASIS] = PROVISION_BASIS_APS220
    return row


# ---------------------------------------------------------------------------
# Top-level entry point
# ---------------------------------------------------------------------------

def extract_cba_industry_rows(
    pdf_path: Path,
    *,
    crb_e_pages: tuple[int, int] = DEFAULT_CRB_E_PAGES,
    crb_f_pages: tuple[int, int] = DEFAULT_CRB_F_PAGES,
    source_publication_override: str | None = None,
) -> pd.DataFrame:
    pdf_path = Path(pdf_path)
    publication = source_publication_override or _infer_publication(pdf_path)

    rows: list[dict[str, Any]] = []
    with pdfplumber.open(pdf_path) as pdf:
        # CRB(e)(ii) — both pages. Each page covers one period (Jun-25
        # on p36, Dec-24 on p37 in the FY2025 release).
        for page_num in crb_e_pages:
            text = pdf.pages[page_num - 1].extract_text() or ""
            rows.extend(parse_crb_e_text(
                text, source_publication=publication, source_page=page_num,
            ))

        # CRB(f)(i) — Jun-25 (full year) on first page, Dec-24 (half
        # year) on second.
        page_jun, page_dec = crb_f_pages
        text_jun = pdf.pages[page_jun - 1].extract_text() or ""
        text_dec = pdf.pages[page_dec - 1].extract_text() or ""
        rows.extend(parse_crb_f_text(
            text_jun, source_publication=publication,
            source_page=page_jun, period_length_months=_FULL_YEAR_MONTHS,
        ))
        rows.extend(parse_crb_f_text(
            text_dec, source_publication=publication,
            source_page=page_dec, period_length_months=_HALF_YEAR_MONTHS,
        ))

    df = pd.DataFrame.from_records(
        rows,
        columns=compose_columns([COL_PROVISION_BASIS, COL_PORTFOLIO_TYPE]),
    )
    return df


def _infer_publication(pdf_path: Path) -> str:
    return f"CBA Pillar 3 — {pdf_path.stem}"
