"""Liberty Financial Group annual report adapter.

Liberty (ASX: LFG) publishes an annual report whose financial-statements
credit risk note (typically Note 5 or 6) carries impaired loans by
segment — residential mortgages, commercial property, motor / asset
finance, and personal / SME. Each line maps to:

    parameter='impaired'
    data_definition_class=IMPAIRED_LOANS_RATIO

These provide useful upper-band reality checks on commercial property
and residential mortgages for non-bank peers.

Source URL:        https://www.lfgroup.com.au/reports/asx-announcements
Reporting cadence: annual (with half-year snapshots)
Coverage:          residential mortgages, commercial property, motor, SME

NO adjustment of any kind — definitions are mapped to canonical segment
names (see ingestion/segment_mapping.yaml) but values are reported
as-published.

Parser strategy
---------------
The credit-risk note is a flat table whose rows look like:

    Residential mortgages    234   12,540   1.87%
    Commercial property       89    1,210   7.36%
    Motor / asset finance     31    2,830   1.10%
    SME loans                 14      460   3.04%

The adapter walks the credit-risk-note section with a regex that picks
the trailing percent-or-decimal column as the impaired ratio. Where the
table publishes a numerator and denominator instead, the parser
computes ratio = numerator / denominator.

Text-driven so the per-adapter test exercises the regex against a
string fixture without committing a binary PDF.
"""
from __future__ import annotations

import logging
import re
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ingestion.adapters.non_bank_base import NonBankDisclosureAdapter
from src.models import DataDefinitionClass, SourceType

logger = logging.getLogger(__name__)


# Heading regex — the credit risk section runs from the matching header
# line until the next note-style heading.
_NOTE_HEADER_RE = re.compile(
    r"(?:credit\s+risk(?:\s+\(continued\))?|loans\s+and\s+advances)\b",
    re.IGNORECASE,
)
_NOTE_END_RE = re.compile(
    r"(?:liquidity\s+risk|market\s+risk|operational\s+risk|"
    r"capital\s+management|note\s+\d+)",
    re.IGNORECASE,
)


# Canonical segment for each Liberty book label. Order matters — most-
# specific labels first so "commercial property" matches before "commercial".
_LIBERTY_SEGMENTS: tuple[tuple[str, str], ...] = (
    ("residential mortgages", "residential_mortgage"),
    ("residential mortgage", "residential_mortgage"),
    ("home loans", "residential_mortgage"),
    ("commercial property loans", "commercial_property"),
    ("commercial property", "commercial_property"),
    ("commercial real estate", "commercial_property"),
    ("motor / asset finance", "consumer_secured"),
    ("motor and asset finance", "consumer_secured"),
    ("motor finance", "consumer_secured"),
    ("auto loans", "consumer_secured"),
    ("sme loans", "sme_corporate"),
    ("sme", "sme_corporate"),
    ("personal loans", "consumer_unsecured"),
    ("bridging", "bridging_residential"),
)


# Row patterns for the impaired-loans table:
# Pattern A — explicit ratio column at end: "Residential mortgages ... 1.87%"
_ROW_RATIO_RE = re.compile(
    r"^\s*(?P<label>[A-Za-z][A-Za-z /&-]+?)\s+"
    r"(?P<rest>[\d,.\s$%-]+?)$",
    re.MULTILINE,
)

# Pattern B — explicit numerator and denominator columns:
#   "Residential mortgages   234   12,540"
# captured by stripping commas and picking the last two integers in a row.

_AS_OF_RE = re.compile(
    r"(?:as\s+at|year\s+ended|period\s+ended|reporting\s+date)\s+"
    r"(?P<dom>\d{1,2})\s+(?P<mon>[A-Za-z]+)\s+(?P<year>\d{4})",
    re.IGNORECASE,
)
_FY_RE = re.compile(
    r"\bFY\s*(?P<year>\d{2,4})\b",
    re.IGNORECASE,
)

_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9,
    "october": 10, "november": 11, "december": 12,
}


def _resolve_period_end(text: str) -> date:
    m = _AS_OF_RE.search(text)
    if m:
        mon = _MONTHS.get(m.group("mon").lower())
        if mon:
            try:
                return date(int(m.group("year")), mon, int(m.group("dom")))
            except ValueError:
                pass
    fy = _FY_RE.search(text)
    if fy:
        year_str = fy.group("year")
        year = int(year_str)
        if year < 100:
            year += 2000
        # Liberty fiscal year ends 30 June.
        return date(year, 6, 30)
    today = date.today()
    year = today.year if today.month >= 7 else today.year - 1
    return date(year, 6, 30)


def _classify_label(label: str) -> str | None:
    lower = label.strip().lower()
    if not lower:
        return None
    for needle, segment in _LIBERTY_SEGMENTS:
        if needle in lower:
            return segment
    return None


def _extract_ratio_from_rest(rest: str) -> float | None:
    """Return the last column as a decimal (impaired ratio) when sensible.

    Handles three common formats:
      1. trailing "1.87%" (or "1.87 %") → 0.0187
      2. trailing decimal like "0.0187" with no % → 0.0187 (already decimal)
      3. integers numerator + denominator: " 234   12,540 " → 234 / 12540
         (last two numbers, taken when neither has a percent sign)
    """
    s = rest.strip()
    pct_match = re.search(r"(\d+(?:\.\d+)?)\s*%\s*$", s)
    if pct_match:
        return float(pct_match.group(1)) / 100.0

    # Strip $ and commas, then collect numeric tokens
    cleaned = s.replace("$", "")
    nums = re.findall(r"-?\d+(?:\.\d+)?", cleaned.replace(",", ""))
    if not nums:
        return None
    if len(nums) >= 2:
        # If the last token is a small decimal, treat it as a ratio
        last = float(nums[-1])
        if 0.0 < last < 1.0:
            return last
        # numerator / denominator (last two integers)
        try:
            num = float(nums[-2])
            den = float(nums[-1])
            if den > 0 and num >= 0 and num < den:
                return num / den
        except ValueError:
            return None
    last = float(nums[-1])
    if 0.0 < last < 1.0:
        return last
    return None


class LibertyAnnualAdapter(NonBankDisclosureAdapter):
    SOURCE_ID = "liberty"
    SOURCE_TYPE = SourceType.NON_BANK_LISTED
    REPORTING_BASIS = (
        "Liberty Financial Group annual report — credit risk note "
        "(impaired loans by segment)"
    )
    SOURCE_URL = "https://www.lfgroup.com.au/reports/asx-announcements"

    def normalise(self, file_path: Path) -> pd.DataFrame:
        if not file_path.exists():
            logger.warning("Liberty file %s not found; emitting empty frame", file_path)
            return self.empty_frame()

        try:
            import pdfplumber
        except ImportError:
            logger.warning(
                "pdfplumber not installed; cannot parse %s — emitting empty frame",
                file_path,
            )
            return self.empty_frame()

        try:
            with pdfplumber.open(file_path) as pdf:
                text = "\n".join((p.extract_text() or "") for p in pdf.pages)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("pdfplumber failed on %s: %s", file_path, exc)
            return self.empty_frame()

        records = self._extract_observations_from_text(text)
        if not records:
            logger.warning(
                "Liberty adapter found no impaired-loan rows in %s; "
                "emitting empty frame",
                file_path,
            )
            return self.empty_frame()
        return pd.DataFrame.from_records(records, columns=self.canonical_columns)

    # ------------------------------------------------------------------
    # Public, text-driven parser
    # ------------------------------------------------------------------

    def _extract_observations_from_text(self, text: str) -> list[dict[str, Any]]:
        """Walk credit-risk note blocks and emit impaired-loans observations."""
        as_of = _resolve_period_end(text)
        out: list[dict[str, Any]] = []

        for block in _iter_credit_risk_blocks(text):
            for row in _ROW_RATIO_RE.finditer(block):
                label = row.group("label")
                rest = row.group("rest")
                segment = _classify_label(label)
                if not segment:
                    continue
                value = _extract_ratio_from_rest(rest)
                if value is None:
                    continue
                if not (0.0 <= value <= 1.0):
                    logger.warning(
                        "Liberty: dropping implausible ratio=%.6f for "
                        "label=%r segment=%s",
                        value, label, segment,
                    )
                    continue
                out.append(_make_liberty_row(
                    segment=segment,
                    published_label=label.strip(),
                    value=value,
                    as_of=as_of,
                    source_url=self.SOURCE_URL,
                    reporting_basis=self.REPORTING_BASIS,
                ))

        return _dedupe(out)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _iter_credit_risk_blocks(text: str):
    """Yield substrings that look like credit-risk note tables."""
    for header in _NOTE_HEADER_RE.finditer(text):
        start = header.start()
        # Search for the next note boundary AFTER the header so the block
        # contains the impaired-loans table itself.
        tail = text[header.end():]
        end_match = _NOTE_END_RE.search(tail)
        end = (header.end() + end_match.start()) if end_match else len(text)
        yield text[start:end]


def _dedupe(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[Any, ...]] = set()
    out: list[dict[str, Any]] = []
    for r in records:
        key = (
            r["segment"], r["parameter"], r["data_definition_class"],
            round(r["value"], 8), r["as_of_date"],
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def _make_liberty_row(
    *,
    segment: str,
    published_label: str,
    value: float,
    as_of: date,
    source_url: str,
    reporting_basis: str,
) -> dict[str, Any]:
    return {
        "source_id": "liberty",
        "source_type": SourceType.NON_BANK_LISTED,
        "segment": segment,
        "product": None,
        "parameter": "impaired",
        "data_definition_class": DataDefinitionClass.IMPAIRED_LOANS_RATIO,
        "value": value,
        "as_of_date": as_of,
        "reporting_basis": reporting_basis,
        "methodology_note": (
            f"Impaired loans / total exposure for segment {published_label!r} "
            f"as published in Liberty annual report credit risk note."
        ),
        "sample_size_n": None,
        "period_start": None,
        "period_end": as_of,
        "source_url": source_url,
        "page_or_table_ref": "Liberty annual report — credit risk note",
    }


__all__ = ["LibertyAnnualAdapter"]
