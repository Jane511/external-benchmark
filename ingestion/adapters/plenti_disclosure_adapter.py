"""Plenti Group quarterly trading update adapter.

Plenti (ASX: PLT) publishes quarterly trading updates with explicit
90+ DPD arrears and net credit loss rates. Example wording (Q4 FY25):

    "Annualised net losses for the quarter were 116 basis points...
     90+ day arrears of 43 basis points at the end of the quarter."

These two metrics map to:

  * ``data_definition_class=LOSS_EXPENSE_RATE``  (annualised net losses)
  * ``data_definition_class=ARREARS_90_PLUS_DAYS``  (90+ DPD)

The adapter also captures ``loan portfolio of $X.XB`` as
``sample_size_n`` metadata (in millions of AUD) when available, and
``quarter ended <date>`` as ``period_end``.

Source URL:        https://www.plenti.com.au/shareholders
Reporting cadence: quarterly trading updates + half-yearly + annual
Coverage:          personal, automotive (consumer secured), green / renewables

NO adjustment of any kind — definitions are mapped to canonical segment
names but values are reported as-published.

Parser is line-driven: each line can both switch the current section
(via a heading regex) and emit metrics (via the loss / arrears regexes).
This keeps section attribution accurate when a single line contains
both a vertical name and the headline number.
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


# Headline metric patterns. Named groups make extraction robust against
# leading numbers that are not the metric value (e.g. "90+ day arrears").
_LOSS_RATE_RE = re.compile(
    r"annualised\s+net\s+(?:credit\s+)?losses?"
    r"(?:\s+(?:of|for\s+the\s+quarter\s+were|were))?\s+"
    r"(?P<num>\d+(?:\.\d+)?)\s*(?P<unit>bps|basis\s*points?|%)",
    re.IGNORECASE,
)

_ARREARS_RE = re.compile(
    r"90\s*\+?\s*day\s+arrears\s+(?:of|at|were)?\s*"
    r"(?P<num>\d+(?:\.\d+)?)\s*(?P<unit>bps|basis\s*points?|%)",
    re.IGNORECASE,
)

_PORTFOLIO_RE = re.compile(
    r"loan\s+portfolio\s+(?:of\s+|increased\s+to\s+|grew\s+to\s+)?"
    r"\$\s*(?P<amount>\d+(?:\.\d+)?)\s*(?P<unit>B|billion|M|million)",
    re.IGNORECASE,
)

_QUARTER_END_RE = re.compile(
    r"quarter\s+ended\s+(?P<dom>\d{1,2})\s+(?P<mon>[A-Za-z]+)\s+(?P<year>\d{4})",
    re.IGNORECASE,
)

_AS_AT_RE = re.compile(
    r"(?:as\s+at\s+|end\s+of\s+the\s+quarter\s+\(\s*)"
    r"(?P<dom>\d{1,2})\s+(?P<mon>[A-Za-z]+)\s+(?P<year>\d{4})",
    re.IGNORECASE,
)


# Section detection — Plenti often quotes per-vertical numbers on a
# single line (e.g. "Automotive loans portfolio ... 90+ day arrears of
# 38 bps"). The parser walks line by line: each line can switch the
# current section AND emit a metric attributed to that section.
_PLENTI_SECTIONS: tuple[tuple[str, tuple[re.Pattern, ...]], ...] = (
    ("consumer_secured", (
        re.compile(r"automotive\s+loans?", re.IGNORECASE),
        re.compile(r"automotive\s+lending", re.IGNORECASE),
        re.compile(r"auto\s+loans?", re.IGNORECASE),
    )),
    ("consumer_unsecured", (
        re.compile(r"personal\s+loans?", re.IGNORECASE),
        re.compile(r"green\s+loans?", re.IGNORECASE),
        re.compile(r"renewable\s+energy\s+loans?", re.IGNORECASE),
    )),
    ("sme_corporate", (
        re.compile(r"sme\s+automotive", re.IGNORECASE),
        re.compile(r"commercial\s+automotive", re.IGNORECASE),
    )),
)


_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9,
    "october": 10, "november": 11, "december": 12,
}


def _to_decimal(num: float, unit: str) -> float | None:
    """Convert (number, unit) to a decimal value. None if unit is unknown."""
    unit_lower = unit.lower()
    if "bps" in unit_lower or "basis" in unit_lower:
        return num / 10_000.0
    if "%" in unit_lower:
        return num / 100.0
    return None


def _resolve_period_end(text: str) -> date:
    """Pull a 'quarter ended …' / 'as at …' date out of the text."""
    for pat in (_QUARTER_END_RE, _AS_AT_RE):
        m = pat.search(text)
        if not m:
            continue
        mon = _MONTHS.get(m.group("mon").lower())
        if not mon:
            continue
        try:
            return date(int(m.group("year")), mon, int(m.group("dom")))
        except ValueError:
            continue
    today = date.today()
    quarter_end_month = ((today.month - 1) // 3) * 3 or 3
    day = 31 if quarter_end_month in (3, 12) else 30
    year = today.year if quarter_end_month <= today.month else today.year - 1
    return date(year, quarter_end_month, day)


def _resolve_portfolio_n(text: str) -> int | None:
    """Pull 'loan portfolio of $X.XB' / '$XXM' as an integer of millions AUD."""
    m = _PORTFOLIO_RE.search(text)
    if not m:
        return None
    amount = float(m.group("amount"))
    unit = m.group("unit").lower()
    if unit in ("b", "billion"):
        return int(round(amount * 1_000))
    return int(round(amount))


def _section_for_line(line: str) -> str | None:
    """Return the canonical segment heading present on ``line``, if any."""
    for segment, patterns in _PLENTI_SECTIONS:
        for pat in patterns:
            if pat.search(line):
                return segment
    return None


class PlentiDisclosureAdapter(NonBankDisclosureAdapter):
    SOURCE_ID = "plenti"
    SOURCE_TYPE = SourceType.NON_BANK_LISTED
    REPORTING_BASIS = (
        "Plenti quarterly trading update — explicit annualised net loss "
        "and 90+ DPD arrears bullet text"
    )
    SOURCE_URL = "https://www.plenti.com.au/shareholders"

    DEFAULT_SEGMENT = "consumer_unsecured"  # Plenti's largest single vertical

    def normalise(self, file_path: Path) -> pd.DataFrame:
        if not file_path.exists():
            logger.warning("Plenti file %s not found; emitting empty frame", file_path)
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
                "Plenti adapter found no metric matches in %s; emitting empty frame",
                file_path,
            )
            return self.empty_frame()
        return pd.DataFrame.from_records(records, columns=self.canonical_columns)

    # ------------------------------------------------------------------
    # Public, text-driven parser
    # ------------------------------------------------------------------

    def _extract_observations_from_text(self, text: str) -> list[dict[str, Any]]:
        as_of = _resolve_period_end(text)
        sample_n = _resolve_portfolio_n(text)
        out: list[dict[str, Any]] = []

        current_segment = self.DEFAULT_SEGMENT
        for raw_line in text.split("\n"):
            line = raw_line.strip()
            if not line:
                continue

            # Switch section if a vertical heading appears on this line.
            seg = _section_for_line(line)
            if seg:
                current_segment = seg

            # Loss-rate matches on this line (attributed to current section).
            for match in _LOSS_RATE_RE.finditer(line):
                value = _to_decimal(float(match.group("num")), match.group("unit"))
                if value is None or not (0.0 <= value <= 1.0):
                    continue
                out.append(_make_plenti_row(
                    segment=current_segment,
                    parameter="loss_rate",
                    ddc=DataDefinitionClass.LOSS_EXPENSE_RATE,
                    value=value,
                    as_of=as_of,
                    sample_n=sample_n,
                    snippet=match.group(0),
                    methodology="Annualised net credit losses for the quarter, "
                                "as published in Plenti's trading update.",
                    source_url=self.SOURCE_URL,
                    reporting_basis=self.REPORTING_BASIS,
                ))

            # Arrears matches on this line.
            for match in _ARREARS_RE.finditer(line):
                value = _to_decimal(float(match.group("num")), match.group("unit"))
                if value is None or not (0.0 <= value <= 1.0):
                    continue
                out.append(_make_plenti_row(
                    segment=current_segment,
                    parameter="arrears",
                    ddc=DataDefinitionClass.ARREARS_90_PLUS_DAYS,
                    value=value,
                    as_of=as_of,
                    sample_n=sample_n,
                    snippet=match.group(0),
                    methodology="90+ days past due as published in Plenti's "
                                "quarterly trading update.",
                    source_url=self.SOURCE_URL,
                    reporting_basis=self.REPORTING_BASIS,
                ))

        return _dedupe(out)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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


def _make_plenti_row(
    *,
    segment: str,
    parameter: str,
    ddc: DataDefinitionClass,
    value: float,
    as_of: date,
    sample_n: int | None,
    snippet: str,
    methodology: str,
    source_url: str,
    reporting_basis: str,
) -> dict[str, Any]:
    snippet_clean = " ".join(snippet.split())
    return {
        "source_id": "plenti",
        "source_type": SourceType.NON_BANK_LISTED,
        "segment": segment,
        "product": None,
        "parameter": parameter,
        "data_definition_class": ddc,
        "value": value,
        "as_of_date": as_of,
        "reporting_basis": reporting_basis,
        "methodology_note": f"{methodology} Snippet: {snippet_clean!r}.",
        "sample_size_n": sample_n,
        "period_start": None,
        "period_end": as_of,
        "source_url": source_url,
        "page_or_table_ref": "Plenti trading update bullet text",
    }


__all__ = ["PlentiDisclosureAdapter"]
