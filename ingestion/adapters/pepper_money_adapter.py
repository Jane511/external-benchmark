"""Pepper Money half-yearly results disclosure adapter.

Pepper Money (ASX: PPM) publishes half-yearly results presentations
that include explicit asset-finance loss expense, residential mortgage
arrears, and SME loan loss expense. Each metric maps to a different
``data_definition_class`` so consumers can filter to compatible numbers
rather than blending heterogeneous definitions.

Source URL:        https://www.peppermoney.com.au/about/debt-investors
Reporting cadence: half-yearly + annual
Coverage:          residential mortgages, asset finance, SME / commercial

NO adjustment of any kind — definitions are mapped to canonical segment
names (see ingestion/segment_mapping.yaml) but values are reported
as-published.

Parser strategy
---------------
Pepper's results presentation publishes the headline numbers in slide
text rather than tables, so the parser is regex-driven over the
extracted page text. Each segment is identified by a section heading
(e.g. "Asset Finance"); within a section, loss-rate / 30+ DPD / 90+ DPD
patterns each capture the published number directly via a named group.

The adapter is text-driven so the parsing logic is testable without
committing a binary PDF — the public ``_extract_observations_from_text``
is what the per-adapter test exercises.
"""
from __future__ import annotations

import logging
import re
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from ingestion.adapters.non_bank_base import NonBankDisclosureAdapter
from src.models import DataDefinitionClass, SourceType

logger = logging.getLogger(__name__)


# Reporting period — pulled from a "1H FY25 Results" / "FY25 Annual Results"
# style heading on the cover slide. Falls back to the most recent half-year
# end if no header is found.
_PERIOD_RE = re.compile(
    r"(?P<half>1H|2H|FY)\s*(?:FY)?\s*(?P<year>\d{2,4})",
    re.IGNORECASE,
)


# Section detection — a single canonical-segment block runs from one of
# these headings until the next section heading or end of text.
_PEPPER_SECTIONS: tuple[tuple[str, tuple[str, ...]], ...] = (
    # (canonical_segment, ordered tuple of synonymous heading regexes)
    ("residential_mortgage", (
        r"residential\s+mortgages?",
        r"mortgages?\s+portfolio",
        r"home\s+loans",
    )),
    ("consumer_secured", (
        r"asset\s+finance",
        r"automotive",
        r"auto\s+loans",
    )),
    ("sme_corporate", (
        r"sme\s*/?\s*commercial",
        r"sme\s+business",
        r"commercial\s+lending",
    )),
)


# Per-metric extraction patterns. Each pattern uses a named ``num`` group
# capturing the percentage immediately after the metric phrase, plus a
# named ``unit`` group (always "%") so the converter is unambiguous.
# We intentionally do NOT use bps/basis-points here because Pepper
# generally publishes percentages in its results pack.
_METRIC_PATTERNS: tuple[tuple[str, DataDefinitionClass, str, re.Pattern], ...] = (
    (
        "loss_rate",
        DataDefinitionClass.LOSS_EXPENSE_RATE,
        "loan loss expense / average book",
        re.compile(
            r"(?:loan\s+loss\s+expense|loss\s+expense\s+ratio|"
            r"loss\s+rate|net\s+loss\s+rate|cost\s+of\s+risk)"
            r"\s*(?:of)?\s*"
            r"(?P<num>\d+(?:\.\d+)?)\s*(?P<unit>%|bps|basis\s*points?)",
            re.IGNORECASE,
        ),
    ),
    (
        "arrears",
        DataDefinitionClass.ARREARS_90_PLUS_DAYS,
        "90+ days past due as published by Pepper",
        re.compile(
            r"90\s*\+?\s*(?:days?|dpd|day\s*arrears?)\s+(?:arrears\s+)?"
            r"(?:of|at|were)?\s*"
            r"(?P<num>\d+(?:\.\d+)?)\s*(?P<unit>%|bps|basis\s*points?)",
            re.IGNORECASE,
        ),
    ),
    (
        "arrears",
        DataDefinitionClass.ARREARS_30_PLUS_DAYS,
        "30+ days past due as published by Pepper",
        re.compile(
            r"30\s*\+?\s*(?:days?|dpd|day\s*arrears?)\s+(?:arrears\s+)?"
            r"(?:of|at|were)?\s*"
            r"(?P<num>\d+(?:\.\d+)?)\s*(?P<unit>%|bps|basis\s*points?)",
            re.IGNORECASE,
        ),
    ),
)


def _to_decimal(num: float, unit: str) -> float | None:
    unit_lower = unit.lower()
    if "bps" in unit_lower or "basis" in unit_lower:
        return num / 10_000.0
    if "%" in unit_lower:
        return num / 100.0
    return None


def _resolve_period_end(text: str) -> date:
    """Coerce a 1H/2H/FY YY heading into a period-end date."""
    m = _PERIOD_RE.search(text)
    if not m:
        # Pepper FY = year ending 30 June; default to last June end.
        today = date.today()
        year = today.year if today.month >= 7 else today.year - 1
        return date(year, 6, 30)
    half = m.group("half").upper()
    year_str = m.group("year")
    year = int(year_str)
    if year < 100:
        year += 2000
    if half == "1H":
        # 1H FY25 = half year ending 31 December 2024
        return date(year - 1, 12, 31)
    if half == "2H" or half == "FY":
        return date(year, 6, 30)
    return date(year, 6, 30)


class PepperMoneyAdapter(NonBankDisclosureAdapter):
    SOURCE_ID = "pepper"
    SOURCE_TYPE = SourceType.NON_BANK_LISTED
    REPORTING_BASIS = (
        "Pepper Money half-yearly results presentation — "
        "asset finance + residential + SME credit performance"
    )
    SOURCE_URL = "https://www.peppermoney.com.au/about/debt-investors"

    def normalise(self, file_path: Path) -> pd.DataFrame:
        """Parse a Pepper Money results presentation PDF.

        Pepper publishes loss-rate and arrears figures by segment in
        narrative slide text rather than tables. The adapter walks each
        segment block and emits one observation per published metric.
        """
        if not file_path.exists():
            logger.warning("Pepper file %s not found; emitting empty frame", file_path)
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
                "Pepper adapter found no metric matches in %s; emitting empty frame",
                file_path,
            )
            return self.empty_frame()
        return pd.DataFrame.from_records(records, columns=self.canonical_columns)

    # ------------------------------------------------------------------
    # Public, text-driven parser
    # ------------------------------------------------------------------

    def _extract_observations_from_text(self, text: str) -> list[dict[str, Any]]:
        """Parse extracted-PDF text into canonical observation records."""
        as_of = _resolve_period_end(text)
        out: list[dict[str, Any]] = []
        for segment, block in _iter_section_blocks(text):
            for parameter, ddc, methodology, pat in _METRIC_PATTERNS:
                for match in pat.finditer(block):
                    snippet = match.group(0)
                    value = _to_decimal(
                        float(match.group("num")), match.group("unit"),
                    )
                    if value is None:
                        continue
                    if not (0.0 <= value <= 1.0):
                        logger.warning(
                            "Pepper: dropping implausible %s=%.6f for %s "
                            "(snippet=%r)",
                            parameter, value, segment, snippet,
                        )
                        continue
                    out.append(_make_pepper_row(
                        segment=segment,
                        parameter=parameter,
                        ddc=ddc,
                        value=value,
                        as_of=as_of,
                        snippet=snippet,
                        methodology=methodology,
                        source_url=self.SOURCE_URL,
                        reporting_basis=self.REPORTING_BASIS,
                    ))
        return _dedupe(out)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _iter_section_blocks(text: str) -> Iterable[tuple[str, str]]:
    """Yield (canonical_segment, block_text) pairs.

    Sections are demarcated by their heading regex; the block runs from
    one heading to the next (or end of text). The same canonical segment
    can be yielded more than once if the source mentions it multiple
    times, which is fine — duplicates are deduped by (segment, parameter,
    ddc, value) downstream.
    """
    hits: list[tuple[int, str]] = []
    for segment, heading_patterns in _PEPPER_SECTIONS:
        for raw in heading_patterns:
            for m in re.finditer(raw, text, flags=re.IGNORECASE):
                hits.append((m.start(), segment))
    if not hits:
        return
    hits.sort()
    for i, (start, segment) in enumerate(hits):
        end = hits[i + 1][0] if i + 1 < len(hits) else len(text)
        yield segment, text[start:end]


def _dedupe(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop duplicate (segment, parameter, ddc, value, as_of) rows."""
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


def _make_pepper_row(
    *,
    segment: str,
    parameter: str,
    ddc: DataDefinitionClass,
    value: float,
    as_of: date,
    snippet: str,
    methodology: str,
    source_url: str,
    reporting_basis: str,
) -> dict[str, Any]:
    snippet_clean = " ".join(snippet.split())
    return {
        "source_id": "pepper",
        "source_type": SourceType.NON_BANK_LISTED,
        "segment": segment,
        "product": None,
        "parameter": parameter,
        "data_definition_class": ddc,
        "value": value,
        "as_of_date": as_of,
        "reporting_basis": reporting_basis,
        "methodology_note": f"{methodology}. Snippet: {snippet_clean!r}.",
        "sample_size_n": None,
        "period_start": None,
        "period_end": as_of,
        "source_url": source_url,
        "page_or_table_ref": "Pepper results presentation slide text",
    }


__all__ = ["PepperMoneyAdapter"]
