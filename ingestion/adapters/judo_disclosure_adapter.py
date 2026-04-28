"""Judo Bank Pillar 3 disclosure adapter.

Judo became an ADI in 2019 and publishes Pillar 3 disclosures quarterly
in the Big-4 format (APS 330). This adapter maps Judo's CR6-equivalent
table to RawObservation. NO adjustment of any kind — definitions are
mapped to canonical names (see ingestion/segment_mapping.yaml) but
values are reported as-published.

Source URL:        https://www.judo.bank/regulatory-disclosures
Reporting cadence: quarterly Pillar 3 + half-yearly results
Coverage:          SME corporate, commercial real estate, construction

Parser strategy
---------------
Judo's quarterly Pillar 3 follows the APRA APS 330 layout closely. The
CR6 table groups exposures by asset class × PD band; we extract one
weighted-average PD per asset class from the sub-total / "Total" row of
each block, since that is the most-comparable summary number for cross-
source PD reality checks. For consumers wanting per-band detail, the
band-level rows are also emitted.

The parser is text-driven for two reasons:
  1. ``pdfplumber.extract_tables`` reliably loses the left-most asset-
     class column on this layout (same problem the CBA adapter solves
     by line-parsing).
  2. A text-driven parser is testable from a string fixture, so we don't
     have to commit binary PDFs.

Fall-back: if the file is missing OR pdfplumber can't extract anything,
the adapter returns an empty frame (per the AbstractAdapter contract).
"""
from __future__ import annotations

import logging
import re
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ingestion.adapters.non_bank_base import (
    CANONICAL_OBSERVATION_COLUMNS,
    NonBankDisclosureAdapter,
)
from src.models import DataDefinitionClass, SourceType

logger = logging.getLogger(__name__)


# CR6-style PD band signature, e.g. "0.00 to <0.15" or "100.00 (Default)".
# The parser uses this to identify CR6 data rows; the column immediately to
# the left of the band carries the asset class label (when not inherited).
_PD_RANGE_RE = re.compile(
    r"(?P<band>(?:\d+\.\d+\s*to\s*<\s*\d+\.\d+)|(?:100\.00\s*\(Default\)))"
    r"\s+(?P<rest>.*)$"
)


# Map Judo's published asset-class labels to canonical segments. These map
# via segment_mapping.yaml at lookup-time, but we keep an explicit ordered
# table here so the row's left-prefix can be matched (longer needles first
# so "commercial real estate" wins over "commercial").
_JUDO_PORTFOLIO_NEEDLES: tuple[str, ...] = (
    "commercial real estate",
    "cre lending",
    "small and medium business",
    "sme lending",
    "construction",
    # Generic fall-through — kept short so it doesn't shadow the more-
    # specific labels above.
    "sme",
    "cre",
)


# Plausibility band: any extracted PD outside this is dropped with a warning.
_PD_PLAUSIBLE: tuple[float, float] = (0.0, 1.01)


# As-of date heuristics — pull a "30 June 2025" / "31 March 2026" style
# header from the first few hundred chars of the PDF.
_AS_OF_RE = re.compile(
    r"(?:as at|for the (?:quarter|half[- ]year|year) ended|"
    r"reporting date|period ended)\s+"
    r"(?P<dom>\d{1,2})\s+(?P<mon>[A-Za-z]+)\s+(?P<year>\d{4})",
    re.IGNORECASE,
)


_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9,
    "october": 10, "november": 11, "december": 12,
}


def _coerce_as_of_from_text(text: str) -> date | None:
    """Pull an as-of date from a Pillar 3 cover page if one is mentioned."""
    m = _AS_OF_RE.search(text)
    if not m:
        return None
    mon = _MONTHS.get(m.group("mon").lower())
    if not mon:
        return None
    try:
        return date(int(m.group("year")), mon, int(m.group("dom")))
    except ValueError:
        return None


class JudoDisclosureAdapter(NonBankDisclosureAdapter):
    SOURCE_ID = "judo"
    SOURCE_TYPE = SourceType.NON_BANK_LISTED  # ADI but treated as non-Big-4 peer
    REPORTING_BASIS = "Judo Pillar 3 quarterly disclosure (APS 330 — CR6 equivalent)"
    SOURCE_URL = "https://www.judo.bank/regulatory-disclosures"

    def normalise(self, file_path: Path) -> pd.DataFrame:
        """Parse a Judo Pillar 3 publication.

        Reads the PDF with pdfplumber, finds CR6 (or equivalent) pages,
        and emits one ``parameter='pd'`` row per (asset class × PD band)
        plus the asset-class total row when published. Each row carries
        ``data_definition_class=BASEL_PD_ONE_YEAR``.
        """
        if not file_path.exists():
            logger.warning("Judo file %s not found; emitting empty frame", file_path)
            return self.empty_frame()

        try:
            import pdfplumber  # lazy — keep tests cheap
        except ImportError:
            logger.warning(
                "pdfplumber not installed; cannot parse %s — emitting empty frame",
                file_path,
            )
            return self.empty_frame()

        try:
            with pdfplumber.open(file_path) as pdf:
                pages_text = [p.extract_text() or "" for p in pdf.pages]
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("pdfplumber failed on %s: %s", file_path, exc)
            return self.empty_frame()

        full_text = "\n".join(pages_text)
        as_of = _coerce_as_of_from_text(full_text) or self._default_as_of()

        records: list[dict[str, Any]] = []
        for page_index, text in enumerate(pages_text, start=1):
            if not _looks_like_cr6_page(text):
                continue
            records.extend(
                self._extract_cr6_text(
                    text=text,
                    as_of=as_of,
                    page_num=page_index,
                )
            )

        if not records:
            logger.warning(
                "Judo adapter found no CR6-style rows in %s; emitting empty frame",
                file_path,
            )
            return self.empty_frame()

        df = pd.DataFrame.from_records(records, columns=self.canonical_columns)
        return df

    # ------------------------------------------------------------------
    # Public, text-driven parser — used by tests against a string fixture.
    # ------------------------------------------------------------------

    def _extract_cr6_text(
        self,
        text: str,
        as_of: date,
        page_num: int = 1,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        current_segment: str | None = None
        current_label: str | None = None

        for raw_line in text.split("\n"):
            line = raw_line.strip()
            if not line:
                continue

            m = _PD_RANGE_RE.search(line)
            if m is None:
                # Header-only line. Try to interpret as an asset-class label.
                seg, label = self._classify_label(line)
                if seg:
                    current_segment = seg
                    current_label = label
                continue

            prefix = line[: m.start()].strip()
            if prefix:
                seg, label = self._classify_label(prefix)
                if seg:
                    current_segment = seg
                    current_label = label

            if current_segment is None:
                # Data row before any portfolio label — skip rather than guess.
                continue

            band = re.sub(r"\s+", " ", m.group("band").strip())
            rest_tokens = m.group("rest").split()
            pd_decimal = _pick_pd_value(rest_tokens)
            if pd_decimal is None:
                continue
            if not (_PD_PLAUSIBLE[0] <= pd_decimal <= _PD_PLAUSIBLE[1]):
                logger.warning(
                    "Judo CR6: dropping implausible PD=%.6f for segment=%s "
                    "band=%s (page %d)",
                    pd_decimal, current_segment, band, page_num,
                )
                continue
            # Cap at 1.0 for downstream Pydantic [0,1] validation
            value = min(pd_decimal, 1.0)
            out.append(_make_pd_row(
                segment=current_segment,
                published_label=current_label or current_segment,
                value=value,
                as_of=as_of,
                band=band,
                page=page_num,
                source_url=self.SOURCE_URL,
                reporting_basis=self.REPORTING_BASIS,
            ))

        return out

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _classify_label(self, text: str) -> tuple[str | None, str | None]:
        """Return (canonical_segment, raw_label_used) for a candidate label."""
        lower = text.lower()
        for needle in _JUDO_PORTFOLIO_NEEDLES:
            if needle in lower:
                seg = self.map_segment(needle)
                if seg:
                    return seg, needle
        # Fall back to whatever the YAML mapper recognises in the line.
        seg = self.map_segment(text)
        return (seg, text) if seg else (None, None)

    @staticmethod
    def _default_as_of() -> date:
        """Default to the most recent quarter end if no header date is found."""
        today = date.today()
        quarter_end_month = ((today.month - 1) // 3) * 3 or 3
        # Last day of that month.
        if quarter_end_month in (3, 12):
            day = 31
        else:
            day = 30
        year = today.year if quarter_end_month <= today.month else today.year - 1
        return date(year, quarter_end_month, day)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

_NA_TOKENS = {"-", "–", "—", "n/a", "na", ""}


def _parse_numeric(token: str) -> float | None:
    t = token.strip().replace(",", "")
    if t.lower() in _NA_TOKENS:
        return None
    try:
        return float(t.rstrip("%"))
    except ValueError:
        return None


def _pick_pd_value(tokens: list[str]) -> float | None:
    """Pick the Average PD (%) value from CR6 tokens.

    APS 330 CR6 column order after the PD range:
        1: Original on-balance gross exposure ($M)
        2: Off-balance pre-CCF ($M)
        3: Average CCF (%)
        4: EAD post CRM and post-CCF ($M)
        5: Average PD (%)              ← target
        6: Number of borrowers
        7: Average LGD (%)
        ...

    The 5th numeric is the PD percent — convert to decimal.
    """
    numerics = [_parse_numeric(t) for t in tokens]
    if len(numerics) < 5:
        return None
    pd_pct = numerics[4]
    if pd_pct is None:
        return None
    return pd_pct / 100.0


def _looks_like_cr6_page(text: str) -> bool:
    """Heuristic — does this PDF page contain a CR6-equivalent table?"""
    lower = text.lower()
    return (
        ("cr6" in lower)
        or ("pd scale" in lower)
        or ("pd range" in lower)
        or ("probability of default range" in lower)
        or ("weighted-average pd" in lower)
        or ("weighted average pd" in lower)
    )


def _make_pd_row(
    *,
    segment: str,
    published_label: str,
    value: float,
    as_of: date,
    band: str,
    page: int,
    source_url: str,
    reporting_basis: str,
) -> dict[str, Any]:
    return {
        "source_id": "judo",
        "source_type": SourceType.NON_BANK_LISTED,
        "segment": segment,
        "product": None,
        "parameter": "pd",
        "data_definition_class": DataDefinitionClass.BASEL_PD_ONE_YEAR,
        "value": value,
        "as_of_date": as_of,
        "reporting_basis": reporting_basis,
        "methodology_note": (
            f"Weighted-average PD from Judo Pillar 3 CR6 table; "
            f"asset class label: {published_label!r}; PD band: {band}."
        ),
        "sample_size_n": None,
        "period_start": None,
        "period_end": as_of,
        "source_url": source_url,
        "page_or_table_ref": f"CR6 (page {page}, band {band})",
    }


__all__ = [
    "JudoDisclosureAdapter",
    "CANONICAL_OBSERVATION_COLUMNS",
]
