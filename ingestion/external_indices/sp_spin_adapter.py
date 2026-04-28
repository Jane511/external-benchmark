"""S&P RMBS Performance Index (SPIN) adapter.

Parses S&P SPIN monthly release PDFs and emits RawObservation rows for
residential RMBS 30+ DPD arrears (prime and non-conforming).

Source URL:        https://www.spglobal.com/ratings/en/regulatory/topic/spin
Reporting cadence: monthly (engine consumes quarterly snapshots)
Coverage:          Prime RMBS 30+ DPD arrears, non-conforming RMBS 30+ DPD arrears

NO adjustment of any kind — values are reported as-published.
"""
from __future__ import annotations

from calendar import monthrange
from datetime import date
import logging
import re
from pathlib import Path

import pandas as pd
import pdfplumber

from ingestion.adapters.non_bank_base import NonBankDisclosureAdapter
from src.models import DataDefinitionClass, SourceType

logger = logging.getLogger(__name__)


_MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}

_REPORTING_PERIOD_PATTERNS = (
    re.compile(
        r"(?:as\s+of|at)\s+"
        r"(January|February|March|April|May|June|July|August|September|"
        r"October|November|December)\s+\d{1,2},\s+(\d{4})",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:in\s+|for\s+|[-\u2013\u2014]\s*)"
        r"(January|February|March|April|May|June|July|August|September|"
        r"October|November|December)\s+(\d{4})",
        re.IGNORECASE,
    ),
)

_PRIME_TABLE_ROW_RE = re.compile(
    r"^\s*Prime\s+SPIN\s+(?P<values>(?:\d+(?:\.\d+)?\s+)+\d+(?:\.\d+)?)\s*$",
    re.IGNORECASE | re.MULTILINE,
)
_NON_CONFORMING_TABLE_ROW_RE = re.compile(
    r"^\s*Non[- ]Conforming\s+SPIN\s+"
    r"(?P<values>(?:\d+(?:\.\d+)?\s+)+\d+(?:\.\d+)?)\s*$",
    re.IGNORECASE | re.MULTILINE,
)

_PRIME_SPIN_PATTERNS = (
    re.compile(
        r"(?:weighted[- ]average\s+)?prime\s+spin\s+"
        r"(?:was|of|stood at|rose to|fell to|increased to|decreased to)"
        r"\s*(\d+(?:\.\d+)?)\s*%",
        re.IGNORECASE,
    ),
    re.compile(
        r"prime\s+(?:spin|index)\s+"
        r"(?:was|of|stood at|rose to|fell to|increased to|decreased to)"
        r"\s*(\d+(?:\.\d+)?)\s*%",
        re.IGNORECASE,
    ),
)
_NON_CONFORMING_SPIN_PATTERNS = (
    re.compile(
        r"(?:weighted[- ]average\s+)?non[- ]?conforming\s+spin\s+"
        r"(?:was|of|stood at|rose to|fell to|increased to|decreased to)"
        r"\s*(\d+(?:\.\d+)?)\s*%",
        re.IGNORECASE,
    ),
    re.compile(
        r"non[- ]?conforming\s+(?:spin|index)\s+"
        r"(?:was|of|stood at|rose to|fell to|increased to|decreased to)"
        r"\s*(\d+(?:\.\d+)?)\s*%",
        re.IGNORECASE,
    ),
)


class SpSpinAdapter(NonBankDisclosureAdapter):
    SOURCE_ID = "sp_spin"
    SOURCE_TYPE = SourceType.RATING_AGENCY_INDEX
    REPORTING_BASIS = "S&P RMBS Performance Index (SPIN), monthly"
    SOURCE_URL = "https://www.spglobal.com/ratings/en/regulatory/topic/spin"

    def normalise(self, file_path: Path) -> pd.DataFrame:
        """Parse a S&P SPIN release PDF and emit headline arrears rows.

        Each PDF should produce up to two observations: prime SPIN and
        non-conforming SPIN. Parsing failures degrade to an empty frame.
        """
        if not file_path.exists():
            logger.warning("SPIN file %s not found; emitting empty frame", file_path)
            return self.empty_frame()

        try:
            text = self._extract_text(file_path)
        except Exception as exc:
            logger.warning("Failed to extract SPIN text from %s: %s", file_path, exc)
            return self.empty_frame()

        records = self._extract_observations_from_text(text, file_path=file_path)
        if not records:
            logger.warning("SPIN adapter found no headline values in %s", file_path)
            return self.empty_frame()
        return pd.DataFrame.from_records(records, columns=self.canonical_columns)

    def _extract_text(self, file_path: Path) -> str:
        text_parts: list[str] = []
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                text_parts.append(page.extract_text() or "")
        return "\n".join(text_parts)

    def _extract_observations_from_text(
        self, text: str, *, file_path: Path
    ) -> list[dict]:
        as_of = self._extract_reporting_period(text)
        if as_of is None:
            logger.warning("Could not extract SPIN reporting period from %s", file_path)
            return []

        records: list[dict] = []
        prime_value = self._extract_prime_spin(text)
        if prime_value is not None:
            records.append(self._build_observation(
                source_id="sp_spin_prime",
                segment="residential_mortgage",
                value=prime_value,
                as_of_date=as_of,
                methodology_note=(
                    "S&P SPIN weighted-average 30+ DPD arrears rate, "
                    "prime RMBS universe"
                ),
                file_path=file_path,
            ))
        else:
            logger.warning("No prime SPIN value found in %s", file_path)

        non_conforming_value = self._extract_non_conforming_spin(text)
        if non_conforming_value is not None:
            records.append(self._build_observation(
                source_id="sp_spin_non_conforming",
                segment="residential_mortgage_specialist",
                value=non_conforming_value,
                as_of_date=as_of,
                methodology_note=(
                    "S&P SPIN weighted-average 30+ DPD arrears rate, "
                    "non-conforming RMBS universe"
                ),
                file_path=file_path,
            ))
        else:
            logger.warning("No non-conforming SPIN value found in %s", file_path)

        return records

    def _extract_reporting_period(self, text: str) -> date | None:
        for pattern in _REPORTING_PERIOD_PATTERNS:
            match = pattern.search(text)
            if match is None:
                continue
            month = _MONTHS.get(match.group(1).lower())
            if month is None:
                continue
            year = int(match.group(2))
            return date(year, month, monthrange(year, month)[1])
        return None

    def _extract_prime_spin(self, text: str) -> float | None:
        table_value = _extract_last_table_value(_PRIME_TABLE_ROW_RE, text)
        if table_value is not None:
            return table_value / 100.0
        return _extract_first_percentage(_PRIME_SPIN_PATTERNS, text)

    def _extract_non_conforming_spin(self, text: str) -> float | None:
        table_value = _extract_last_table_value(_NON_CONFORMING_TABLE_ROW_RE, text)
        if table_value is not None:
            return table_value / 100.0
        return _extract_first_percentage(_NON_CONFORMING_SPIN_PATTERNS, text)

    def _build_observation(
        self,
        *,
        source_id: str,
        segment: str,
        value: float,
        as_of_date: date,
        methodology_note: str,
        file_path: Path,
    ) -> dict:
        return {
            "source_id": source_id,
            "source_type": self.SOURCE_TYPE.value,
            "segment": segment,
            "product": None,
            "parameter": "arrears",
            "data_definition_class": (
                DataDefinitionClass.ARREARS_30_PLUS_DAYS.value
            ),
            "value": value,
            "as_of_date": as_of_date.isoformat(),
            "reporting_basis": self.REPORTING_BASIS,
            "methodology_note": methodology_note,
            "sample_size_n": None,
            "period_start": None,
            "period_end": as_of_date.isoformat(),
            "source_url": self.SOURCE_URL,
            "page_or_table_ref": (
                f"Headline SPIN row in staged PDF: {file_path.name}"
            ),
        }


def _extract_last_table_value(pattern: re.Pattern, text: str) -> float | None:
    match = pattern.search(text)
    if match is None:
        return None
    values = [float(v) for v in re.findall(r"\d+(?:\.\d+)?", match.group("values"))]
    if not values:
        return None
    return values[-1]


def _extract_first_percentage(patterns: tuple[re.Pattern, ...], text: str) -> float | None:
    for pattern in patterns:
        match = pattern.search(text)
        if match:
            return float(match.group(1)) / 100.0
    return None


__all__ = ["SpSpinAdapter"]
