"""Qualitas (ASX:QAL) disclosure adapter.

Qualitas is an ASX-listed alternative real estate investment manager
specialising in CRE private credit. They publish:
  - Aggregate fund performance (returns, AUM growth)
  - Portfolio commentary in half-yearly results presentations
  - QRI (Qualitas Real Estate Income Fund) monthly reports

What they DON'T publish:
  - Segment-level default rates
  - PD or LGD figures by asset class
  - Cohort-level loss tables

This adapter records their published commentary as qualitative
observations so downstream consumers (PD, LGD reality-check) know
the source exists and what's available. NO adjustment, NO numeric
inference from commentary text.

Source URL:        https://www.qualitas.com.au/
Reporting cadence: half-yearly results, monthly QRI updates
Coverage:          CRE credit (commercial property, development)
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ingestion.adapters.non_bank_base import NonBankDisclosureAdapter
from src.models import SourceType

logger = logging.getLogger(__name__)


class QualitasDisclosureAdapter(NonBankDisclosureAdapter):
    SOURCE_ID = "qualitas"
    SOURCE_TYPE = SourceType.NON_BANK_LISTED
    REPORTING_BASIS = "Half-yearly results presentation; QRI monthly reports"
    SOURCE_URL = "https://www.qualitas.com.au/"

    def normalise(self, file_path: Path) -> pd.DataFrame:
        """Parse a Qualitas disclosure.

        Qualitas publishes commentary, not numbers. When sample disclosures
        are available, parse the narrative and extract any sector
        commentary (e.g. "office sector showing stress in Q3 2025") as
        ``qualitative_commentary`` observations with ``value=0.0``.

        Until a sample is available: return an empty frame (a valid
        outcome per the AbstractAdapter contract).
        """
        if not file_path.exists():
            logger.warning(
                "Qualitas file %s not found; emitting empty frame", file_path
            )
            return self.empty_frame()
        return self.empty_frame()
