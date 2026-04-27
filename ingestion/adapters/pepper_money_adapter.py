"""Pepper Money annual / half-yearly disclosure adapter.

Pepper Money (ASX: PPM) publishes annual and half-yearly results with
delinquency and loss data on residential and commercial loans.

Source URL:        https://www.peppermoney.com.au/investors/
Reporting cadence: half-yearly + annual
Coverage:          residential mortgages, commercial loans, consumer loans

NO adjustment of any kind — definitions are mapped to canonical segment
names but values are reported as-published.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ingestion.adapters.non_bank_base import NonBankDisclosureAdapter
from src.models import SourceType

logger = logging.getLogger(__name__)


class PepperMoneyAdapter(NonBankDisclosureAdapter):
    SOURCE_ID = "pepper"
    SOURCE_TYPE = SourceType.NON_BANK_LISTED
    REPORTING_BASIS = "Pepper Money half-yearly results — credit performance"
    SOURCE_URL = "https://www.peppermoney.com.au/investors/"

    def normalise(self, file_path: Path) -> pd.DataFrame:
        """Parse a Pepper Money results announcement.

        Pepper publishes 30+, 60+, 90+ days arrears, plus loss rates by
        loan vintage. The adapter emits one observation per (segment,
        parameter, period_end) combination.

        TODO: implement parsing once a sample Pepper investor pack is
        retrieved.
        """
        if not file_path.exists():
            logger.warning("Pepper file %s not found; emitting empty frame", file_path)
            return self.empty_frame()
        return self.empty_frame()
