"""S&P RMBS Performance Index (SPIN) adapter.

S&P SPIN publishes monthly aggregate Australian prime and non-conforming
arrears statistics. Public, free.

Source URL:        https://www.spglobal.com/ratings/en/regulatory/topic/spin
Reporting cadence: monthly
Coverage:          Prime RMBS arrears, Non-Conforming RMBS arrears

NO adjustment of any kind — values are reported as-published.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ingestion.adapters.non_bank_base import NonBankDisclosureAdapter
from src.models import SourceType

logger = logging.getLogger(__name__)


class SpSpinAdapter(NonBankDisclosureAdapter):
    SOURCE_ID = "sp_spin"
    SOURCE_TYPE = SourceType.RATING_AGENCY_INDEX
    REPORTING_BASIS = "S&P RMBS Performance Index (SPIN), monthly"
    SOURCE_URL = "https://www.spglobal.com/ratings/en/regulatory/topic/spin"

    def normalise(self, file_path: Path) -> pd.DataFrame:
        """Parse the S&P SPIN release.

        SPIN is an aggregate index across rated RMBS deals. The published
        figure is total-arrears %, which the adapter records as a
        parameter='pd' proxy for the residential_mortgage segment with
        methodology_note='SPIN total arrears (proxy for default rate)'.

        TODO: implement parsing of the SPIN release format.
        """
        if not file_path.exists():
            logger.warning("SPIN file %s not found; emitting empty frame", file_path)
            return self.empty_frame()
        return self.empty_frame()
