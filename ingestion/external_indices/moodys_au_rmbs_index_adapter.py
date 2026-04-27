"""Moody's Australia RMBS Index adapter.

Moody's publishes a monthly Australia RMBS performance summary covering
delinquencies and prepayment.

Source URL:        https://www.moodys.com/  (filter for Australia RMBS)
Reporting cadence: monthly
Coverage:          Australia RMBS delinquencies (30+, 60+, 90+ days)

NO adjustment of any kind — values are reported as-published.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ingestion.adapters.non_bank_base import NonBankDisclosureAdapter
from src.models import SourceType

logger = logging.getLogger(__name__)


class MoodysAuRmbsIndexAdapter(NonBankDisclosureAdapter):
    SOURCE_ID = "moodys_au_rmbs"
    SOURCE_TYPE = SourceType.RATING_AGENCY_INDEX
    REPORTING_BASIS = "Moody's Australia RMBS Index, monthly"
    SOURCE_URL = "https://www.moodys.com/"

    def normalise(self, file_path: Path) -> pd.DataFrame:
        """Parse the Moody's Australia RMBS Index release.

        TODO: implement parsing once a sample release is retrieved.
        """
        if not file_path.exists():
            logger.warning(
                "Moody's RMBS index file %s not found; emitting empty frame",
                file_path,
            )
            return self.empty_frame()
        return self.empty_frame()
