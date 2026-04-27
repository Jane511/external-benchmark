"""Fitch APAC RMBS Dinkum Index adapter.

Fitch publishes the quarterly APAC RMBS Dinkum Index covering arrears,
defaults, and losses on Australian RMBS pools.

Source URL:        https://www.fitchratings.com/sites/au-rmbs-dinkum
Reporting cadence: quarterly
Coverage:          Australia RMBS arrears, defaults, losses

NO adjustment of any kind — values are reported as-published.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ingestion.adapters.non_bank_base import NonBankDisclosureAdapter
from src.models import SourceType

logger = logging.getLogger(__name__)


class FitchDinkumAdapter(NonBankDisclosureAdapter):
    SOURCE_ID = "fitch_dinkum"
    SOURCE_TYPE = SourceType.RATING_AGENCY_INDEX
    REPORTING_BASIS = "Fitch APAC RMBS Dinkum Index, quarterly"
    SOURCE_URL = "https://www.fitchratings.com/sites/au-rmbs-dinkum"

    def normalise(self, file_path: Path) -> pd.DataFrame:
        """Parse the Fitch Dinkum release.

        TODO: implement parsing once a sample Dinkum release is retrieved.
        """
        if not file_path.exists():
            logger.warning(
                "Dinkum file %s not found; emitting empty frame", file_path
            )
            return self.empty_frame()
        return self.empty_frame()
