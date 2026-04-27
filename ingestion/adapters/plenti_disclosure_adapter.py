"""Plenti Group disclosure adapter.

Plenti (ASX: PLT) publishes monthly investor updates and half-yearly
results with arrears and credit loss data on personal loans, automotive
loans, and renewable energy loans.

Source URL:        https://investors.plenti.com.au/
Reporting cadence: monthly investor update + half-yearly results
Coverage:          personal loans, auto loans, SME automotive, renewables

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


class PlentiDisclosureAdapter(NonBankDisclosureAdapter):
    SOURCE_ID = "plenti"
    SOURCE_TYPE = SourceType.NON_BANK_LISTED
    REPORTING_BASIS = "Plenti monthly investor update — arrears + net loss table"
    SOURCE_URL = "https://investors.plenti.com.au/"

    def normalise(self, file_path: Path) -> pd.DataFrame:
        """Parse a Plenti investor update.

        Plenti's monthly update gives 90+ days arrears and trailing-12m
        net credit losses by product. Both are useful — emit one row
        per (product, parameter, period_end). methodology_note should
        explicitly distinguish 'arrears 90+' from 'net credit loss' so
        consumers don't conflate them.

        TODO: implement parsing once a sample Plenti investor update
        is retrieved.
        """
        if not file_path.exists():
            logger.warning("Plenti file %s not found; emitting empty frame", file_path)
            return self.empty_frame()
        return self.empty_frame()
