"""MoneyMe disclosure adapter.

MoneyMe (ASX: MME) publishes annual reports with credit performance
metrics on its consumer and SME / commercial loan books.

Source URL:        https://investors.moneyme.com.au/
Reporting cadence: half-yearly + annual + monthly trading updates
Coverage:          consumer unsecured, autopay (auto), SME / commercial

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


class MoneyMeDisclosureAdapter(NonBankDisclosureAdapter):
    SOURCE_ID = "moneyme"
    SOURCE_TYPE = SourceType.NON_BANK_LISTED
    REPORTING_BASIS = "MoneyMe half-yearly results — net credit losses table"
    SOURCE_URL = "https://investors.moneyme.com.au/"

    def normalise(self, file_path: Path) -> pd.DataFrame:
        """Parse a MoneyMe investor pack.

        MoneyMe reports 'net credit losses %' on the loan book — the
        adapter emits this as parameter='lgd' or parameter='pd' depending
        on what the disclosure actually labels (read the methodology
        footnote on the slide). The methodology_note column should
        capture the exact source-side phrasing.

        TODO: implement parsing once a sample MoneyMe investor pack is
        retrieved.
        """
        if not file_path.exists():
            logger.warning("MoneyMe file %s not found; emitting empty frame", file_path)
            return self.empty_frame()
        return self.empty_frame()
