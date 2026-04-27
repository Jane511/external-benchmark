"""Wisr Limited disclosure adapter.

Wisr (ASX: WZR) publishes quarterly cashflow / business updates and
annual reports with loan book performance data on personal loans
(prime + near-prime) and secured vehicle loans.

Source URL:        https://wisr.com.au/about/investors/
Reporting cadence: quarterly cashflow + annual report
Coverage:          personal loans (prime, near-prime), secured vehicle

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


class WisrDisclosureAdapter(NonBankDisclosureAdapter):
    SOURCE_ID = "wisr"
    SOURCE_TYPE = SourceType.NON_BANK_LISTED
    REPORTING_BASIS = "Wisr quarterly business update — loan book + 90+ arrears"
    SOURCE_URL = "https://wisr.com.au/about/investors/"

    def normalise(self, file_path: Path) -> pd.DataFrame:
        """Parse a Wisr quarterly update.

        Wisr separates 'Wisr Prime' from 'Wisr Freedom' (near-prime).
        Both map to canonical 'consumer_unsecured' but the product
        column carries the granular split. methodology_note should
        capture the loan-book size at period_end.

        TODO: implement parsing once a sample Wisr quarterly update
        is retrieved.
        """
        if not file_path.exists():
            logger.warning("Wisr file %s not found; emitting empty frame", file_path)
            return self.empty_frame()
        return self.empty_frame()
