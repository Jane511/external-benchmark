"""RBA Securitisation Dataset aggregates adapter.

The full RBA Securitisation Dataset is restricted, but RBA publishes
aggregate statistics derived from it in RBA Bulletin articles and the
Financial Stability Review. Free, very valuable.

Source URL:        https://www.rba.gov.au/securitisations/
Reporting cadence: irregular (RBA Bulletin / FSR articles)
Coverage:          residential mortgage performance aggregates

NO adjustment of any kind — values are reported as-published.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ingestion.adapters.non_bank_base import NonBankDisclosureAdapter
from src.models import SourceType

logger = logging.getLogger(__name__)


class RbaSecuritisationAggregatesAdapter(NonBankDisclosureAdapter):
    SOURCE_ID = "rba_securitisation"
    SOURCE_TYPE = SourceType.RBA_AGGREGATE
    REPORTING_BASIS = "RBA Bulletin aggregates from Securitisation Dataset"
    SOURCE_URL = "https://www.rba.gov.au/securitisations/"

    def normalise(self, file_path: Path) -> pd.DataFrame:
        """Parse RBA-published aggregates.

        TODO: implement once a target RBA Bulletin / FSR article is
        identified and a sample data table extracted.
        """
        if not file_path.exists():
            logger.warning(
                "RBA securitisation file %s not found; emitting empty frame",
                file_path,
            )
            return self.empty_frame()
        return self.empty_frame()
