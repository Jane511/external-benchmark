"""RBA Financial Stability Review aggregates adapter.

RBA FSR publishes semi-annual aggregate statistics on household and
business loan performance.

Source URL:        https://www.rba.gov.au/publications/fsr/
Reporting cadence: semi-annual
Coverage:          household + business loan arrears, NPL ratios

NO adjustment of any kind — values are reported as-published.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ingestion.adapters.non_bank_base import NonBankDisclosureAdapter
from src.models import SourceType

logger = logging.getLogger(__name__)


class RbaFsrAggregatesAdapter(NonBankDisclosureAdapter):
    SOURCE_ID = "rba_fsr"
    SOURCE_TYPE = SourceType.RBA_AGGREGATE
    REPORTING_BASIS = "RBA Financial Stability Review aggregates, semi-annual"
    SOURCE_URL = "https://www.rba.gov.au/publications/fsr/"

    def normalise(self, file_path: Path) -> pd.DataFrame:
        """Parse the RBA FSR data tables.

        TODO: implement once a target FSR data release is identified.
        """
        if not file_path.exists():
            logger.warning(
                "RBA FSR file %s not found; emitting empty frame", file_path
            )
            return self.empty_frame()
        return self.empty_frame()
