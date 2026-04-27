"""Liberty Financial Group annual disclosure adapter.

Liberty (ASX: LFG) publishes annual reports with loan loss data by
segment (residential mortgages, commercial property loans, SME loans,
auto/personal). Annual cadence; half-yearly results carry partial-year
arrears tables.

Source URL:        https://www.libertyfinancial.com.au/about/investor-information
Reporting cadence: annual + half-yearly
Coverage:          residential mortgages, commercial property, SME, auto

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


class LibertyAnnualAdapter(NonBankDisclosureAdapter):
    SOURCE_ID = "liberty"
    SOURCE_TYPE = SourceType.NON_BANK_LISTED
    REPORTING_BASIS = "Liberty Financial Group annual report — credit risk section"
    SOURCE_URL = "https://www.libertyfinancial.com.au/about/investor-information"

    def normalise(self, file_path: Path) -> pd.DataFrame:
        """Parse a Liberty annual / half-yearly publication.

        Liberty reports per-portfolio loss rates and arrears bands
        (30+, 60+, 90+ days). The adapter maps these to:
          - parameter='pd' from the 90+ days arrears rate as a default
            proxy where no PD is published directly
          - parameter='lgd' from realised loss rate where disclosed

        TODO: implement PDF parsing once a sample Liberty annual file
        is retrieved. The methodology_note column must explicitly say
        "90+ arrears proxy" when PD is derived from arrears rather than
        a published PD figure, so consumers can see the provenance.
        """
        if not file_path.exists():
            logger.warning("Liberty file %s not found; emitting empty frame", file_path)
            return self.empty_frame()
        return self.empty_frame()
