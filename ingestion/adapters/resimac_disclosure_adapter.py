"""Resimac Group disclosure adapter.

Resimac (ASX: RMC) publishes annual and half-yearly results with
detailed delinquency tables broken out by Prime vs Specialist
mortgages, plus commercial loans.

Source URL:        https://www.resimac.com.au/about-resimac/investor-relations/
Reporting cadence: half-yearly + annual
Coverage:          residential mortgages (Prime / Specialist), commercial

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


class ResimacDisclosureAdapter(NonBankDisclosureAdapter):
    SOURCE_ID = "resimac"
    SOURCE_TYPE = SourceType.NON_BANK_LISTED
    REPORTING_BASIS = "Resimac half-yearly results — Prime / Specialist split"
    SOURCE_URL = "https://www.resimac.com.au/about-resimac/investor-relations/"

    def normalise(self, file_path: Path) -> pd.DataFrame:
        """Parse a Resimac investor pack.

        Resimac uses 'Prime' and 'Specialist' product splits — both map
        to the canonical 'residential_mortgage' segment, but the
        adapter records the product field so consumers can subset.

        TODO: implement parsing once a sample Resimac investor pack is
        retrieved.
        """
        if not file_path.exists():
            logger.warning("Resimac file %s not found; emitting empty frame", file_path)
            return self.empty_frame()
        return self.empty_frame()
