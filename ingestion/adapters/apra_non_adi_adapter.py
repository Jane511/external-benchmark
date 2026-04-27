"""APRA non-ADI lender register adapter.

APRA publishes a register of authorised non-ADI lenders. Some entries
publish quarterly statistics; most don't. This adapter:

  - Reads the register file (CSV / XLSX as published)
  - For each registered non-ADI that publishes performance data, emits
    one or more RawObservation rows
  - For non-ADIs that don't publish (the majority), emits nothing —
    they appear in the register but contribute no observation

Source URL:   https://www.apra.gov.au/register-of-non-adi-lenders
Source type:  SourceType.APRA_NON_ADI

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


class ApraNonAdiAdapter(NonBankDisclosureAdapter):
    SOURCE_ID = "apra_non_adi_register"
    SOURCE_TYPE = SourceType.APRA_NON_ADI
    REPORTING_BASIS = "APRA register of non-ADI lenders (registered entities only)"
    SOURCE_URL = "https://www.apra.gov.au/register-of-non-adi-lenders"

    def normalise(self, file_path: Path) -> pd.DataFrame:
        """Parse the APRA non-ADI register.

        The register itself is a list of entities, not performance data.
        For entities that DO publish performance data, this adapter
        delegates to the entity-specific disclosure adapter (when one
        exists) and emits its rows; otherwise the entity is skipped
        with a debug log.

        TODO: implement once the register CSV format is confirmed.
        """
        if not file_path.exists():
            logger.warning(
                "APRA non-ADI register file %s not found; emitting empty frame",
                file_path,
            )
            return self.empty_frame()
        return self.empty_frame()
