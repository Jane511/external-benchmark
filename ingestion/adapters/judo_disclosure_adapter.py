"""Judo Bank Pillar 3 disclosure adapter.

Judo became an ADI in 2019 and publishes Pillar 3 disclosures quarterly
in the Big-4 format. This adapter maps Judo's CR6-equivalent table to
RawObservation. NO adjustment of any kind — definitions are mapped to
canonical names (see ingestion/segment_mapping.yaml) but values are
reported as-published.

Source URL:        https://www.judo.bank/investor-centre/
Reporting cadence: quarterly Pillar 3 + half-yearly results
Coverage:          SME corporate, commercial real estate

Implementation note: Judo's Pillar 3 PDF closely resembles the Big-4
layout. When a sample PDF is available, lift the parsing pattern from
ingestion/adapters/cba_pillar3_pdf_adapter.py rather than reinventing.
For now the adapter declares the canonical contract and returns an
empty frame when no parseable file is supplied — empty is a valid
outcome per AbstractAdapter.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ingestion.adapters.non_bank_base import NonBankDisclosureAdapter
from src.models import SourceType

logger = logging.getLogger(__name__)


class JudoDisclosureAdapter(NonBankDisclosureAdapter):
    SOURCE_ID = "judo"
    SOURCE_TYPE = SourceType.NON_BANK_LISTED  # ADI but treated as non-Big-4
    REPORTING_BASIS = "Judo Pillar 3 quarterly disclosure (CR6-equivalent)"
    SOURCE_URL = "https://www.judo.bank/investor-centre/"

    def normalise(self, file_path: Path) -> pd.DataFrame:
        """Parse a Judo Pillar 3 publication.

        Expected segments: SME corporate, commercial real estate,
        construction. Each row carries an Average PD (%) and Average
        LGD (%) to emit as separate parameter='pd' / parameter='lgd'
        observations.

        TODO: implement PDF parsing once a sample Judo Pillar 3 file is
        retrieved. The pattern from cba_pillar3_pdf_adapter.py applies
        directly — same CR6 layout.
        """
        if not file_path.exists():
            logger.warning("Judo file %s not found; emitting empty frame", file_path)
            return self.empty_frame()
        # Placeholder: real implementation parses the Pillar 3 PDF / XLSX.
        return self.empty_frame()
