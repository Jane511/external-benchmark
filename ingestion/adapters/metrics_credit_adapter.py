"""Metrics Credit Partners disclosure adapter.

Metrics Credit Partners runs CRE-focused credit funds. They publish
aggregate returns and portfolio commentary, no segment-level loss
tables. Recorded as qualitative_commentary so downstream consumers
know the source exists.

Source URL:        https://metrics.com.au/
Reporting cadence: monthly fund reports, half-yearly results
Coverage:          CRE credit, infrastructure debt
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ingestion.adapters.non_bank_base import NonBankDisclosureAdapter
from src.models import SourceType

logger = logging.getLogger(__name__)


class MetricsCreditAdapter(NonBankDisclosureAdapter):
    SOURCE_ID = "metrics_credit"
    SOURCE_TYPE = SourceType.NON_BANK_LISTED
    REPORTING_BASIS = "Monthly fund reports; half-yearly results"
    SOURCE_URL = "https://metrics.com.au/"

    def normalise(self, file_path: Path) -> pd.DataFrame:
        """Parse a Metrics Credit Partners disclosure.

        Metrics publishes commentary and aggregate fund returns, not
        segment-level loss tables. Until sample disclosures are available,
        emit an empty frame; once available, parse the narrative into
        ``qualitative_commentary`` observations with ``value=0.0``.
        """
        if not file_path.exists():
            logger.warning(
                "Metrics Credit file %s not found; emitting empty frame", file_path
            )
            return self.empty_frame()
        return self.empty_frame()
