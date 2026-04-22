"""Westpac (WBC) Pillar 3 PDF scraper.

See `ingestion/pillar3/nab.py` for the design rationale and extension points.
WBC's APS 330 table headers lean on "Credit Risk — Exposures by portfolio type
and PD band"; adjust the patterns here if a future edition shifts.
"""
from __future__ import annotations

from ingestion.adapters.wbc_pillar3_pdf_adapter import WbcPillar3PdfAdapter
from ingestion.pillar3.base import PdfPillar3Scraper


class WBCScraper(PdfPillar3Scraper):
    """Westpac Banking Corporation Pillar 3 PDF scraper."""

    PUBLISHER = "WBC"
    SOURCE_URL_KEY = "wbc_pillar3"
    PDF_ADAPTER_CLS = WbcPillar3PdfAdapter

    IRB_HEADER_PATTERN = "Credit Risk"
    SLOTTING_HEADER_PATTERN = "Specialised Lending"

    IRB_COLUMN_MAP = {
        "asset_class": 0,
        "exposure_ead": 1,
        "pd": 2,
        "lgd": 3,
    }
    SLOTTING_COLUMN_MAP = {"grade": 0, "pd": 1}
