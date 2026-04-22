"""ANZ Pillar 3 PDF scraper.

ANZ headers tables as "IRB Approach — Credit Risk Exposures"; the "IRB" prefix
alone is shared with NAB but the full string differs. Pattern is permissive
enough to work either way.
"""
from __future__ import annotations

from ingestion.adapters.anz_pillar3_pdf_adapter import AnzPillar3PdfAdapter
from ingestion.pillar3.base import PdfPillar3Scraper


class ANZScraper(PdfPillar3Scraper):
    """Australia and New Zealand Banking Group Pillar 3 PDF scraper."""

    PUBLISHER = "ANZ"
    SOURCE_URL_KEY = "anz_pillar3"
    PDF_ADAPTER_CLS = AnzPillar3PdfAdapter

    IRB_HEADER_PATTERN = "IRB Approach"
    SLOTTING_HEADER_PATTERN = "Specialised Lending"

    IRB_COLUMN_MAP = {
        "asset_class": 0,
        "exposure_ead": 1,
        "pd": 2,
        "lgd": 3,
    }
    SLOTTING_COLUMN_MAP = {"grade": 0, "pd": 1}
