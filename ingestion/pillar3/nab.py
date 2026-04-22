"""NAB Pillar 3 PDF scraper.

NAB publishes a Pillar 3 PDF (no Excel companion) following APS 330. Table
locators below are starting points; the first live run against a real NAB PDF
may need tuning — override any of the class attributes on a subclass, or
bump the patterns/column maps here.

Extension points:
    IRB_HEADER_PATTERN        substring searched on each page's detected tables
    SLOTTING_HEADER_PATTERN   same, for specialised lending / slotting
    IRB_COLUMN_MAP            {"asset_class": i, "exposure_ead": i, "pd": i, "lgd": i}
    SLOTTING_COLUMN_MAP       {"grade": i, "pd": i}

Tests inject canonical fixture tables (JSON) via `_build_points_from_tables()`
to bypass pdfplumber entirely — pdfplumber is only exercised when a real PDF
path is supplied to `scrape()`.
"""
from __future__ import annotations

from ingestion.adapters.nab_pillar3_pdf_adapter import NabPillar3PdfAdapter
from ingestion.pillar3.base import PdfPillar3Scraper


class NABScraper(PdfPillar3Scraper):
    """NAB (National Australia Bank) Pillar 3 PDF scraper."""

    PUBLISHER = "NAB"
    SOURCE_URL_KEY = "nab_pillar3"
    PDF_ADAPTER_CLS = NabPillar3PdfAdapter

    # NAB's APS 330 "CR6" table has headers like
    # "IRB — Credit risk exposures by portfolio and PD range".
    IRB_HEADER_PATTERN = "IRB"
    SLOTTING_HEADER_PATTERN = "Specialised Lending"

    # Column positions tuned to NAB's 2024 H2 layout.
    # Adjust when a real PDF introduces new columns (e.g. RWA, EL).
    IRB_COLUMN_MAP = {
        "asset_class": 0,
        "exposure_ead": 1,
        "pd": 2,
        "lgd": 3,
    }
    SLOTTING_COLUMN_MAP = {"grade": 0, "pd": 1}
