"""Big 4 Pillar 3 scrapers (APS 330 credit risk disclosures).

Phase 2 ships CBA (Excel companion parser) as the real implementation;
NAB, WBC, ANZ are stubbed pending pdfplumber integration.
"""
from ingestion.pillar3.anz import ANZScraper
from ingestion.pillar3.base import Pillar3BaseScraper
from ingestion.pillar3.cba import CBAScraper
from ingestion.pillar3.nab import NABScraper
from ingestion.pillar3.wbc import WBCScraper

__all__ = [
    "Pillar3BaseScraper",
    "CBAScraper",
    "NABScraper",
    "WBCScraper",
    "ANZScraper",
]
