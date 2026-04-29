"""Bank Pillar 3 scrapers (APS 330 credit risk disclosures).

Includes the Big 4 plus Macquarie Bank Limited.
"""
from ingestion.pillar3.anz import ANZScraper
from ingestion.pillar3.base import Pillar3BaseScraper
from ingestion.pillar3.cba import CBAScraper
from ingestion.pillar3.mqg import MQGScraper
from ingestion.pillar3.nab import NABScraper
from ingestion.pillar3.wbc import WBCScraper

__all__ = [
    "Pillar3BaseScraper",
    "CBAScraper",
    "MQGScraper",
    "NABScraper",
    "WBCScraper",
    "ANZScraper",
]
