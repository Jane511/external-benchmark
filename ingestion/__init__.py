"""Ingestion layer — automates collection of raw benchmark data.

**Direction of dependency is strict:** `ingestion` imports from `src`.
`src` never imports from `ingestion`. Scrapers can break (URLs change, PDF
layouts shift) without corrupting the core registry or calibration pipeline.
"""
from ingestion.base import BaseScraper, ScrapedDataPoint

__all__ = ["BaseScraper", "ScrapedDataPoint"]
