"""Standalone download scripts for external benchmark source files.

Automated (direct XLSX/PDF downloads):
- ``apra_downloader.ApraAdiDownloader`` — APRA quarterly ADI statistics
- ``pillar3_downloader.Pillar3Downloader`` — Big 4 Pillar 3 disclosures
- ``abs_business_counts_downloader.AbsBusinessCountsDownloader`` — ABS cat. 8165
- ``asic_insolvency_downloader.AsicInsolvencyDownloader`` — ASIC Series 1+2

Manual (by design, see individual module docstrings for rationale):
- ``icc_downloader.IccTradeDownloader`` — ICC Trade Register
"""

from scripts.download_sources.abs_business_counts_downloader import (
    AbsBusinessCountsDownloader,
)
from scripts.download_sources.apra_downloader import ApraAdiDownloader
from scripts.download_sources.asic_insolvency_downloader import (
    AsicInsolvencyDownloader,
)
from scripts.download_sources.icc_downloader import IccTradeDownloader
from scripts.download_sources.pillar3_downloader import Pillar3Downloader

__all__ = [
    "AbsBusinessCountsDownloader",
    "ApraAdiDownloader",
    "AsicInsolvencyDownloader",
    "IccTradeDownloader",
    "Pillar3Downloader",
]
