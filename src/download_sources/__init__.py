"""Standalone download scripts for external benchmark source files.

Automated (direct XLSX/PDF downloads):
- ``apra_downloader.ApraAdiDownloader`` — APRA quarterly ADI statistics
- ``pillar3_downloader.Pillar3Downloader`` — Big 4 Pillar 3 disclosures
- ``rba_downloader.RbaDownloader`` — RBA FSR PDF + Securitisation snapshot

Best-effort (per-source graceful — OK / MANUAL / FAIL):
- ``non_bank_downloader.NonBankDisclosureDownloader`` — non-bank
  lender investor-relations pages (judo, liberty, pepper, resimac,
  moneyme, plenti, qualitas, metrics_credit, latrobe, bluestone,
  latitude, humm, zip). Many IR sites are bot-protected; the
  downloader writes a per-lender ``_MANUAL.md`` note when it can't
  fetch.
- ``external_indices_downloader.ExternalIndicesDownloader`` — S&P SPIN
  manual-staging helper. The adapter parses staged PDFs.
"""

from src.download_sources.apra_downloader import ApraAdiDownloader
from src.download_sources.external_indices_downloader import (
    ExternalIndicesDownloader,
)
from src.download_sources.governance_publications_downloader import (
    GovernancePublicationsDownloader,
)
from src.download_sources.non_bank_downloader import (
    NonBankDisclosureDownloader,
)
from src.download_sources.pillar3_downloader import Pillar3Downloader
from src.download_sources.rba_downloader import RbaDownloader

__all__ = [
    "ApraAdiDownloader",
    "ExternalIndicesDownloader",
    "GovernancePublicationsDownloader",
    "NonBankDisclosureDownloader",
    "Pillar3Downloader",
    "RbaDownloader",
]
