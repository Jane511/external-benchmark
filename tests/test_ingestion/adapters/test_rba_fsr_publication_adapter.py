"""Tests for RBA Financial Stability Review publication capture."""

from __future__ import annotations

from ingestion.adapters.rba_publications_adapter import RbaFsrScraper


_FSR_LANDING = """
<html><body>
  <a href="/publications/fsr/2025/oct/">October</a>
  <a href="/publications/fsr/2026/mar/">March</a>
  <a href="/publications/fsr/2025/apr/">April</a>
</body></html>
"""


def test_rba_fsr_latest_issue_from_landing_fixture() -> None:
    scraper = RbaFsrScraper()
    issue_url, period = scraper._latest_issue_url(_FSR_LANDING)
    assert issue_url == "https://www.rba.gov.au/publications/fsr/2026/mar/"
    assert period == "March 2026"


def test_rba_fsr_download_pdf_anchor_from_issue_fixture() -> None:
    html = '<a href="/publications/fsr/2026/pdf/financial-stability-review-2026-03.pdf">Download PDF</a>'
    found = RbaFsrScraper._find_download_pdf(
        html, "https://www.rba.gov.au/publications/fsr/2026/mar/",
    )
    assert found is not None
    assert found[0] == (
        "https://www.rba.gov.au/publications/fsr/2026/pdf/"
        "financial-stability-review-2026-03.pdf"
    )
