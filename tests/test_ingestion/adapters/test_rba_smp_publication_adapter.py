"""Tests for RBA Statement on Monetary Policy publication capture."""

from __future__ import annotations

from ingestion.adapters.rba_publications_adapter import RbaSmpScraper


_SMP_LANDING = """
<html><body>
  <a href="/publications/smp/2025/nov/">November</a>
  <a href="/publications/smp/2026/feb/">February</a>
  <a href="/publications/smp/2025/aug/">August</a>
</body></html>
"""


def test_rba_smp_latest_issue_from_landing_fixture() -> None:
    scraper = RbaSmpScraper()
    issue_url, period = scraper._latest_issue_url(_SMP_LANDING)
    assert issue_url == "https://www.rba.gov.au/publications/smp/2026/feb/"
    assert period == "February 2026"


def test_rba_smp_filename_uses_quarter_and_year() -> None:
    scraper = RbaSmpScraper()
    assert scraper._filename("February 2026") == "RBA_SMP_Q1_2026.pdf"
