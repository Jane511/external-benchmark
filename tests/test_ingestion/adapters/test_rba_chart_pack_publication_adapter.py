"""Tests for RBA Chart Pack publication capture."""

from __future__ import annotations

from ingestion.adapters.rba_publications_adapter import RbaChartPackScraper


_CHART_PACK_LANDING = """
<html><body>
  <a href="/chart-pack/pdf/chart-pack.pdf">Chart Pack 5.65MB</a>
</body></html>
"""


def test_rba_chart_pack_discovers_pdf_from_landing_fixture() -> None:
    scraper = RbaChartPackScraper()
    pdf_url, period = scraper._discover_latest_url(_CHART_PACK_LANDING)
    assert pdf_url == "https://www.rba.gov.au/chart-pack/pdf/chart-pack.pdf"
    assert period.startswith("Q")


def test_rba_chart_pack_filename_uses_current_quarter_shape() -> None:
    scraper = RbaChartPackScraper()
    assert scraper._filename("Q2 2026") == "RBA_ChartPack_Q2_2026.pdf"
