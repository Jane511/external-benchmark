"""Tests for ingestion/apra_adi.py caching integration."""
from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import patch

import openpyxl
import pytest

from ingestion.apra_adi import ApraAdiScraper


def _write_fixture_at(path: Path) -> None:
    """Helper: write a minimal APRA XLSX at `path`."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Asset Quality"
    ws.append(["Period", "Category", "90DPD_Rate", "NPL_Rate"])
    ws.append([date(2025, 9, 30), "Residential", 0.012, 0.008])
    wb.save(path)


def test_apra_scraper_uses_source_path_when_provided(
    apra_xlsx_fixture: Path, sources_config: dict,
) -> None:
    """Backwards compat: passing source_path bypasses the downloader entirely."""
    cfg = sources_config["sources"]["apra_adi_performance"]
    scraper = ApraAdiScraper(source_path=apra_xlsx_fixture, config=cfg,
                             retrieval_date=date(2025, 12, 1))
    with patch("ingestion.downloader.urlretrieve") as mock_urlretrieve:
        points = scraper.scrape()
    assert len(points) > 0
    assert mock_urlretrieve.call_count == 0   # no download attempted


def test_apra_scraper_auto_downloads_when_no_source_path(
    tmp_path: Path, sources_config: dict,
) -> None:
    """No source_path -> FileDownloader fetches from SOURCE_URLS URL."""
    cfg = sources_config["sources"]["apra_adi_performance"]

    cache_base = tmp_path / "raw"

    def fake_urlretrieve(url, dest):
        """Write the fixture XLSX to the expected cache location."""
        _write_fixture_at(Path(dest))
        return dest, None

    with patch("ingestion.downloader.urlretrieve", side_effect=fake_urlretrieve) as mock:
        scraper = ApraAdiScraper(
            config=cfg,
            retrieval_date=date(2025, 9, 30),   # -> Q3 2025 filename pattern
            cache_base=cache_base,
        )
        points = scraper.scrape()

    assert len(points) > 0
    assert mock.call_count == 1
    # Cached file lives under cache_base/apra/
    cached_files = list((cache_base / "apra").iterdir())
    assert len(cached_files) == 1
    assert cached_files[0].name.startswith("ADI_Performance_Q3_2025")
