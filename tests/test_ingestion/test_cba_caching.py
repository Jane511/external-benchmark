"""Tests for ingestion/pillar3/cba.py caching integration (H1/H2 derivation)."""
from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import patch

import openpyxl
import pytest

from ingestion.pillar3.cba import CBAScraper


def _write_cba_fixture_at(path: Path) -> None:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "IRB Credit Risk"
    ws.append(["Portfolio", "Exposure_EAD_Mn", "PD", "LGD"])
    ws.append(["Residential Mortgage", 500000, 0.0072, 0.22])
    sl = wb.create_sheet("Specialised Lending")
    sl.append(["Grade", "PD"])
    sl.append(["Strong", 0.004])
    wb.save(path)


def test_cba_scraper_uses_source_path_when_provided(
    cba_pillar3_xlsx: Path, sources_config: dict,
) -> None:
    cfg = sources_config["sources"]["cba_pillar3"]
    scraper = CBAScraper(
        source_path=cba_pillar3_xlsx, config=cfg,
        reporting_date=date(2025, 6, 30),
    )
    with patch("ingestion.downloader.urlretrieve") as mock_urlretrieve:
        points = scraper.scrape()
    assert len(points) > 0
    assert mock_urlretrieve.call_count == 0


@pytest.mark.parametrize("reporting_date, expected_half_in_filename", [
    (date(2025, 6, 30),  "H1"),    # Jan–Jun -> H1
    (date(2025, 12, 31), "H2"),    # Jul–Dec -> H2
    (date(2025, 3, 15),  "H1"),
    (date(2025, 9, 15),  "H2"),
])
def test_cba_derives_half_from_reporting_date(
    tmp_path: Path, sources_config: dict,
    reporting_date: date, expected_half_in_filename: str,
) -> None:
    cfg = sources_config["sources"]["cba_pillar3"]
    cache_base = tmp_path / "raw"

    captured: dict = {}

    def fake_urlretrieve(url, dest):
        captured["dest"] = Path(dest)
        _write_cba_fixture_at(Path(dest))
        return dest, None

    with patch("ingestion.downloader.urlretrieve", side_effect=fake_urlretrieve):
        scraper = CBAScraper(
            config=cfg,
            reporting_date=reporting_date,
            cache_base=cache_base,
        )
        scraper.scrape()

    filename = captured["dest"].name
    assert expected_half_in_filename in filename
    assert f"{reporting_date.year}" in filename
