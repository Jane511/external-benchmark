"""Tests for ingestion/apra_adi.py — XLSX parsing + validation."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from ingestion.apra_adi import ApraAdiScraper


def test_scrape_extracts_one_point_per_row_and_metric(
    apra_xlsx_fixture: Path, sources_config: dict,
) -> None:
    cfg = sources_config["sources"]["apra_adi_performance"]
    scraper = ApraAdiScraper(
        source_path=apra_xlsx_fixture, config=cfg,
        retrieval_date=date(2025, 12, 1),
    )
    points = scraper.scrape()
    # 3 categories x 2 metrics = 6 points
    assert len(points) == 6

    by_name = {p.source_name: p for p in points}
    assert "APRA_RESIDENTIAL_90DPD" in by_name
    assert by_name["APRA_RESIDENTIAL_90DPD"].raw_value == 0.012
    assert by_name["APRA_RESIDENTIAL_NPL"].raw_value == 0.008
    assert by_name["APRA_COMMERCIAL_90DPD"].raw_value == 0.018


def test_scrape_reads_period_as_date(
    apra_xlsx_fixture: Path, sources_config: dict,
) -> None:
    cfg = sources_config["sources"]["apra_adi_performance"]
    scraper = ApraAdiScraper(
        source_path=apra_xlsx_fixture, config=cfg,
        retrieval_date=date(2025, 12, 1),
    )
    points = scraper.scrape()
    for p in points:
        assert p.value_date == date(2025, 9, 30)
        assert p.period_years == 1
        assert p.publisher == "APRA"
        assert p.geography == "AU"


def test_validate_drops_out_of_range_values(
    apra_xlsx_bad_values: Path, sources_config: dict,
) -> None:
    cfg = sources_config["sources"]["apra_adi_performance"]
    scraper = ApraAdiScraper(source_path=apra_xlsx_bad_values, config=cfg)
    raw_points = scraper.scrape()
    # Raw scrape captures the 95% row
    assert any(p.raw_value == 0.95 for p in raw_points)
    # Validation drops it (90DPD range is [0, 0.20])
    valid = scraper.validate(raw_points)
    assert not any(p.raw_value == 0.95 for p in valid)


def test_scrape_missing_file_raises(sources_config: dict, tmp_path: Path) -> None:
    cfg = sources_config["sources"]["apra_adi_performance"]
    scraper = ApraAdiScraper(
        source_path=tmp_path / "does_not_exist.xlsx", config=cfg,
    )
    with pytest.raises(FileNotFoundError):
        scraper.scrape()


def test_scraper_metadata_properties(sources_config: dict, tmp_path: Path) -> None:
    cfg = sources_config["sources"]["apra_adi_performance"]
    scraper = ApraAdiScraper(source_path=tmp_path / "x.xlsx", config=cfg)
    assert scraper.expected_frequency_days == 120
    assert "APRA" in scraper.source_name
