"""Tests for ingestion/pillar3/anz.py — fixture-driven."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from ingestion.pillar3.anz import ANZScraper
from ingestion.transform import scraped_to_entry


FIXTURE = Path(__file__).parent.parent / "fixtures" / "anz_pillar3_tables.json"


@pytest.fixture()
def anz_fixture_tables() -> list[dict]:
    return json.loads(FIXTURE.read_text())["tables"]


@pytest.fixture()
def anz_scraper(sources_config: dict) -> ANZScraper:
    return ANZScraper(
        config=sources_config["sources"]["anz_pillar3"],
        reporting_date=date(2025, 6, 30),
    )


def test_anz_parses_fixture_into_ten_points(
    anz_scraper: ANZScraper, anz_fixture_tables,
) -> None:
    points = anz_scraper._build_points_from_tables(anz_fixture_tables)
    assert len(points) == 10
    assert all(p.publisher == "ANZ" for p in points)


def test_anz_scrape_handles_retail_residential_mortgage_label(
    anz_scraper: ANZScraper, anz_fixture_tables,
) -> None:
    """ANZ uses 'Retail residential mortgage' — should normalise the same."""
    asset_map = {
        "Retail residential mortgage": "residential_mortgage",
        "Commercial property": "commercial_property_investment",
        "Corporate (incl. SME)": "corporate_sme",
        "Specialised Lending": "development",
    }
    points = anz_scraper._build_points_from_tables(anz_fixture_tables)
    ids = {
        scraped_to_entry(p, override_asset_class_map=asset_map).source_id
        for p in points
    }
    assert "ANZ_RESIDENTIAL_MORTGAGE_PD_FY2025" in ids
    assert "ANZ_DEVELOPMENT_GOOD_PD_FY2025" in ids


def test_anz_fixture_values_inside_aps330_ranges(
    anz_scraper: ANZScraper, anz_fixture_tables,
) -> None:
    raw = anz_scraper._build_points_from_tables(anz_fixture_tables)
    assert len(anz_scraper.validate(raw)) == len(raw)
