"""Tests for ingestion/pillar3/wbc.py — fixture-driven."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from ingestion.pillar3.wbc import WBCScraper
from ingestion.transform import scraped_to_entry


FIXTURE = Path(__file__).parent.parent / "fixtures" / "wbc_pillar3_tables.json"


@pytest.fixture()
def wbc_fixture_tables() -> list[dict]:
    return json.loads(FIXTURE.read_text())["tables"]


@pytest.fixture()
def wbc_scraper(sources_config: dict) -> WBCScraper:
    return WBCScraper(
        config=sources_config["sources"]["wbc_pillar3"],
        reporting_date=date(2025, 6, 30),
    )


def test_wbc_parses_fixture_into_ten_points(
    wbc_scraper: WBCScraper, wbc_fixture_tables,
) -> None:
    points = wbc_scraper._build_points_from_tables(wbc_fixture_tables)
    assert len(points) == 10
    assert all(p.publisher == "WBC" for p in points)
    # WBC uses "Housing" as its residential label (distinct from NAB's "Residential mortgage").
    assert any("HOUSING" in p.source_name for p in points)


def test_wbc_source_ids_normalise_housing_to_residential_mortgage(
    wbc_scraper: WBCScraper, wbc_fixture_tables,
) -> None:
    asset_map = {
        "Housing": "residential_mortgage",
        "Commercial real estate": "commercial_property_investment",
        "Corporate SME": "corporate_sme",
        "Specialised Lending": "development",
    }
    points = wbc_scraper._build_points_from_tables(wbc_fixture_tables)
    ids = {
        scraped_to_entry(p, override_asset_class_map=asset_map).source_id
        for p in points
    }
    # After transform, "Housing" collapses to the same canonical asset_class as NAB.
    assert "WBC_RESIDENTIAL_MORTGAGE_PD_FY2025" in ids
    assert "WBC_COMMERCIAL_PROPERTY_INVESTMENT_LGD_FY2025" in ids


def test_wbc_validation_keeps_all_fixture_values(
    wbc_scraper: WBCScraper, wbc_fixture_tables,
) -> None:
    """WBC fixture values sit inside APS 330 ranges — validation should pass everything."""
    raw = wbc_scraper._build_points_from_tables(wbc_fixture_tables)
    assert wbc_scraper.validate(raw) == raw
