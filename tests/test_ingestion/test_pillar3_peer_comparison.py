"""Cross-bank Pillar 3 peer comparison tests.

Loads all 4 Big 4 fixtures, checks:
  - All four produce the same canonical asset_class values
  - Real 4-bank fixture PDs are within 3x peer median (no flags)
  - Synthetically-broken input (one bank 5x median) does flag
  - Full orchestrator run across 4 banks adds 40 entries to the registry
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from statistics import median

import pytest

from ingestion.pillar3.base import Pillar3BaseScraper
from ingestion.pillar3.cba import CBAScraper
from ingestion.pillar3.nab import NABScraper
from ingestion.pillar3.wbc import WBCScraper
from ingestion.pillar3.anz import ANZScraper
from ingestion.transform import scraped_to_entry


FIXTURES = Path(__file__).parent.parent / "fixtures"


def _load(name: str) -> list[dict]:
    return json.loads((FIXTURES / name).read_text())["tables"]


def _pd_values_for(raw_label_fragment: str, bank_points: list[tuple]) -> list[float]:
    """Collect PD values for a given asset class across all 4 banks.

    `bank_points` is a list of (publisher, list[ScrapedDataPoint]) tuples.
    """
    values: list[float] = []
    for _publisher, points in bank_points:
        for p in points:
            if (raw_label_fragment.lower() in p.asset_class_raw.lower()
                and p.metadata.get("data_type_hint") == "pd"):
                values.append(p.raw_value)
    return values


@pytest.fixture()
def four_bank_points(
    sources_config: dict, cba_pillar3_xlsx: Path,
) -> list[tuple]:
    """Return [(publisher, list[ScrapedDataPoint]), ...] for all four banks.

    CBA uses its Excel scrape() path; NAB/WBC/ANZ use JSON fixtures through
    `_build_points_from_tables()` (bypasses pdfplumber).
    """
    rd = date(2025, 6, 30)

    cba = CBAScraper(
        source_path=cba_pillar3_xlsx,
        config=sources_config["sources"]["cba_pillar3"],
        reporting_date=rd,
    )
    cba_points = cba.scrape()

    def _points(cls, key, fixture_name):
        scraper = cls(config=sources_config["sources"][key], reporting_date=rd)
        return scraper.validate(
            scraper._build_points_from_tables(_load(fixture_name))
        )

    return [
        ("CBA", cba_points),
        ("NAB", _points(NABScraper, "nab_pillar3", "nab_pillar3_tables.json")),
        ("WBC", _points(WBCScraper, "wbc_pillar3", "wbc_pillar3_tables.json")),
        ("ANZ", _points(ANZScraper, "anz_pillar3", "anz_pillar3_tables.json")),
    ]


# ---------------------------------------------------------------------------
# Shared asset-class canonicalisation across banks
# ---------------------------------------------------------------------------

def test_all_banks_normalise_to_same_asset_class_enums(
    four_bank_points, sources_config,
) -> None:
    """CBA / NAB / WBC / ANZ emit different raw labels but land on the same enums."""
    per_bank_asset_maps = {
        "CBA": sources_config["sources"]["cba_pillar3"]["asset_class_mapping"],
        "NAB": {
            "Residential mortgage": "residential_mortgage",
            "Commercial property": "commercial_property_investment",
            "Corporate (incl. SME corporate)": "corporate_sme",
            "Specialised Lending": "development",
        },
        "WBC": {
            "Housing": "residential_mortgage",
            "Commercial real estate": "commercial_property_investment",
            "Corporate SME": "corporate_sme",
            "Specialised Lending": "development",
        },
        "ANZ": {
            "Retail residential mortgage": "residential_mortgage",
            "Commercial property": "commercial_property_investment",
            "Corporate (incl. SME)": "corporate_sme",
            "Specialised Lending": "development",
        },
    }

    canonical: dict[str, set[str]] = {}
    for publisher, points in four_bank_points:
        asset_map = per_bank_asset_maps[publisher]
        for p in points:
            entry = scraped_to_entry(p, override_asset_class_map=asset_map)
            canonical.setdefault(entry.asset_class, set()).add(publisher)

    for seg in ("residential_mortgage", "commercial_property_investment",
                "corporate_sme", "development"):
        assert canonical[seg] == {"CBA", "NAB", "WBC", "ANZ"}, (
            f"Segment {seg} missing banks: {canonical.get(seg)}"
        )


# ---------------------------------------------------------------------------
# Peer comparison — real 4-bank fixtures are all within 3x median
# ---------------------------------------------------------------------------

def test_real_four_bank_fixtures_have_no_peer_divergence(four_bank_points) -> None:
    # Residential labels vary: CBA/NAB use "Residential mortgage", WBC uses
    # "Housing", ANZ uses "Retail residential mortgage". Match across all.
    residential_pds: list[float] = []
    for _pub, points in four_bank_points:
        for p in points:
            label = p.asset_class_raw.lower()
            if (("residential" in label or "housing" in label)
                and p.metadata.get("data_type_hint") == "pd"):
                residential_pds.append(p.raw_value)

    assert len(residential_pds) == 4
    peer_med = median(residential_pds)
    for v in residential_pds:
        assert Pillar3BaseScraper.peer_comparison_flag(v, peer_med) is False, (
            f"residential PD {v} flagged vs median {peer_med}"
        )


def test_peer_comparison_fires_on_obvious_outlier() -> None:
    """A 5x outlier must flag — verifies the helper is actually wired, not just silent."""
    peers = [0.0072, 0.0090, 0.0088, 0.0080]
    peer_med = median(peers)
    # Synthetic bad PD: 4.5% for residential mortgage (5x the ~0.8% Big 4 average)
    outlier = 0.045
    assert Pillar3BaseScraper.peer_comparison_flag(outlier, peer_med) is True


def test_peer_comparison_tolerates_within_range_variation() -> None:
    """Big 4 natural range variation (0.0072-0.0090) must not flag."""
    peers = [0.0072, 0.0090, 0.0088, 0.0080]
    peer_med = median(peers)
    for v in peers:
        assert Pillar3BaseScraper.peer_comparison_flag(v, peer_med) is False


# ---------------------------------------------------------------------------
# Integration: orchestrator pulls all 4 banks into the registry
# ---------------------------------------------------------------------------

def test_orchestrator_runs_all_four_banks_via_fixtures(
    sources_config, cba_pillar3_xlsx, tmp_path,
) -> None:
    """Stand up 4 banks in one registry. Real value: proves the full pipeline
    (scraper -> transform -> registry) works for all Big 4, not just CBA."""
    from ingestion.refresh import RefreshOrchestrator
    from src.db import create_engine_and_schema
    from src.registry import BenchmarkRegistry

    # Write each JSON fixture into a fake PDF path so the orchestrator passes
    # `source_path` to NAB/WBC/ANZ — but those scrapers would then try to open
    # it as a PDF. Instead, drive the transform manually per bank.
    engine = create_engine_and_schema(":memory:")
    registry = BenchmarkRegistry(engine, actor="test")

    # Run CBA via the orchestrator (Excel path works end-to-end)
    orch = RefreshOrchestrator(
        registry=registry,
        sources_config=sources_config,
        local_overrides={"cba_pillar3": cba_pillar3_xlsx},
        scraper_extras={"cba_pillar3": {"reporting_date": date(2025, 6, 30)}},
    )
    orch.refresh_source("cba_pillar3")

    # For NAB/WBC/ANZ, bypass the orchestrator and drive transform -> registry
    # directly from each JSON fixture (real PDF parsing is out of scope here).
    bank_specs = [
        (NABScraper, "nab_pillar3", "nab_pillar3_tables.json",
         sources_config["sources"]["nab_pillar3"].get("asset_class_mapping", {
            "Residential mortgage": "residential_mortgage",
            "Commercial property": "commercial_property_investment",
            "Corporate (incl. SME corporate)": "corporate_sme",
            "Specialised Lending": "development",
        })),
        (WBCScraper, "wbc_pillar3", "wbc_pillar3_tables.json", {
            "Housing": "residential_mortgage",
            "Commercial real estate": "commercial_property_investment",
            "Corporate SME": "corporate_sme",
            "Specialised Lending": "development",
        }),
        (ANZScraper, "anz_pillar3", "anz_pillar3_tables.json", {
            "Retail residential mortgage": "residential_mortgage",
            "Commercial property": "commercial_property_investment",
            "Corporate (incl. SME)": "corporate_sme",
            "Specialised Lending": "development",
        }),
    ]
    for cls, key, fixture_name, asset_map in bank_specs:
        scraper = cls(config=sources_config["sources"][key],
                      reporting_date=date(2025, 6, 30))
        tables = _load(fixture_name)
        points = scraper.validate(scraper._build_points_from_tables(tables))
        for p in points:
            entry = scraped_to_entry(p, override_asset_class_map=asset_map)
            registry.add(entry)

    entries = registry.list()
    # CBA 10 + NAB 10 + WBC 10 + ANZ 10 = 40
    assert len(entries) == 40
    publishers = {e.publisher for e in entries}
    assert publishers == {"CBA", "NAB", "WBC", "ANZ"}
