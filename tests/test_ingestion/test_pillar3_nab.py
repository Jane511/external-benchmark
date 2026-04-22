"""Tests for ingestion/pillar3/nab.py — fixture-driven; no real PDFs."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from ingestion.pillar3.base import PdfPillar3Scraper
from ingestion.pillar3.nab import NABScraper
from ingestion.transform import scraped_to_entry
from src.models import DataType, SourceType


FIXTURE = Path(__file__).parent.parent / "fixtures" / "nab_pillar3_tables.json"


@pytest.fixture()
def nab_fixture_tables() -> list[dict]:
    """Return NAB's canonical `tables` list from the JSON fixture."""
    data = json.loads(FIXTURE.read_text())
    return data["tables"]


@pytest.fixture()
def nab_scraper(sources_config: dict) -> NABScraper:
    cfg = sources_config["sources"]["nab_pillar3"]
    return NABScraper(config=cfg, reporting_date=date(2025, 6, 30))


# ---------------------------------------------------------------------------
# Fixture parse -> ScrapedDataPoints
# ---------------------------------------------------------------------------

def test_nab_parses_fixture_tables_into_ten_points(
    nab_scraper: NABScraper, nab_fixture_tables,
) -> None:
    """3 portfolios × 2 metrics + 4 slotting grades = 10 points."""
    points = nab_scraper._build_points_from_tables(nab_fixture_tables)
    assert len(points) == 10
    assert all(p.publisher == "NAB" for p in points)

    by_name = {p.source_name: p for p in points}
    assert by_name["NAB_RESIDENTIAL_MORTGAGE_PD"].raw_value == 0.0090
    assert by_name["NAB_RESIDENTIAL_MORTGAGE_LGD"].raw_value == 0.24
    assert by_name["NAB_COMMERCIAL_PROPERTY_PD"].raw_value == 0.0220


def test_nab_source_ids_match_flagship_format(
    nab_scraper: NABScraper, nab_fixture_tables,
) -> None:
    cfg_asset_map = {
        "Residential mortgage": "residential_mortgage",
        "Commercial property": "commercial_property_investment",
        "Corporate (incl. SME corporate)": "corporate_sme",
        "Specialised Lending": "development",
    }
    points = nab_scraper.validate(nab_scraper._build_points_from_tables(nab_fixture_tables))
    ids = {
        scraped_to_entry(p, override_asset_class_map=cfg_asset_map).source_id
        for p in points
    }
    # Mirrors the CBA format from Phase 2
    assert "NAB_RESIDENTIAL_MORTGAGE_PD_FY2025" in ids
    assert "NAB_COMMERCIAL_PROPERTY_INVESTMENT_PD_FY2025" in ids
    assert "NAB_CORPORATE_SME_LGD_FY2025" in ids
    assert "NAB_DEVELOPMENT_STRONG_PD_FY2025" in ids
    assert "NAB_DEVELOPMENT_WEAK_PD_FY2025" in ids


# ---------------------------------------------------------------------------
# Asset-class normalisation (shared base helper, NAB-flavoured labels)
# ---------------------------------------------------------------------------

def test_nab_normalises_bank_specific_labels() -> None:
    fn = PdfPillar3Scraper._normalise_asset_class_label
    assert fn("Residential mortgage") == "residential_mortgage"
    assert fn("Retail residential mortgage") == "residential_mortgage"
    assert fn("Housing") == "residential_mortgage"
    assert fn("Commercial property") == "commercial_property_investment"
    assert fn("Commercial real estate") == "commercial_property_investment"
    assert fn("CRE") == "commercial_property_investment"
    assert fn("Corporate (incl. SME)") == "corporate_sme"
    assert fn("SME") == "corporate_sme"
    assert fn("Specialised Lending") == "development"


# ---------------------------------------------------------------------------
# Validation (inherited from Pillar3BaseScraper)
# ---------------------------------------------------------------------------

def test_nab_validate_rejects_out_of_range_pd(
    nab_scraper: NABScraper,
) -> None:
    implausible = [
        {
            "name": "IRB Credit Risk",
            "rows": [
                {
                    "asset_class_raw": "Residential mortgage",
                    "pd": 0.50,       # 50% PD — far above APS 330 ceiling
                    "lgd": 0.25,
                    "exposure_ead_mn": 400000,
                },
            ],
        },
    ]
    points = nab_scraper._build_points_from_tables(implausible)
    assert any(p.raw_value == 0.50 for p in points)
    valid = nab_scraper.validate(points)
    assert not any(p.raw_value == 0.50 for p in valid)
    # LGD row still survives
    assert any(p.raw_value == 0.25 for p in valid)


# ---------------------------------------------------------------------------
# scrape() with missing PDF -> FileNotFoundError (not NotImplementedError)
# ---------------------------------------------------------------------------

def test_nab_scrape_missing_pdf_raises_file_not_found(
    sources_config: dict, tmp_path: Path,
) -> None:
    cfg = sources_config["sources"]["nab_pillar3"]
    scraper = NABScraper(
        source_path=tmp_path / "missing.pdf", config=cfg,
        reporting_date=date(2025, 6, 30),
    )
    with pytest.raises(FileNotFoundError, match="NAB Pillar 3 PDF not found"):
        scraper.scrape()


def test_nab_resolve_source_path_derives_h1_h2(
    sources_config: dict, tmp_path: Path,
) -> None:
    """H1 for Jan–Jun reporting, H2 for Jul–Dec — filename reflects it."""
    from unittest.mock import patch

    captured: dict = {}

    def fake_urlretrieve(url, dest):
        Path(dest).write_bytes(b"fake pdf")
        captured["dest"] = Path(dest)
        return dest, None

    cfg = sources_config["sources"]["nab_pillar3"]
    with patch("ingestion.downloader.urlretrieve", side_effect=fake_urlretrieve):
        scraper = NABScraper(
            config=cfg, reporting_date=date(2025, 12, 31),
            cache_base=tmp_path / "raw",
        )
        scraper._resolve_source_path()

    assert "H2" in captured["dest"].name
    assert "2025" in captured["dest"].name
