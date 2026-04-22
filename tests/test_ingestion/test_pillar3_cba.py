"""Tests for ingestion/pillar3/cba.py + ingestion/pillar3/base.py.

Covers:
  - CBAScraper parses IRB credit risk and specialised lending sheets
  - Pillar3BaseScraper.validate_with_ranges picks the right APS 330 range
  - Stubs (NAB/WBC/ANZ) raise NotImplementedError pointing at CBA
  - Peer comparison helper flags >3x divergence
  - End-to-end transform produces expected source_ids
  - Orchestrator round-trip writes BenchmarkEntry rows
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from ingestion.pillar3.anz import ANZScraper
from ingestion.pillar3.base import (
    Pillar3BaseScraper,
    default_cba_period_code,
)
from ingestion.pillar3.cba import CBAScraper
from ingestion.pillar3.nab import NABScraper
from ingestion.pillar3.wbc import WBCScraper
from ingestion.refresh import RefreshOrchestrator
from ingestion.transform import scraped_to_entry
from src.db import create_engine_and_schema
from src.models import DataType, SourceType
from src.registry import BenchmarkRegistry


# ---------------------------------------------------------------------------
# CBAScraper — scrape()
# ---------------------------------------------------------------------------

def test_cba_scrape_emits_six_irb_points_plus_four_slotting(
    cba_pillar3_xlsx: Path, sources_config: dict,
) -> None:
    cfg = sources_config["sources"]["cba_pillar3"]
    scraper = CBAScraper(
        source_path=cba_pillar3_xlsx, config=cfg,
        reporting_date=date(2025, 6, 30),
    )
    points = scraper.scrape()
    # 3 portfolios x 2 metrics (PD, LGD) = 6 IRB + 4 slotting grades = 10
    assert len(points) == 10

    by_name = {p.source_name: p for p in points}
    assert by_name["CBA_RESIDENTIAL_MORTGAGE_PD"].raw_value == 0.0072
    assert by_name["CBA_RESIDENTIAL_MORTGAGE_LGD"].raw_value == 0.22
    assert by_name["CBA_CRE_INVESTMENT_PD"].raw_value == 0.0250
    assert by_name["CBA_CORPORATE_SME_PD"].raw_value == 0.0280

    # Slotting entries present with metric_column populated
    slotting = [p for p in points if "SLOTTING" in p.source_name]
    assert len(slotting) == 4
    assert all(p.metadata.get("metric_column") for p in slotting)


def test_cba_scrape_sets_publisher_and_period_code(
    cba_pillar3_xlsx: Path, sources_config: dict,
) -> None:
    cfg = sources_config["sources"]["cba_pillar3"]
    scraper = CBAScraper(
        source_path=cba_pillar3_xlsx, config=cfg,
        reporting_date=date(2025, 6, 30),
    )
    for p in scraper.scrape():
        assert p.publisher == "CBA"
        assert p.value_date == date(2025, 6, 30)
        assert p.metadata["period_code"] == "FY2025"


def test_cba_scrape_missing_file_raises(sources_config: dict, tmp_path: Path) -> None:
    cfg = sources_config["sources"]["cba_pillar3"]
    scraper = CBAScraper(source_path=tmp_path / "missing.xlsx", config=cfg)
    with pytest.raises(FileNotFoundError):
        scraper.scrape()


# ---------------------------------------------------------------------------
# Validation — APS 330 ranges from base
# ---------------------------------------------------------------------------

def test_cba_validate_accepts_realistic_ranges(
    cba_pillar3_xlsx: Path, sources_config: dict,
) -> None:
    cfg = sources_config["sources"]["cba_pillar3"]
    scraper = CBAScraper(source_path=cba_pillar3_xlsx, config=cfg,
                         reporting_date=date(2025, 6, 30))
    valid = scraper.validate(scraper.scrape())
    # All 10 points are within the APS 330 plausibility ranges
    assert len(valid) == 10


def test_validate_drops_out_of_range_residential_pd(
    tmp_path: Path, sources_config: dict,
) -> None:
    """A residential PD of 50% must fail the (0.003, 0.025) bound."""
    import openpyxl

    cfg = sources_config["sources"]["cba_pillar3"]
    bad_path = tmp_path / "cba_bad.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "IRB Credit Risk"
    ws.append(["Portfolio", "Exposure_EAD_Mn", "PD", "LGD"])
    ws.append(["Residential Mortgage", 500000, 0.50, 0.22])  # 50% PD — implausible
    wb.save(bad_path)

    scraper = CBAScraper(source_path=bad_path, config=cfg,
                         reporting_date=date(2025, 6, 30))
    raw = scraper.scrape()
    assert any(p.raw_value == 0.50 for p in raw)
    valid = scraper.validate(raw)
    # The 50% PD must be dropped; LGD (0.22) is fine.
    assert not any(p.raw_value == 0.50 for p in valid)
    assert any(p.raw_value == 0.22 for p in valid)


# ---------------------------------------------------------------------------
# Peer comparison helper
# ---------------------------------------------------------------------------

def test_peer_comparison_flag_triggers_above_3x_divergence() -> None:
    # Median is 0.01; our value 0.04 is 4x above — should flag
    assert Pillar3BaseScraper.peer_comparison_flag(0.04, peer_median=0.01) is True
    # Our value 0.012 is within 3x — should not flag
    assert Pillar3BaseScraper.peer_comparison_flag(0.012, peer_median=0.01) is False
    # Value 0.003 is ~3.3x below — should flag
    assert Pillar3BaseScraper.peer_comparison_flag(0.003, peer_median=0.01) is True


def test_peer_comparison_handles_zero_edge_cases() -> None:
    # peer_median = 0 -> cannot divide; return False (governance handles separately)
    assert Pillar3BaseScraper.peer_comparison_flag(0.01, peer_median=0.0) is False
    # value = 0 but peers non-zero -> suspicious
    assert Pillar3BaseScraper.peer_comparison_flag(0.0, peer_median=0.01) is True


# ---------------------------------------------------------------------------
# Period code helper
# ---------------------------------------------------------------------------

def test_default_cba_period_code_for_june_30() -> None:
    assert default_cba_period_code(date(2025, 6, 30)) == "FY2025"


def test_default_cba_period_code_for_december_31_is_h1_next_fy() -> None:
    """Dec 31 2024 is half-way through FY2025 (CBA's fiscal year)."""
    assert default_cba_period_code(date(2024, 12, 31)) == "H1FY2025"


# ---------------------------------------------------------------------------
# Transform — source_id format matches the user-specified pattern
# ---------------------------------------------------------------------------

def test_transform_produces_expected_cba_source_ids(
    cba_pillar3_xlsx: Path, sources_config: dict,
) -> None:
    cfg = sources_config["sources"]["cba_pillar3"]
    scraper = CBAScraper(source_path=cba_pillar3_xlsx, config=cfg,
                         reporting_date=date(2025, 6, 30))
    points = scraper.scrape()
    entries = [
        scraped_to_entry(p, override_asset_class_map=cfg["asset_class_mapping"])
        for p in points
    ]

    ids = {e.source_id for e in entries}
    assert "CBA_RESIDENTIAL_MORTGAGE_PD_FY2025" in ids
    assert "CBA_RESIDENTIAL_MORTGAGE_LGD_FY2025" in ids
    assert "CBA_COMMERCIAL_PROPERTY_INVESTMENT_PD_FY2025" in ids
    assert "CBA_CORPORATE_SME_PD_FY2025" in ids
    # Slotting entries include metric_code disambiguator
    assert "CBA_DEVELOPMENT_STRONG_PD_FY2025" in ids
    assert "CBA_DEVELOPMENT_WEAK_PD_FY2025" in ids


def test_transform_tags_source_type_as_pillar3(
    cba_pillar3_xlsx: Path, sources_config: dict,
) -> None:
    cfg = sources_config["sources"]["cba_pillar3"]
    scraper = CBAScraper(source_path=cba_pillar3_xlsx, config=cfg,
                         reporting_date=date(2025, 6, 30))
    entry = scraped_to_entry(
        scraper.scrape()[0],
        override_asset_class_map=cfg["asset_class_mapping"],
    )
    assert entry.source_type == SourceType.PILLAR3
    assert entry.data_type in {DataType.PD, DataType.LGD}


# ---------------------------------------------------------------------------
# Orchestrator round-trip
# ---------------------------------------------------------------------------

def test_orchestrator_refresh_cba_adds_ten_entries(
    cba_pillar3_xlsx: Path, sources_config: dict,
) -> None:
    engine = create_engine_and_schema(":memory:")
    registry = BenchmarkRegistry(engine, actor="test")
    orch = RefreshOrchestrator(
        registry=registry, sources_config=sources_config,
        local_overrides={"cba_pillar3": cba_pillar3_xlsx},
        scraper_extras={
            "cba_pillar3": {"reporting_date": date(2025, 6, 30)},
        },
    )
    report = orch.refresh_source("cba_pillar3")
    assert report.errors == []
    assert report.counts.get("add", 0) == 10

    entries = registry.list()
    assert len(entries) == 10
    assert all(e.source_type == SourceType.PILLAR3 for e in entries)


def test_orchestrator_refresh_cba_is_idempotent(
    cba_pillar3_xlsx: Path, sources_config: dict,
) -> None:
    engine = create_engine_and_schema(":memory:")
    registry = BenchmarkRegistry(engine, actor="test")
    orch = RefreshOrchestrator(
        registry=registry, sources_config=sources_config,
        local_overrides={"cba_pillar3": cba_pillar3_xlsx},
        scraper_extras={"cba_pillar3": {"reporting_date": date(2025, 6, 30)}},
    )
    orch.refresh_source("cba_pillar3")
    report2 = orch.refresh_source("cba_pillar3")
    assert report2.counts.get("skip_unchanged", 0) == 10


# ---------------------------------------------------------------------------
# NAB / WBC / ANZ — no longer stubs; real PdfPillar3Scraper subclasses.
# Keeping these tests as smoke checks for the class contract (source_name,
# expected_frequency_days) and to ensure scrape() now reaches the PDF path
# rather than raising NotImplementedError.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("ScraperCls", [NABScraper, WBCScraper, ANZScraper])
def test_pdf_scrapers_reach_pdf_path_not_stub_error(
    ScraperCls, sources_config: dict, tmp_path: Path,
) -> None:
    """Phase-2-completion check: scrape() on a missing PDF raises FileNotFoundError
    (not NotImplementedError). Real parsing happens only when a real PDF exists."""
    key_by_cls = {
        NABScraper: "nab_pillar3",
        WBCScraper: "wbc_pillar3",
        ANZScraper: "anz_pillar3",
    }
    cfg = sources_config["sources"][key_by_cls[ScraperCls]]
    scraper = ScraperCls(source_path=tmp_path / "dummy.pdf", config=cfg)
    with pytest.raises(FileNotFoundError, match="Pillar 3 PDF not found"):
        scraper.scrape()


def test_scrapers_expose_source_name_and_frequency(
    sources_config: dict, tmp_path: Path,
) -> None:
    scraper = NABScraper(
        source_path=tmp_path / "x.pdf",
        config=sources_config["sources"]["nab_pillar3"],
    )
    assert scraper.source_name == "NAB_PILLAR3"
    assert scraper.expected_frequency_days == 180
