"""Tests for ingestion/asic_abs_import.py — failure-rate importer (Phase 6)."""
from __future__ import annotations

import logging
import shutil
from datetime import date
from pathlib import Path

import pytest

from ingestion.asic_abs_import import ASICABSFailureRateImporter
from ingestion.transform import scraped_to_entry
from src.models import DataType, SourceType


FIX = Path(__file__).parent.parent / "fixtures"


@pytest.fixture()
def asic_abs_dirs(tmp_path: Path) -> tuple[Path, Path]:
    """Stage the fixture CSVs in isolated asic/ and abs/ dirs under tmp_path."""
    asic = tmp_path / "asic"
    absd = tmp_path / "abs"
    asic.mkdir()
    absd.mkdir()
    shutil.copy(FIX / "asic_sample.csv", asic / "asic_insolvency_extract.csv")
    shutil.copy(FIX / "abs_sample.csv", absd / "abs_business_counts.csv")
    return asic, absd


@pytest.fixture()
def importer_with_fixtures(asic_abs_dirs) -> ASICABSFailureRateImporter:
    asic, absd = asic_abs_dirs
    return ASICABSFailureRateImporter(
        asic_dir=asic, abs_dir=absd,
        config={"source_name": "asic_abs"},
        retrieval_date=date(2025, 11, 1),
    )


# ---------------------------------------------------------------------------
# Both files present -> failure rates computed
# ---------------------------------------------------------------------------

def test_both_files_present_produces_five_failure_rate_points(
    importer_with_fixtures: ASICABSFailureRateImporter,
) -> None:
    points = importer_with_fixtures.scrape()
    assert len(points) == 5
    assert all(p.metadata.get("data_type_hint") == "failure_rate" for p in points)


def test_failure_rate_computation_correct(
    importer_with_fixtures: ASICABSFailureRateImporter,
) -> None:
    """Construction: 1240 / 395000 ≈ 0.00314."""
    points = importer_with_fixtures.scrape()
    by_industry = {p.asset_class_raw: p.raw_value for p in points}
    assert by_industry["industry_construction"] == pytest.approx(1240 / 395000)
    assert by_industry["industry_retail_trade"] == pytest.approx(680 / 152000)


# ---------------------------------------------------------------------------
# Missing inputs: WARNING + empty
# ---------------------------------------------------------------------------

def test_asic_missing_logs_abs_guidance_and_returns_empty(
    tmp_path: Path, caplog,
) -> None:
    caplog.set_level(logging.WARNING, logger="ingestion.asic_abs_import")
    absd = tmp_path / "abs"
    absd.mkdir()
    shutil.copy(FIX / "abs_sample.csv", absd / "abs_business_counts.csv")

    importer = ASICABSFailureRateImporter(
        asic_dir=tmp_path / "asic_missing",  # doesn't exist
        abs_dir=absd,
        config={},
    )
    result = importer.scrape()
    assert result == []
    assert "asic insolvency data missing" in caplog.text.lower()
    assert "asic.gov.au" in caplog.text.lower()


def test_abs_missing_logs_asic_guidance_and_returns_empty(
    tmp_path: Path, caplog,
) -> None:
    caplog.set_level(logging.WARNING, logger="ingestion.asic_abs_import")
    asic = tmp_path / "asic"
    asic.mkdir()
    shutil.copy(FIX / "asic_sample.csv", asic / "asic_insolvency_extract.csv")

    importer = ASICABSFailureRateImporter(
        asic_dir=asic,
        abs_dir=tmp_path / "abs_missing",  # doesn't exist
        config={},
    )
    result = importer.scrape()
    assert result == []
    assert "abs business counts missing" in caplog.text.lower()
    assert "abs.gov.au" in caplog.text.lower()


def test_both_missing_logs_both_warnings(tmp_path: Path, caplog) -> None:
    caplog.set_level(logging.WARNING, logger="ingestion.asic_abs_import")
    importer = ASICABSFailureRateImporter(
        asic_dir=tmp_path / "a", abs_dir=tmp_path / "b", config={},
    )
    assert importer.scrape() == []
    lower = caplog.text.lower()
    assert "asic insolvency data missing" in lower
    assert "abs business counts missing" in lower


# ---------------------------------------------------------------------------
# Industry label normalisation + ANZSIC mapping
# ---------------------------------------------------------------------------

def test_industry_label_normalisation_is_case_insensitive_and_comma_tolerant(
    tmp_path: Path,
) -> None:
    """ASIC writes 'Transport, Postal and Warehousing' (with comma), ABS might
    write it without one. The merge must still succeed."""
    asic = tmp_path / "asic"; asic.mkdir()
    absd = tmp_path / "abs"; absd.mkdir()
    (asic / "asic_insolvency_extract.csv").write_text(
        "as_of_date,industry,insolvency_count,source_note\n"
        '2025-09-30,"Transport, Postal and Warehousing",280,ASIC Q3 2025\n'
    )
    (absd / "abs_business_counts.csv").write_text(
        "as_of_date,anzsic_division_code,industry,business_count,source_note\n"
        "2024-06-30,I,Transport Postal and Warehousing,105000,ABS 8165\n"
    )
    importer = ASICABSFailureRateImporter(
        asic_dir=asic, abs_dir=absd, config={},
    )
    points = importer.scrape()
    assert len(points) == 1
    assert points[0].asset_class_raw == "industry_transport"


def test_anzsic_map_covers_fixture_industries(
    importer_with_fixtures: ASICABSFailureRateImporter,
) -> None:
    points = importer_with_fixtures.scrape()
    assets = {p.asset_class_raw for p in points}
    assert assets == {
        "industry_construction",
        "industry_retail_trade",
        "industry_accommodation_food",
        "industry_manufacturing",
        "industry_transport",
    }


# ---------------------------------------------------------------------------
# Source ID format + transform
# ---------------------------------------------------------------------------

def test_source_id_format_matches_spec(
    importer_with_fixtures: ASICABSFailureRateImporter,
) -> None:
    """Expected: ASIC_ABS_{asset_class_upper}_FAILURE_RATE_{YYYYQn}."""
    points = importer_with_fixtures.scrape()
    ids = {scraped_to_entry(p).source_id for p in points}
    assert "ASIC_ABS_INDUSTRY_CONSTRUCTION_FAILURE_RATE_2025Q3" in ids
    assert "ASIC_ABS_INDUSTRY_RETAIL_TRADE_FAILURE_RATE_2025Q3" in ids


def test_transform_produces_failure_rate_benchmark_entries(
    importer_with_fixtures: ASICABSFailureRateImporter,
) -> None:
    points = importer_with_fixtures.scrape()
    entries = [scraped_to_entry(p) for p in points]
    for e in entries:
        assert e.data_type == DataType.FAILURE_RATE
        assert e.source_type == SourceType.INSOLVENCY
        assert 0 <= e.value <= 0.1


# ---------------------------------------------------------------------------
# Validation drops out-of-range with WARNING
# ---------------------------------------------------------------------------

def test_validation_drops_failure_rate_above_ten_percent(
    tmp_path: Path, caplog,
) -> None:
    """Manufactured row with insolvency_count exceeding business_count --
    failure_rate > 1 (implausible) must be dropped with a WARNING."""
    caplog.set_level(logging.WARNING, logger="ingestion.asic_abs_import")
    asic = tmp_path / "asic"; asic.mkdir()
    absd = tmp_path / "abs"; absd.mkdir()
    (asic / "asic_insolvency_extract.csv").write_text(
        "as_of_date,industry,insolvency_count,source_note\n"
        "2025-09-30,Construction,5000,bad\n"       # very high -> ~0.5 failure
        "2025-09-30,Retail Trade,680,ok\n"
    )
    (absd / "abs_business_counts.csv").write_text(
        "as_of_date,anzsic_division_code,industry,business_count,source_note\n"
        "2024-06-30,E,Construction,10000,bad\n"    # small denominator -> ~0.5
        "2024-06-30,G,Retail Trade,152000,ok\n"
    )
    importer = ASICABSFailureRateImporter(
        asic_dir=asic, abs_dir=absd, config={},
    )
    points = importer.scrape()
    # Only Retail Trade passes (Construction's 0.5 exceeds the 0.10 ceiling)
    assert len(points) == 1
    assert points[0].asset_class_raw == "industry_retail_trade"
    assert "dropping" in caplog.text.lower()


# ---------------------------------------------------------------------------
# Missing join key -> WARNING + skip, continue
# ---------------------------------------------------------------------------

def test_asic_industry_not_in_abs_logs_warning_and_skips_that_sector(
    tmp_path: Path, caplog,
) -> None:
    caplog.set_level(logging.WARNING, logger="ingestion.asic_abs_import")
    asic = tmp_path / "asic"; asic.mkdir()
    absd = tmp_path / "abs"; absd.mkdir()
    (asic / "asic_insolvency_extract.csv").write_text(
        "as_of_date,industry,insolvency_count,source_note\n"
        "2025-09-30,Construction,1240,ok\n"
        "2025-09-30,Nonexistent Made Up Sector,50,stray ASIC row\n"
    )
    (absd / "abs_business_counts.csv").write_text(
        "as_of_date,anzsic_division_code,industry,business_count,source_note\n"
        "2024-06-30,E,Construction,395000,ok\n"
    )
    importer = ASICABSFailureRateImporter(
        asic_dir=asic, abs_dir=absd, config={},
    )
    points = importer.scrape()
    # Construction still emits; Nonexistent sector dropped with WARNING.
    assert len(points) == 1
    assert points[0].asset_class_raw == "industry_construction"
    assert "no abs counterpart" in caplog.text.lower()


# ---------------------------------------------------------------------------
# File-pattern flexibility (8165* alt filename)
# ---------------------------------------------------------------------------

def test_abs_file_pattern_accepts_cat8165_filename(tmp_path: Path) -> None:
    asic = tmp_path / "asic"; asic.mkdir()
    absd = tmp_path / "abs"; absd.mkdir()
    shutil.copy(FIX / "asic_sample.csv", asic / "asic_insolvency_extract.csv")
    # ABS raw filename starts with 81650
    shutil.copy(FIX / "abs_sample.csv", absd / "81650DO001_202306.csv")

    importer = ASICABSFailureRateImporter(
        asic_dir=asic, abs_dir=absd, config={},
    )
    points = importer.scrape()
    assert len(points) == 5
