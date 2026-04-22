"""Tests for ingestion/icc_trade.py — ICC Trade Register scraper (Phase 3).

All tests feed canonical JSON fixture into `_build_points_from_tables()`,
bypassing pdfplumber. Real-PDF exercise is a tuning task for when a report
copy arrives.
"""
from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path

import pytest

from ingestion.icc_trade import IccTradeScraper
from ingestion.refresh import RefreshOrchestrator
from ingestion.transform import scraped_to_entry
from src.db import create_engine_and_schema
from src.models import DataType, SourceType
from src.registry import BenchmarkRegistry


FIXTURE = Path(__file__).parent.parent / "fixtures" / "icc_trade_tables.json"


@pytest.fixture()
def icc_fixture() -> dict:
    return json.loads(FIXTURE.read_text())


@pytest.fixture()
def icc_scraper(sources_config: dict) -> IccTradeScraper:
    return IccTradeScraper(
        config=sources_config["sources"]["icc_trade"],
        retrieval_date=date(2025, 1, 15),
    )


# ---------------------------------------------------------------------------
# Fixture -> ScrapedDataPoints
# ---------------------------------------------------------------------------

def test_fixture_parse_produces_expected_point_count(
    icc_scraper: IccTradeScraper, icc_fixture: dict,
) -> None:
    """5 products × 3 measures = 15 default-rate points; 3 LGD rows × 2 = 6 LGD points.

    Total before validation: 21. All fixture values are within plausibility
    ranges so validation should keep all 21.
    """
    points = icc_scraper._build_points_from_tables(icc_fixture)
    assert len(points) == 21

    valid = icc_scraper.validate(points)
    assert len(valid) == 21


def test_three_measures_produce_three_entries_per_product(
    icc_scraper: IccTradeScraper, icc_fixture: dict,
) -> None:
    points = icc_scraper._build_points_from_tables(icc_fixture)
    import_lc_points = [
        p for p in points
        if p.asset_class_raw == "Import LC"
        and p.metadata.get("data_type_hint") == "default_rate"
    ]
    assert len(import_lc_points) == 3
    metric_codes = {p.metadata.get("metric_column") for p in import_lc_points}
    assert metric_codes == {"EXPOSURE_WEIGHTED", "OBLIGOR_WEIGHTED", "TRANSACTION_WEIGHTED"}


# ---------------------------------------------------------------------------
# Product label normalisation
# ---------------------------------------------------------------------------

def test_product_label_normalisation_covers_common_variants(
    icc_scraper: IccTradeScraper,
) -> None:
    fn = icc_scraper._normalise_product
    assert fn("Import LC") == "trade_import_lc"
    assert fn("Import Letter of Credit") == "trade_import_lc"
    assert fn("Export LC") == "trade_export_lc"
    assert fn("Export Letter of Credit") == "trade_export_lc"
    assert fn("Performance guarantee") == "trade_performance_guarantee"
    assert fn("Performance guarantees and standbys") == "trade_performance_guarantee"
    assert fn("Standby letter of credit") == "trade_performance_guarantee"
    assert fn("Trade loan") == "trade_loan"
    assert fn("Trade loans") == "trade_loan"
    assert fn("SCF payables") == "scf_payables"
    assert fn("Supply chain finance") == "scf_payables"
    assert fn("Payables finance") == "scf_payables"


# ---------------------------------------------------------------------------
# Source ID format
# ---------------------------------------------------------------------------

def test_source_id_format_matches_spec(
    icc_scraper: IccTradeScraper, icc_fixture: dict,
) -> None:
    """Expected: ICC_{ASSET_CLASS}_{METRIC}_{DATA_TYPE}_FY{YEAR}.

    The canonical asset class slug comes via transform.py's override map,
    which we pass explicitly here to mirror what the orchestrator does.
    """
    points = icc_scraper._build_points_from_tables(icc_fixture)
    asset_map = {k.title(): v for k, v in icc_scraper.PRODUCT_MAP.items()}
    # Easier: build a map that matches the fixture's raw labels explicitly.
    asset_map = {
        "Import LC": "trade_import_lc",
        "Export LC": "trade_export_lc",
        "Performance guarantee": "trade_performance_guarantee",
        "Trade loan": "trade_loan",
        "SCF payables": "scf_payables",
    }

    source_ids = {
        scraped_to_entry(p, override_asset_class_map=asset_map).source_id
        for p in points
    }

    # Default rate source_ids — all three measures for Import LC
    assert "ICC_TRADE_IMPORT_LC_EXPOSURE_WEIGHTED_DEFAULT_RATE_FY2024" in source_ids
    assert "ICC_TRADE_IMPORT_LC_OBLIGOR_WEIGHTED_DEFAULT_RATE_FY2024" in source_ids
    assert "ICC_TRADE_IMPORT_LC_TRANSACTION_WEIGHTED_DEFAULT_RATE_FY2024" in source_ids

    # Other products
    assert "ICC_TRADE_LOAN_OBLIGOR_WEIGHTED_DEFAULT_RATE_FY2024" in source_ids
    assert "ICC_SCF_PAYABLES_TRANSACTION_WEIGHTED_DEFAULT_RATE_FY2024" in source_ids

    # LGD + recovery rate (no metric_code disambiguator)
    assert "ICC_TRADE_IMPORT_LC_LGD_FY2024" in source_ids
    assert "ICC_TRADE_IMPORT_LC_RECOVERY_RATE_FY2024" in source_ids


def test_transform_tags_source_type_as_icc_trade(
    icc_scraper: IccTradeScraper, icc_fixture: dict,
) -> None:
    points = icc_scraper._build_points_from_tables(icc_fixture)
    entry = scraped_to_entry(points[0])
    assert entry.source_type == SourceType.ICC_TRADE
    assert entry.data_type in {DataType.DEFAULT_RATE, DataType.LGD, DataType.RECOVERY_RATE}
    assert entry.geography == "GLOBAL"


# ---------------------------------------------------------------------------
# Validation drops out-of-range values with logging
# ---------------------------------------------------------------------------

def test_validation_drops_out_of_range_trade_loan(
    icc_scraper: IccTradeScraper, caplog,
) -> None:
    caplog.set_level(logging.WARNING, logger="ingestion.icc_trade")

    bad = {
        "report_year": 2024,
        "default_rates": [
            # Trade loan max is 0.015; 0.10 should be dropped
            {"product": "Trade loan", "exposure_weighted": 0.10},
        ],
    }
    raw = icc_scraper._build_points_from_tables(bad)
    assert len(raw) == 1
    valid = icc_scraper.validate(raw)
    assert len(valid) == 0
    assert "dropping" in caplog.text.lower()


def test_validation_keeps_values_inside_range(
    icc_scraper: IccTradeScraper,
) -> None:
    ok = {
        "report_year": 2024,
        "default_rates": [
            {"product": "Import LC", "exposure_weighted": 0.0003},
            {"product": "Trade loan", "exposure_weighted": 0.005},
        ],
    }
    points = icc_scraper._build_points_from_tables(ok)
    assert len(icc_scraper.validate(points)) == 2


# ---------------------------------------------------------------------------
# Manual-download semantics: missing source PDF
# ---------------------------------------------------------------------------

def test_missing_icc_cache_dir_raises_with_manual_download_hint(
    sources_config: dict, tmp_path: Path,
) -> None:
    scraper = IccTradeScraper(
        config=sources_config["sources"]["icc_trade"],
        cache_base=tmp_path / "doesnt_exist",
    )
    with pytest.raises(FileNotFoundError, match="Manual-download"):
        scraper.scrape()


def test_missing_icc_pdf_in_cache_dir_raises_with_download_url(
    sources_config: dict, tmp_path: Path,
) -> None:
    (tmp_path / "icc").mkdir()   # cache dir exists but empty
    scraper = IccTradeScraper(
        config=sources_config["sources"]["icc_trade"],
        cache_base=tmp_path,
    )
    with pytest.raises(FileNotFoundError, match="iccwbo.org"):
        scraper.scrape()


def test_specific_report_year_raises_if_that_year_missing(
    sources_config: dict, tmp_path: Path,
) -> None:
    icc_dir = tmp_path / "icc"
    icc_dir.mkdir()
    # Put a 2024 PDF, request 2025
    (icc_dir / "ICC_Trade_Register_2024.pdf").write_bytes(b"%PDF-1.4\n%%EOF\n")
    scraper = IccTradeScraper(
        config=sources_config["sources"]["icc_trade"],
        cache_base=tmp_path,
        report_year=2025,
    )
    with pytest.raises(FileNotFoundError, match="2025 edition"):
        scraper.scrape()


# ---------------------------------------------------------------------------
# Multi-year cache — latest selection
# ---------------------------------------------------------------------------

def test_latest_year_selected_when_report_year_none(
    sources_config: dict, tmp_path: Path,
) -> None:
    icc_dir = tmp_path / "icc"
    icc_dir.mkdir()
    (icc_dir / "ICC_Trade_Register_2022.pdf").write_bytes(b"x")
    (icc_dir / "ICC_Trade_Register_2023.pdf").write_bytes(b"x")
    (icc_dir / "ICC_Trade_Register_2024.pdf").write_bytes(b"x")
    scraper = IccTradeScraper(
        config=sources_config["sources"]["icc_trade"],
        cache_base=tmp_path,
    )
    chosen = scraper._resolve_source_path()
    assert chosen.name == "ICC_Trade_Register_2024.pdf"


def test_report_year_parameter_picks_specific_file(
    sources_config: dict, tmp_path: Path,
) -> None:
    icc_dir = tmp_path / "icc"
    icc_dir.mkdir()
    (icc_dir / "ICC_Trade_Register_2022.pdf").write_bytes(b"x")
    (icc_dir / "ICC_Trade_Register_2023.pdf").write_bytes(b"x")
    (icc_dir / "ICC_Trade_Register_2024.pdf").write_bytes(b"x")
    scraper = IccTradeScraper(
        config=sources_config["sources"]["icc_trade"],
        cache_base=tmp_path,
        report_year=2023,
    )
    chosen = scraper._resolve_source_path()
    assert chosen.name == "ICC_Trade_Register_2023.pdf"


# ---------------------------------------------------------------------------
# Graceful handling when LGD table absent
# ---------------------------------------------------------------------------

def test_lgd_table_absent_default_rates_still_extracted(
    icc_scraper: IccTradeScraper,
) -> None:
    """Fixture with no lgd_rates key should still produce default-rate points."""
    dr_only = {
        "report_year": 2024,
        "default_rates": [
            {"product": "Import LC", "exposure_weighted": 0.0003},
            {"product": "Export LC", "exposure_weighted": 0.0001},
        ],
        # no "lgd_rates" key at all
    }
    points = icc_scraper._build_points_from_tables(dr_only)
    assert len(points) == 2
    assert all(p.metadata.get("data_type_hint") == "default_rate" for p in points)


# ---------------------------------------------------------------------------
# Orchestrator round-trip: scrape via fixture -> transform -> registry
# ---------------------------------------------------------------------------

def test_orchestrator_roundtrip_populates_registry(
    sources_config: dict, icc_fixture: dict, tmp_path: Path, monkeypatch,
) -> None:
    """Monkey-patch IccTradeScraper to skip PDF reading and feed fixture directly."""
    # Set up a fake PDF so _resolve_source_path succeeds
    icc_dir = tmp_path / "icc"
    icc_dir.mkdir()
    fake_pdf = icc_dir / "ICC_Trade_Register_2024.pdf"
    fake_pdf.write_bytes(b"%PDF-1.4\n%%EOF\n")

    # Bypass the actual PDF extraction by stubbing _extract_tables_from_pdf.
    def _stub_extract(self, pdf_path):
        return icc_fixture

    monkeypatch.setattr(IccTradeScraper, "_extract_tables_from_pdf", _stub_extract)

    engine = create_engine_and_schema(":memory:")
    registry = BenchmarkRegistry(engine, actor="test")
    orch = RefreshOrchestrator(
        registry=registry, sources_config=sources_config,
        local_overrides={"icc_trade": fake_pdf},
        scraper_extras={"icc_trade": {
            "report_year": 2024, "cache_base": tmp_path,
        }},
    )
    report = orch.refresh_source("icc_trade")
    assert report.errors == []
    assert report.counts.get("add", 0) == 21

    entries = registry.list()
    assert len(entries) == 21
    publishers = {e.publisher for e in entries}
    assert publishers == {"ICC"}
