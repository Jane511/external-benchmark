"""Tests for src/csv_exporter.py — CSV bundle for downstream consumers."""
from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

import pytest

from src.csv_exporter import (
    export_all_csvs,
    export_raw_data_inventory,
    export_raw_observations,
    export_reality_check_bands,
    export_segment_trend,
    export_validation_flags,
)
from src.db import create_engine_and_schema
from src.models import DataDefinitionClass, RawObservation, SourceType
from src.registry import BenchmarkRegistry


@pytest.fixture()
def populated_registry() -> BenchmarkRegistry:
    engine = create_engine_and_schema(":memory:")
    reg = BenchmarkRegistry(engine, actor="test")
    reg.add_observations([
        RawObservation(
            source_id="CBA_PILLAR3_RES_2024H2", source_type=SourceType.BANK_PILLAR3,
            segment="residential_mortgage", parameter="pd",
            data_definition_class=DataDefinitionClass.BASEL_PD_ONE_YEAR,
            value=0.0072, as_of_date=date(2024, 12, 31),
            reporting_basis="CBA Pillar 3 H2 2024",
            methodology_note="CR6 EAD-weighted Average PD",
            source_url="https://example.com/cba",
        ),
        RawObservation(
            source_id="CBA_PILLAR3_RES_2024H2", source_type=SourceType.BANK_PILLAR3,
            segment="residential_mortgage", parameter="pd",
            data_definition_class=DataDefinitionClass.BASEL_PD_ONE_YEAR,
            value=0.0068, as_of_date=date(2024, 6, 30),
            reporting_basis="CBA Pillar 3 H1 2024",
            methodology_note="CR6 EAD-weighted Average PD",
            source_url="https://example.com/cba",
        ),
        RawObservation(
            source_id="judo", source_type=SourceType.NON_BANK_LISTED,
            segment="commercial_property", parameter="pd",
            data_definition_class=DataDefinitionClass.BASEL_PD_ONE_YEAR,
            value=0.045, as_of_date=date(2024, 6, 30),
            reporting_basis="Judo H1 FY25",
            methodology_note="Average PD on CRE book",
        ),
    ])
    return reg


def test_export_raw_observations_writes_one_row_per_observation(
    populated_registry, tmp_path
) -> None:
    path = export_raw_observations(populated_registry, out_dir=tmp_path)
    assert path.exists()
    with path.open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) == 3
    by_id = {r["source_id"]: r for r in rows}
    assert by_id["CBA_PILLAR3_RES_2024H2"]["is_big4"] == "true"
    assert by_id["CBA_PILLAR3_RES_2024H2"]["data_definition_class"] == "basel_pd_one_year"
    assert by_id["judo"]["is_big4"] == "false"
    assert by_id["judo"]["source_type"] == "non_bank_listed"


def test_export_validation_flags_one_row_per_segment(
    populated_registry, tmp_path
) -> None:
    path = export_validation_flags(populated_registry, out_dir=tmp_path)
    with path.open(encoding="utf-8") as fh:
        # First line is a `# units:` comment row that documents
        # decimal-vs-percent and the peer-ratio definition. Skip it
        # when reading the table.
        first = fh.readline()
        assert first.startswith("# units:")
        rows = list(csv.DictReader(fh))
    segments = {r["segment"] for r in rows}
    assert segments == {"commercial_property", "residential_mortgage"}
    # The new column name is peer_big4_vs_non_bank_ratio; the legacy
    # bank_vs_nonbank_ratio column has been dropped.
    assert "peer_big4_vs_non_bank_ratio" in rows[0]
    assert "bank_vs_nonbank_ratio" not in rows[0]


def test_export_validation_flag_sources_long_form(
    populated_registry, tmp_path
) -> None:
    """Long-form companion CSV — one row per (segment, flag_type, source_id)."""
    from src.csv_exporter import export_validation_flag_sources
    path = export_validation_flag_sources(populated_registry, out_dir=tmp_path)
    with path.open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    # Header is always present even if no flags fire. Rows here may be
    # empty (the populated_registry fixture is too small to trip outliers).
    assert path.exists()
    for row in rows:
        assert row["flag_type"] in {"outlier", "stale"}


def test_export_segment_trend_writes_current_prior_rows(
    populated_registry, tmp_path
) -> None:
    path = export_segment_trend(populated_registry, out_dir=tmp_path)
    with path.open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) == 1
    row = rows[0]
    assert row["segment"] == "residential_mortgage"
    assert row["parameter"] == "pd"
    assert row["source_id"] == "CBA_PILLAR3_RES_2024H2"
    assert row["current_as_of"] == "2024-12-31"
    assert row["prior_as_of"] == "2024-06-30"
    assert float(row["delta"]) == pytest.approx(0.0004)


def test_export_reality_check_bands_carries_review_dates(tmp_path) -> None:
    path = export_reality_check_bands(out_dir=tmp_path)
    with path.open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    products = {r["product"] for r in rows}
    # The 8 PD-workbook products from config/reality_check_bands.yaml
    assert {
        "term_loan", "line_of_credit", "invoice_finance", "trade_finance",
        "asset_finance", "bridging", "development", "commercial_property",
    }.issubset(products)
    cre = next(r for r in rows if r["product"] == "commercial_property")
    assert float(cre["upper_band_pd"]) > float(cre["lower_band_pd"]) > 0
    assert cre["last_review_date"] == "2026-04-28"


def test_raw_data_inventory_walks_subdirs_and_classifies(tmp_path) -> None:
    raw = tmp_path / "data_raw"
    (raw / "pillar3").mkdir(parents=True)
    (raw / "pillar3" / "CBA_FY25.pdf").write_bytes(b"%PDF-1.4 stub")
    (raw / "non_bank" / "judo").mkdir(parents=True)
    (raw / "non_bank" / "judo" / "_MANUAL.md").write_text("manual hint")
    (raw / "rba").mkdir()
    (raw / "rba" / "fsr-2026-03.pdf").write_bytes(b"%PDF stub")
    (raw / "pillar3" / ".gitkeep").write_text("")  # must be skipped

    path = export_raw_data_inventory(raw_dir=raw, out_dir=tmp_path)
    with path.open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))

    by_file = {r["filename"]: r for r in rows}
    assert ".gitkeep" not in by_file
    assert by_file["CBA_FY25.pdf"]["source_family"] == "pillar3"
    assert by_file["CBA_FY25.pdf"]["subfamily"] == ""  # file directly under family
    assert by_file["CBA_FY25.pdf"]["kind"] == "pdf"
    assert by_file["_MANUAL.md"]["source_family"] == "non_bank"
    assert by_file["_MANUAL.md"]["subfamily"] == "judo"
    assert by_file["_MANUAL.md"]["kind"] == "manual_note"
    assert by_file["fsr-2026-03.pdf"]["source_family"] == "rba"


def test_raw_data_inventory_handles_missing_root(tmp_path) -> None:
    """Missing data/raw/ directory yields an empty CSV (header only)."""
    path = export_raw_data_inventory(
        raw_dir=tmp_path / "does_not_exist", out_dir=tmp_path,
    )
    assert path.exists()
    text = path.read_text(encoding="utf-8")
    # Only the header row is written.
    assert text.strip().count("\n") == 0
    assert text.startswith("source_family,")


def test_export_all_csvs_returns_six_paths(populated_registry, tmp_path) -> None:
    raw = tmp_path / "data_raw"
    raw.mkdir()
    paths = export_all_csvs(
        populated_registry, out_dir=tmp_path / "csv", raw_dir=raw,
    )
    assert set(paths) == {
        "raw_observations", "validation_flags", "validation_flag_sources",
        "segment_trend", "reality_check_bands", "raw_data_inventory",
    }
    for p in paths.values():
        assert p.exists()
        assert p.stat().st_size > 0
