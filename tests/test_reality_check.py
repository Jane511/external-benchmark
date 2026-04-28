"""Tests for src/reality_check.py and config/reality_check_bands.yaml.

Reality-check bands are the contract between the engine and downstream
consumers. These tests verify:
  - The YAML loads cleanly into typed dataclasses
  - Every PD-workbook product has a band
  - Every source_id referenced in any band actually resolves in
    raw_observations after seed + migration
"""
from __future__ import annotations

import pytest

from scripts.migrate_to_raw_observations import migrate
from src.db import create_engine_and_schema
from src.reality_check import (
    RealityCheckBand,
    RealityCheckBandLibrary,
    load_reality_check_bands,
)
from src.registry import BenchmarkRegistry
from src.seed_data import load_seed_data


def test_load_reality_check_bands_returns_library() -> None:
    library = load_reality_check_bands()
    assert isinstance(library, RealityCheckBandLibrary)
    assert library.last_review_date == "2026-04-28"
    assert library.next_review_due == "2026-10-31"


def test_reality_check_bands_cover_all_pd_workbook_products() -> None:
    """The 8 products in the PD workbook must each have a band."""
    library = load_reality_check_bands()
    expected = {
        "term_loan", "line_of_credit", "invoice_finance", "trade_finance",
        "asset_finance", "bridging", "development", "commercial_property",
    }
    actual = set(library.all_products())
    assert expected.issubset(actual), (
        f"Missing reality-check bands for: {sorted(expected - actual)}"
    )


def test_each_band_has_upper_and_lower_pd_in_unit_interval() -> None:
    """Bands are PD values; both bounds must be in [0, 1] and ordered."""
    library = load_reality_check_bands()
    for product, band in library.bands_by_product.items():
        assert isinstance(band, RealityCheckBand)
        assert 0.0 < band.lower_band_pd < band.upper_band_pd < 1.0, (
            f"{product}: bounds out of order or out of range"
        )
        assert band.rationale.strip(), f"{product}: rationale is empty"


def test_for_product_returns_band_or_none() -> None:
    library = load_reality_check_bands()
    cre = library.for_product("commercial_property")
    assert cre is not None
    assert "APRA_QPEX_CRE_IMPAIRED_2024Q4" in cre.upper_sources

    assert library.for_product("nonexistent_product") is None


def test_reality_check_band_sources_resolve_in_raw_observations(tmp_path) -> None:
    """Every source_id referenced by any band must exist in raw_observations
    after the standard seed + migration flow."""
    db_path = tmp_path / "bench.db"
    engine = create_engine_and_schema(str(db_path))
    reg = BenchmarkRegistry(engine, actor="test")
    load_seed_data(reg)

    scanned, migrated, skipped = migrate(str(db_path))
    assert migrated > 0, "migration produced no rows; seed/inference broken"

    library = load_reality_check_bands()
    referenced = library.all_referenced_source_ids()

    # Re-open through registry; query each referenced source_id.
    for source_id in referenced:
        rows = reg.query_observations(definition_classes=None)
        match = [r for r in rows if r.source_id == source_id]
        assert match, (
            f"Reality-check band references source_id={source_id!r} but it "
            f"is not present in raw_observations after migration."
        )


def test_system_wide_references_have_descriptions() -> None:
    library = load_reality_check_bands()
    for key, ref in library.system_wide_references.items():
        assert "source_id" in ref, f"{key} missing source_id"
        assert ref.get("description"), f"{key} missing description"
