"""Tests for ingestion/transform.py — value normalisation + mapping rules."""
from __future__ import annotations

from datetime import date

import pytest

from ingestion.base import ScrapedDataPoint
from ingestion.transform import (
    generate_source_id,
    infer_data_type,
    map_asset_class,
    map_source_type,
    normalize_value,
    scraped_to_entry,
)
from src.models import DataType, SourceType


# ---------------------------------------------------------------------------
# Unit normalisation
# ---------------------------------------------------------------------------

def test_normalize_value_ratio_passthrough() -> None:
    assert normalize_value(0.015, "ratio") == 0.015


def test_normalize_value_percent_divides_by_100() -> None:
    assert normalize_value(2.5, "percent") == pytest.approx(0.025)


def test_normalize_value_basis_points_divides_by_10000() -> None:
    assert normalize_value(75, "basis_points") == pytest.approx(0.0075)


def test_normalize_value_unknown_unit_raises() -> None:
    with pytest.raises(ValueError, match="Unknown raw_unit"):
        normalize_value(1.0, "furlongs")


# ---------------------------------------------------------------------------
# Source type mapping
# ---------------------------------------------------------------------------

def test_map_source_type_exact_match() -> None:
    assert map_source_type("APRA") == SourceType.APRA_ADI
    assert map_source_type("illion") == SourceType.BUREAU


def test_map_source_type_prefix_match_falls_through() -> None:
    assert map_source_type("APRA (aggregate)") == SourceType.APRA_ADI


def test_map_source_type_unknown_raises() -> None:
    with pytest.raises(ValueError, match="Unknown publisher"):
        map_source_type("FakeBank")


# ---------------------------------------------------------------------------
# Asset class mapping
# ---------------------------------------------------------------------------

def test_map_asset_class_baseline_is_case_insensitive() -> None:
    assert map_asset_class("Residential Mortgage") == "residential_mortgage"
    assert map_asset_class("CRE") == "commercial_property_investment"


def test_map_asset_class_override_wins() -> None:
    override = {"Residential": "residential_mortgage_custom"}
    assert map_asset_class("Residential", override) == "residential_mortgage_custom"


def test_map_asset_class_unknown_label_passes_through_slugged() -> None:
    assert map_asset_class("Space Finance") == "space_finance"


# ---------------------------------------------------------------------------
# Data type inference
# ---------------------------------------------------------------------------

def test_infer_data_type_from_common_hints() -> None:
    assert infer_data_type("pd") == DataType.PD
    assert infer_data_type("90dpd") == DataType.IMPAIRED_RATIO
    assert infer_data_type("npl") == DataType.IMPAIRED_RATIO
    assert infer_data_type("impaired") == DataType.IMPAIRED_RATIO


def test_infer_data_type_unknown_raises() -> None:
    with pytest.raises(ValueError, match="Cannot infer"):
        infer_data_type("mystery_metric")


# ---------------------------------------------------------------------------
# Source ID generation — deterministic
# ---------------------------------------------------------------------------

def test_generate_source_id_deterministic() -> None:
    sid1 = generate_source_id("APRA", "residential_mortgage", DataType.IMPAIRED_RATIO, date(2025, 9, 30))
    sid2 = generate_source_id("APRA", "residential_mortgage", DataType.IMPAIRED_RATIO, date(2025, 9, 30))
    assert sid1 == sid2
    assert sid1 == "APRA_RESIDENTIAL_MORTGAGE_IMPAIRED_RATIO_2025Q3"


def test_generate_source_id_distinct_quarters() -> None:
    q3 = generate_source_id("APRA", "x", DataType.PD, date(2025, 9, 30))
    q4 = generate_source_id("APRA", "x", DataType.PD, date(2025, 12, 31))
    assert q3 != q4
    assert "2025Q3" in q3
    assert "2025Q4" in q4


# ---------------------------------------------------------------------------
# End-to-end: ScrapedDataPoint -> BenchmarkEntry
# ---------------------------------------------------------------------------

def _scraped() -> ScrapedDataPoint:
    return ScrapedDataPoint(
        source_name="APRA_RESIDENTIAL_90DPD",
        publisher="APRA",
        raw_value=0.012,
        raw_unit="ratio",
        value_date=date(2025, 9, 30),
        period_years=1,
        asset_class_raw="Residential",
        geography="AU",
        url="https://www.apra.gov.au/x",
        retrieval_date=date(2025, 12, 1),
        quality_indicators={"coverage": "all_adis"},
        metadata={"data_type_hint": "impaired_ratio"},
    )


def test_scraped_to_entry_produces_valid_benchmark_entry() -> None:
    # No metric_column in metadata -> source_id uses the base form
    entry = scraped_to_entry(_scraped())
    assert entry.source_id == "APRA_RESIDENTIAL_MORTGAGE_IMPAIRED_RATIO_2025Q3"
    assert entry.source_type == SourceType.APRA_ADI
    assert entry.data_type == DataType.IMPAIRED_RATIO
    assert entry.asset_class == "residential_mortgage"
    assert entry.value == 0.012
    assert entry.value_date == date(2025, 9, 30)


def test_scraped_to_entry_uses_override_asset_class_map() -> None:
    entry = scraped_to_entry(
        _scraped(),
        override_asset_class_map={"Residential": "residential_mortgage"},
    )
    assert entry.asset_class == "residential_mortgage"


def test_scraped_to_entry_carries_quality_indicators_into_notes() -> None:
    entry = scraped_to_entry(_scraped())
    assert "coverage=all_adis" in entry.notes
