"""Tests for ingestion/base.py — ScrapedDataPoint + BaseScraper contract."""
from __future__ import annotations

from datetime import date

import pytest

from ingestion.base import BaseScraper, ScrapedDataPoint, plausible_value_range


def test_scraped_data_point_is_frozen() -> None:
    p = ScrapedDataPoint(
        source_name="X", publisher="APRA", raw_value=0.01, raw_unit="ratio",
        value_date=date(2025, 9, 30), period_years=1, asset_class_raw="Residential",
        geography="AU", url="http://x", retrieval_date=date(2025, 12, 1),
    )
    with pytest.raises(Exception):  # FrozenInstanceError from dataclasses
        p.raw_value = 0.02  # type: ignore[misc]


def test_scraped_data_point_default_dicts_independent() -> None:
    a = ScrapedDataPoint(
        source_name="A", publisher="APRA", raw_value=0.01, raw_unit="ratio",
        value_date=date(2025, 9, 30), period_years=1, asset_class_raw="r",
        geography="AU", url="x", retrieval_date=date(2025, 12, 1),
    )
    b = ScrapedDataPoint(
        source_name="B", publisher="APRA", raw_value=0.02, raw_unit="ratio",
        value_date=date(2025, 9, 30), period_years=1, asset_class_raw="r",
        geography="AU", url="x", retrieval_date=date(2025, 12, 1),
    )
    a.metadata["k"] = "v"  # not shared with b because of default_factory
    assert "k" not in b.metadata


def test_plausible_value_range_helper() -> None:
    assert plausible_value_range(0.05, 0.0, 0.2) is True
    assert plausible_value_range(-0.01, 0.0, 0.2) is False
    assert plausible_value_range(0.21, 0.0, 0.2) is False


def test_base_scraper_cannot_be_instantiated() -> None:
    with pytest.raises(TypeError):
        BaseScraper()  # type: ignore[abstract]
