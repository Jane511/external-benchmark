"""Tests for the Liberty Financial Group annual report adapter.

Synthesised credit-risk-note text mirrors the impaired-loans table that
sits in Liberty's audited annual report. The parser is text-driven so
the test can drive the regex against a string without committing a
binary PDF.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from ingestion.adapters.liberty_annual_adapter import LibertyAnnualAdapter
from ingestion.adapters.non_bank_base import CANONICAL_OBSERVATION_COLUMNS
from src.models import DataDefinitionClass, RawObservation, SourceType


_LIBERTY_PAGE = (
    "Liberty Financial Group — Annual Report FY25\n"
    "For the year ended 30 June 2025\n"
    "\n"
    "Note 5 — Credit Risk\n"
    "Liberty's credit-risk exposure is concentrated in residential mortgages, "
    "commercial property and motor / asset finance.\n"
    "\n"
    "Impaired loans by segment\n"
    "Residential mortgages    234   12,540   1.87%\n"
    "Commercial property       89    1,210   7.36%\n"
    "Motor / asset finance     31    2,830   1.10%\n"
    "SME loans                 14      460   3.04%\n"
    "\n"
    "Note 6 — Liquidity Risk\n"
    "(unrelated content here)\n"
)


def test_liberty_extracts_impaired_loans_per_segment() -> None:
    adapter = LibertyAnnualAdapter()
    rows = adapter._extract_observations_from_text(_LIBERTY_PAGE)
    assert rows, "Liberty extractor produced zero rows"

    by_segment = {r["segment"]: r for r in rows}
    assert "residential_mortgage" in by_segment
    assert "commercial_property" in by_segment
    assert "consumer_secured" in by_segment  # motor / asset finance

    for r in rows:
        assert r["parameter"] == "impaired"
        assert r["data_definition_class"] == DataDefinitionClass.IMPAIRED_LOANS_RATIO
        assert 0.0 <= r["value"] <= 1.0
        assert r["source_id"] == "liberty"


def test_liberty_residential_impaired_value_matches_published_pct() -> None:
    adapter = LibertyAnnualAdapter()
    rows = adapter._extract_observations_from_text(_LIBERTY_PAGE)
    res = next(r for r in rows if r["segment"] == "residential_mortgage")
    assert abs(res["value"] - 0.0187) < 1e-9


def test_liberty_period_end_from_year_ended() -> None:
    adapter = LibertyAnnualAdapter()
    rows = adapter._extract_observations_from_text(_LIBERTY_PAGE)
    assert rows
    assert rows[0]["as_of_date"] == date(2025, 6, 30)


def test_liberty_extracts_at_least_three_segments() -> None:
    """Brief asserts >=3 observations covering residential, commercial, motor."""
    adapter = LibertyAnnualAdapter()
    rows = adapter._extract_observations_from_text(_LIBERTY_PAGE)
    segments = {r["segment"] for r in rows}
    assert segments >= {
        "residential_mortgage", "commercial_property", "consumer_secured",
    }, f"missing one of the required segments; got {segments}"


def test_liberty_rows_validate_against_raw_observation() -> None:
    adapter = LibertyAnnualAdapter()
    rows = adapter._extract_observations_from_text(_LIBERTY_PAGE)
    for r in rows:
        RawObservation(**r)


def test_liberty_normalise_missing_file_returns_empty(tmp_path: Path) -> None:
    adapter = LibertyAnnualAdapter()
    df = adapter.normalise(tmp_path / "does_not_exist.pdf")
    assert isinstance(df, pd.DataFrame)
    assert df.empty
    assert set(df.columns) == set(CANONICAL_OBSERVATION_COLUMNS)
