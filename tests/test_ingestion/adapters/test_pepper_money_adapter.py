"""Tests for the Pepper Money results-presentation PDF adapter.

Synthesised slide-text fixtures mimic the bullet-pointed style Pepper
uses in its half-yearly results pack. The parser is text-driven so the
tests don't have to commit a binary PDF.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from ingestion.adapters.non_bank_base import CANONICAL_OBSERVATION_COLUMNS
from ingestion.adapters.pepper_money_adapter import PepperMoneyAdapter
from src.models import DataDefinitionClass, RawObservation, SourceType


_PEPPER_PAGE = (
    "Pepper Money — 1H FY25 Results Presentation\n"
    "For the half year ended 31 December 2024\n"
    "\n"
    "Asset Finance\n"
    "  - AUM grew 8% YoY to $4.8B\n"
    "  - Loan loss expense of 1.85% (FY24: 1.42%)\n"
    "  - 90+ days arrears 2.10% (FY24: 1.88%)\n"
    "\n"
    "Residential Mortgages\n"
    "  - Settlements +12% YoY\n"
    "  - 90+ days arrears 0.95%\n"
    "\n"
    "SME / Commercial\n"
    "  - Loan loss expense of 0.75%\n"
    "  - 30+ days arrears 1.20%\n"
)


def test_pepper_extracts_asset_finance_loss_and_arrears() -> None:
    adapter = PepperMoneyAdapter()
    rows = adapter._extract_observations_from_text(_PEPPER_PAGE)
    assert rows, "Pepper extractor produced zero rows"

    by_segment = {}
    for r in rows:
        by_segment.setdefault(r["segment"], []).append(r)

    # Asset finance: at least loss_rate and 90+ DPD arrears
    af_rows = by_segment.get("consumer_secured", [])
    assert af_rows, "no asset-finance (consumer_secured) rows extracted"
    af_params = {(r["parameter"], r["data_definition_class"]) for r in af_rows}
    assert ("loss_rate", DataDefinitionClass.LOSS_EXPENSE_RATE) in af_params
    assert ("arrears", DataDefinitionClass.ARREARS_90_PLUS_DAYS) in af_params


def test_pepper_residential_mortgage_arrears_extracted() -> None:
    adapter = PepperMoneyAdapter()
    rows = adapter._extract_observations_from_text(_PEPPER_PAGE)
    res = [r for r in rows if r["segment"] == "residential_mortgage"]
    assert res, "no residential mortgage rows extracted"
    arrears = [
        r for r in res
        if r["data_definition_class"] == DataDefinitionClass.ARREARS_90_PLUS_DAYS
    ]
    assert arrears, "no 90+ DPD arrears row for residential mortgages"
    assert abs(arrears[0]["value"] - 0.0095) < 1e-9


def test_pepper_sme_loss_rate_extracted() -> None:
    adapter = PepperMoneyAdapter()
    rows = adapter._extract_observations_from_text(_PEPPER_PAGE)
    sme = [r for r in rows if r["segment"] == "sme_corporate"]
    assert sme, "no SME / commercial rows extracted"
    loss_rate_rows = [
        r for r in sme
        if r["data_definition_class"] == DataDefinitionClass.LOSS_EXPENSE_RATE
    ]
    assert loss_rate_rows, "no loan loss expense row for SME"
    assert abs(loss_rate_rows[0]["value"] - 0.0075) < 1e-9


def test_pepper_period_end_resolved_from_heading() -> None:
    adapter = PepperMoneyAdapter()
    rows = adapter._extract_observations_from_text(_PEPPER_PAGE)
    assert rows
    # 1H FY25 = period ending 31 December 2024
    assert rows[0]["as_of_date"] == date(2024, 12, 31)


def test_pepper_rows_validate_against_raw_observation() -> None:
    adapter = PepperMoneyAdapter()
    rows = adapter._extract_observations_from_text(_PEPPER_PAGE)
    for r in rows:
        RawObservation(**r)


def test_pepper_normalise_missing_file_returns_empty(tmp_path: Path) -> None:
    adapter = PepperMoneyAdapter()
    df = adapter.normalise(tmp_path / "does_not_exist.pdf")
    assert isinstance(df, pd.DataFrame)
    assert df.empty
    assert set(df.columns) == set(CANONICAL_OBSERVATION_COLUMNS)


def test_pepper_extracts_at_least_four_observations_from_realistic_pack() -> None:
    """Brief asserts >=4 observations from a typical results pack."""
    adapter = PepperMoneyAdapter()
    rows = adapter._extract_observations_from_text(_PEPPER_PAGE)
    assert len(rows) >= 4, (
        f"expected at least 4 observations, got {len(rows)}: "
        f"{[(r['segment'], r['parameter']) for r in rows]}"
    )
