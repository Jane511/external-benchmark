"""Tests for the Plenti quarterly trading update PDF adapter.

Synthesised slide-text fixture mimics Plenti's standard trading-update
wording (per Q4 FY25 release). Parser is regex-driven over text so
tests don't have to commit a binary PDF.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from ingestion.adapters.non_bank_base import CANONICAL_OBSERVATION_COLUMNS
from ingestion.adapters.plenti_disclosure_adapter import PlentiDisclosureAdapter
from src.models import DataDefinitionClass, RawObservation, SourceType


_PLENTI_PAGE = (
    "Plenti Group — Quarterly Trading Update Q4 FY25\n"
    "For the quarter ended 31 March 2025\n"
    "\n"
    "Loan portfolio of $2.4B at the end of the quarter, +18% YoY.\n"
    "\n"
    "Annualised net losses for the quarter were 116 basis points "
    "(Q3 FY25: 110 bps).\n"
    "\n"
    "90+ day arrears of 43 basis points at the end of the quarter.\n"
    "\n"
    "Automotive loans portfolio grew to $1.6B (+22% YoY); 90+ day "
    "arrears of 38 basis points.\n"
    "\n"
    "Personal loans portfolio of $0.6B; annualised net losses of "
    "180 basis points.\n"
)


def test_plenti_extracts_loss_rate_from_quarterly_update() -> None:
    adapter = PlentiDisclosureAdapter()
    rows = adapter._extract_observations_from_text(_PLENTI_PAGE)
    loss_rows = [
        r for r in rows
        if r["data_definition_class"] == DataDefinitionClass.LOSS_EXPENSE_RATE
    ]
    assert loss_rows, "no annualised net loss rows extracted"
    # 116 bps → 0.0116
    values = sorted(round(r["value"], 6) for r in loss_rows)
    assert 0.0116 in values


def test_plenti_extracts_90plus_arrears() -> None:
    adapter = PlentiDisclosureAdapter()
    rows = adapter._extract_observations_from_text(_PLENTI_PAGE)
    arrears_rows = [
        r for r in rows
        if r["data_definition_class"] == DataDefinitionClass.ARREARS_90_PLUS_DAYS
    ]
    assert arrears_rows, "no 90+ DPD arrears rows extracted"
    values = sorted(round(r["value"], 6) for r in arrears_rows)
    assert 0.0043 in values


def test_plenti_period_end_resolved_from_quarter_ended() -> None:
    adapter = PlentiDisclosureAdapter()
    rows = adapter._extract_observations_from_text(_PLENTI_PAGE)
    assert rows
    assert rows[0]["as_of_date"] == date(2025, 3, 31)


def test_plenti_loan_portfolio_captured_as_sample_size() -> None:
    """'$2.4B loan portfolio' should land in sample_size_n (in $M)."""
    adapter = PlentiDisclosureAdapter()
    rows = adapter._extract_observations_from_text(_PLENTI_PAGE)
    assert rows
    assert rows[0]["sample_size_n"] == 2_400


def test_plenti_automotive_block_classified_as_consumer_secured() -> None:
    adapter = PlentiDisclosureAdapter()
    rows = adapter._extract_observations_from_text(_PLENTI_PAGE)
    secured = [r for r in rows if r["segment"] == "consumer_secured"]
    # The automotive 38 bps arrears must land under consumer_secured
    arrears = [
        r for r in secured
        if r["data_definition_class"] == DataDefinitionClass.ARREARS_90_PLUS_DAYS
    ]
    assert arrears, "no automotive (consumer_secured) arrears row extracted"
    values = sorted(round(r["value"], 6) for r in arrears)
    assert 0.0038 in values


def test_plenti_rows_validate_against_raw_observation() -> None:
    adapter = PlentiDisclosureAdapter()
    rows = adapter._extract_observations_from_text(_PLENTI_PAGE)
    for r in rows:
        RawObservation(**r)


def test_plenti_normalise_missing_file_returns_empty(tmp_path: Path) -> None:
    adapter = PlentiDisclosureAdapter()
    df = adapter.normalise(tmp_path / "does_not_exist.pdf")
    assert isinstance(df, pd.DataFrame)
    assert df.empty
    assert set(df.columns) == set(CANONICAL_OBSERVATION_COLUMNS)
