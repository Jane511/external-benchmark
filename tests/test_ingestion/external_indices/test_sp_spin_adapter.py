"""Tests for the S&P SPIN adapter."""
from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ingestion.adapters.non_bank_base import CANONICAL_OBSERVATION_COLUMNS
from ingestion.external_indices.sp_spin_adapter import SpSpinAdapter


@pytest.fixture
def adapter() -> SpSpinAdapter:
    return SpSpinAdapter()


def test_adapter_returns_empty_for_missing_file(
    adapter: SpSpinAdapter, tmp_path: Path
) -> None:
    result = adapter.normalise(tmp_path / "missing.pdf")
    assert isinstance(result, pd.DataFrame)
    assert result.empty
    assert set(result.columns) == set(CANONICAL_OBSERVATION_COLUMNS)


def test_extract_reporting_period_as_of_phrasing(adapter: SpSpinAdapter) -> None:
    text = "RMBS Arrears Statistics Australia As of February 28, 2026"
    assert adapter._extract_reporting_period(text) == date(2026, 2, 28)


def test_extract_reporting_period_standard_phrasing(adapter: SpSpinAdapter) -> None:
    text = "...arrears in July 2025 increased to 0.93%..."
    assert adapter._extract_reporting_period(text) == date(2025, 7, 31)


def test_extract_reporting_period_title_phrasing(adapter: SpSpinAdapter) -> None:
    text = "Australian RMBS Arrears Statistics - December 2024 (Excluding...)"
    assert adapter._extract_reporting_period(text) == date(2024, 12, 31)


def test_extract_prime_spin_table_row_uses_latest_month(
    adapter: SpSpinAdapter,
) -> None:
    text = "Prime SPIN 0.97 0.93 0.94 0.88 0.79"
    assert adapter._extract_prime_spin(text) == pytest.approx(0.0079)


def test_extract_non_conforming_spin_table_row_uses_latest_month(
    adapter: SpSpinAdapter,
) -> None:
    text = "Non-Conforming SPIN 4.39 4.36 3.97 3.78 3.90"
    assert adapter._extract_non_conforming_spin(text) == pytest.approx(0.0390)


def test_extract_prime_spin_standard(adapter: SpSpinAdapter) -> None:
    text = "The weighted-average prime SPIN of 0.93% in July 2025..."
    assert adapter._extract_prime_spin(text) == pytest.approx(0.0093)


def test_extract_prime_spin_alternative_phrasing(adapter: SpSpinAdapter) -> None:
    text = "...the prime SPIN rose to 1.05% in March 2024..."
    assert adapter._extract_prime_spin(text) == pytest.approx(0.0105)


def test_extract_non_conforming_spin(adapter: SpSpinAdapter) -> None:
    text = "The non-conforming SPIN of 3.45% in July 2025..."
    assert adapter._extract_non_conforming_spin(text) == pytest.approx(0.0345)


def test_extract_returns_none_when_value_missing(adapter: SpSpinAdapter) -> None:
    text = "This PDF has no SPIN values for some reason."
    assert adapter._extract_prime_spin(text) is None
    assert adapter._extract_non_conforming_spin(text) is None


@patch("ingestion.external_indices.sp_spin_adapter.pdfplumber.open")
def test_full_parse_produces_two_observations(
    mock_pdf_open: MagicMock, adapter: SpSpinAdapter, tmp_path: Path
) -> None:
    pdf_path = tmp_path / "spin_2025_07.pdf"
    pdf_path.write_bytes(b"fake pdf content")

    mock_page = MagicMock()
    mock_page.extract_text.return_value = (
        "Australian RMBS Arrears Statistics - July 2025 "
        "The weighted-average prime SPIN of 0.93% in July 2025 "
        "and non-conforming SPIN of 3.45%..."
    )
    mock_pdf = MagicMock()
    mock_pdf.pages = [mock_page]
    mock_pdf_open.return_value.__enter__.return_value = mock_pdf

    result = adapter.normalise(pdf_path)

    assert len(result) == 2
    assert set(result["source_id"]) == {"sp_spin_prime", "sp_spin_non_conforming"}

    prime_row = result[result["source_id"] == "sp_spin_prime"].iloc[0]
    assert prime_row["value"] == pytest.approx(0.0093)
    assert prime_row["segment"] == "residential_mortgage"
    assert prime_row["data_definition_class"] == "arrears_30_plus_days"
    assert prime_row["as_of_date"] == "2025-07-31"

    nc_row = result[result["source_id"] == "sp_spin_non_conforming"].iloc[0]
    assert nc_row["value"] == pytest.approx(0.0345)
    assert nc_row["segment"] == "residential_mortgage_specialist"


@patch("ingestion.external_indices.sp_spin_adapter.pdfplumber.open")
def test_partial_parse_returns_only_extractable_values(
    mock_pdf_open: MagicMock, adapter: SpSpinAdapter, tmp_path: Path
) -> None:
    pdf_path = tmp_path / "spin_2025_07.pdf"
    pdf_path.write_bytes(b"fake pdf content")

    mock_page = MagicMock()
    mock_page.extract_text.return_value = (
        "Australian RMBS Arrears Statistics - July 2025 "
        "The weighted-average prime SPIN of 0.93% in July 2025."
    )
    mock_pdf = MagicMock()
    mock_pdf.pages = [mock_page]
    mock_pdf_open.return_value.__enter__.return_value = mock_pdf

    result = adapter.normalise(pdf_path)

    assert len(result) == 1
    assert result.iloc[0]["source_id"] == "sp_spin_prime"


@patch("ingestion.external_indices.sp_spin_adapter.pdfplumber.open")
def test_unparseable_pdf_returns_empty_frame(
    mock_pdf_open: MagicMock, adapter: SpSpinAdapter, tmp_path: Path
) -> None:
    pdf_path = tmp_path / "bad.pdf"
    pdf_path.write_bytes(b"fake pdf content")
    mock_pdf_open.side_effect = Exception("PDF parse failed")

    result = adapter.normalise(pdf_path)
    assert result.empty
