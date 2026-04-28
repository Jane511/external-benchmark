"""Tests for ingestion/adapters/metrics_credit_adapter.py."""
from __future__ import annotations

from pathlib import Path

from ingestion.adapters.metrics_credit_adapter import MetricsCreditAdapter
from ingestion.adapters.non_bank_base import CANONICAL_OBSERVATION_COLUMNS
from src.models import SourceType


def test_metrics_credit_adapter_declares_canonical_columns() -> None:
    adapter = MetricsCreditAdapter()
    assert adapter.SOURCE_ID == "metrics_credit"
    assert adapter.SOURCE_TYPE == SourceType.NON_BANK_LISTED
    assert set(adapter.canonical_columns) == set(CANONICAL_OBSERVATION_COLUMNS)


def test_metrics_credit_adapter_returns_empty_frame_when_file_missing(tmp_path: Path) -> None:
    adapter = MetricsCreditAdapter()
    df = adapter.normalise(tmp_path / "does_not_exist.pdf")
    assert df.empty
    assert set(df.columns) == set(CANONICAL_OBSERVATION_COLUMNS)


def test_metrics_credit_adapter_validate_output_passes_on_empty_frame(tmp_path: Path) -> None:
    adapter = MetricsCreditAdapter()
    df = adapter.normalise(tmp_path / "missing.pdf")
    adapter.validate_output(df)  # must not raise
