"""Smoke tests for the external_indices adapters."""
from __future__ import annotations

import pytest

from ingestion.adapters.non_bank_base import CANONICAL_OBSERVATION_COLUMNS
from ingestion.external_indices.rba_fsr_aggregates_adapter import (
    RbaFsrAggregatesAdapter,
)
from ingestion.external_indices.rba_securitisation_aggregates_adapter import (
    RbaSecuritisationAggregatesAdapter,
)
from ingestion.external_indices.sp_spin_adapter import SpSpinAdapter
from src.models import SourceType


_ADAPTERS = [
    SpSpinAdapter,
    RbaSecuritisationAggregatesAdapter,
    RbaFsrAggregatesAdapter,
]


@pytest.mark.parametrize("adapter_cls", _ADAPTERS)
def test_external_index_adapter_canonical_shape(adapter_cls, tmp_path):
    adapter = adapter_cls()
    assert set(adapter.canonical_columns) == set(CANONICAL_OBSERVATION_COLUMNS)
    df = adapter.normalise(tmp_path / "missing.pdf")
    assert df.empty


@pytest.mark.parametrize("adapter_cls,expected_type", [
    (SpSpinAdapter, SourceType.RATING_AGENCY_INDEX),
    (RbaSecuritisationAggregatesAdapter, SourceType.RBA_AGGREGATE),
    (RbaFsrAggregatesAdapter, SourceType.RBA_AGGREGATE),
])
def test_external_index_source_type(adapter_cls, expected_type):
    adapter = adapter_cls()
    assert adapter.SOURCE_TYPE == expected_type
