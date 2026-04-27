"""Smoke tests for non-bank disclosure adapters.

Each adapter is a skeleton — the parsing logic is TODO until sample
publications are retrieved. The smoke test verifies the contract:

  - canonical_columns matches CANONICAL_OBSERVATION_COLUMNS
  - source_name is set
  - normalise() with a missing path returns an empty frame (per the
    AbstractAdapter contract)

When real sample files are available, add per-adapter parsing tests.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.adapters.apra_non_adi_adapter import ApraNonAdiAdapter
from ingestion.adapters.judo_disclosure_adapter import JudoDisclosureAdapter
from ingestion.adapters.liberty_annual_adapter import LibertyAnnualAdapter
from ingestion.adapters.moneyme_disclosure_adapter import MoneyMeDisclosureAdapter
from ingestion.adapters.non_bank_base import (
    CANONICAL_OBSERVATION_COLUMNS,
    map_segment,
)
from ingestion.adapters.pepper_money_adapter import PepperMoneyAdapter
from ingestion.adapters.plenti_disclosure_adapter import PlentiDisclosureAdapter
from ingestion.adapters.resimac_disclosure_adapter import ResimacDisclosureAdapter
from ingestion.adapters.wisr_disclosure_adapter import WisrDisclosureAdapter


_NON_BANK_ADAPTERS = [
    JudoDisclosureAdapter,
    LibertyAnnualAdapter,
    PepperMoneyAdapter,
    ResimacDisclosureAdapter,
    MoneyMeDisclosureAdapter,
    PlentiDisclosureAdapter,
    WisrDisclosureAdapter,
    ApraNonAdiAdapter,
]


@pytest.mark.parametrize("adapter_cls", _NON_BANK_ADAPTERS)
def test_adapter_declares_canonical_columns(adapter_cls):
    adapter = adapter_cls()
    assert set(adapter.canonical_columns) == set(CANONICAL_OBSERVATION_COLUMNS)


@pytest.mark.parametrize("adapter_cls", _NON_BANK_ADAPTERS)
def test_adapter_source_name_is_set(adapter_cls):
    adapter = adapter_cls()
    assert adapter.source_name
    assert adapter.source_name != "OVERRIDE_ME"


@pytest.mark.parametrize("adapter_cls", _NON_BANK_ADAPTERS)
def test_adapter_normalise_missing_file_returns_empty(adapter_cls, tmp_path):
    adapter = adapter_cls()
    df = adapter.normalise(tmp_path / "does_not_exist.pdf")
    assert df.empty
    assert set(df.columns) == set(CANONICAL_OBSERVATION_COLUMNS)


def test_segment_mapping_matches_known_aliases():
    assert map_segment("cba", "Commercial property") == "commercial_property"
    assert map_segment("judo", "SME lending") == "sme_corporate"
    assert map_segment("resimac", "Prime") == "residential_mortgage"


def test_segment_mapping_returns_none_for_unknown():
    assert map_segment("cba", "wholly invented label") is None
