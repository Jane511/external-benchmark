"""Tests for the reality-check additions to src/seed_data.py.

After Brief 2's seed extension, every reality-check entry must be:
  - constructable as a BenchmarkEntry without raising
  - migrate-able into raw_observations with a non-None
    data_definition_class (no inference fallthrough)
  - have a unique source_id within the seed set
"""
from __future__ import annotations

import pytest

from scripts.migrate_to_raw_observations import (
    _infer_definition_class,
    _infer_parameter,
)
from src.db import create_engine_and_schema
from src.models import DataDefinitionClass
from src.registry import BenchmarkRegistry
from src.seed_data import (
    SEED_ENTRIES,
    _BIG4_PILLAR3_CRE,
    _METRICS_CREDIT_COMMENTARY,
    _QUALITAS_COMMENTARY,
    _REALITY_CHECK_ENTRIES,
    load_seed_data,
)


def test_seed_entries_have_unique_source_ids() -> None:
    ids = [e.source_id for e in SEED_ENTRIES]
    assert len(ids) == len(set(ids)), (
        f"Duplicate source_id in SEED_ENTRIES: "
        f"{[i for i in ids if ids.count(i) > 1]}"
    )


def test_reality_check_entries_present() -> None:
    """The brief identifies several reality-check sources; all must seed."""
    seeded = {e.source_id for e in _REALITY_CHECK_ENTRIES}
    expected = {
        "APRA_QPEX_CRE_IMPAIRED_2024Q4",
        "APRA_PERF_COMMERCIAL_NPL_2024Q4",
        "RBA_FSR_HH_ARREARS_90PLUS_2024H2",
        "RBA_FSR_BUSINESS_ARREARS_2024H2",
        "sp_spin_prime",
        "sp_spin_non_conforming",
        "SP_SPIN_PRIME_RMBS_30PLUS_2024Q4",
        "SP_SPIN_NON_CONFORMING_30PLUS_2024Q4",
        "CBA_PILLAR3_CRE_PD_2024H2",
        "NAB_PILLAR3_CRE_PD_2024H2",
        "WBC_PILLAR3_CRE_PD_2024H2",
        "ANZ_PILLAR3_CRE_PD_2024H2",
        "QUALITAS_CRE_COMMENTARY_2024H2",
        "METRICS_CRE_COMMENTARY_2024H2",
    }
    missing = expected - seeded
    assert not missing, f"Missing reality-check seed entries: {sorted(missing)}"


@pytest.mark.parametrize("entry", _REALITY_CHECK_ENTRIES, ids=lambda e: e.source_id)
def test_each_reality_check_entry_has_inferable_definition_class(entry) -> None:
    """No reality-check seed entry should fall through `_infer_definition_class`."""
    cls = _infer_definition_class(entry.source_id, entry.data_type.value)
    assert cls is not None, (
        f"Definition-class inference failed for {entry.source_id}"
    )
    parameter = _infer_parameter(cls, entry.source_id, entry.data_type.value)
    assert parameter in {"pd", "lgd", "arrears", "impaired", "npl",
                         "loss_rate", "commentary"}


def test_qualitas_and_metrics_inferred_as_qualitative() -> None:
    """Both commentary placeholders must classify as QUALITATIVE_COMMENTARY."""
    for entry in _QUALITAS_COMMENTARY + _METRICS_CREDIT_COMMENTARY:
        cls = _infer_definition_class(entry.source_id, entry.data_type.value)
        assert cls is DataDefinitionClass.QUALITATIVE_COMMENTARY


def test_big4_pillar3_cre_inferred_as_basel_pd() -> None:
    """The new Big 4 commercial_property entries are Basel PD observations."""
    for entry in _BIG4_PILLAR3_CRE:
        cls = _infer_definition_class(entry.source_id, entry.data_type.value)
        assert cls is DataDefinitionClass.BASEL_PD_ONE_YEAR


def test_load_seed_data_inserts_all_entries() -> None:
    """load_seed_data() inserts every SEED_ENTRIES row."""
    engine = create_engine_and_schema(":memory:")
    reg = BenchmarkRegistry(engine, actor="test")
    inserted = load_seed_data(reg)
    assert inserted == len(SEED_ENTRIES)
