"""Tests for scripts/migrate_to_raw_observations.py.

Covers the new `data_definition_class` inference plus end-to-end
migration of seed data into raw_observations.
"""
from __future__ import annotations

import pytest

from scripts.migrate_to_raw_observations import (
    _infer_definition_class,
    _infer_parameter,
    migrate,
)
from src.db import create_engine_and_schema
from src.models import DataDefinitionClass, DataType
from src.registry import BenchmarkRegistry
from src.seed_data import SEED_ENTRIES, load_seed_data


@pytest.mark.parametrize("source_id,data_type,expected", [
    ("CBA_PILLAR3_RES_2024H2", "pd", DataDefinitionClass.BASEL_PD_ONE_YEAR),
    ("NAB_PILLAR3_CRE_PD_2024H2", "pd", DataDefinitionClass.BASEL_PD_ONE_YEAR),
    ("CBA_PILLAR3_RES_LGD_2024H2", "lgd", DataDefinitionClass.REALISED_LOSS_RATE),
    ("APS113_RES_LGD_FLOOR", "supervisory_value",
     DataDefinitionClass.REGULATORY_FLOOR_LGD),
    ("APS113_SLOTTING_STRONG_PD", "pd", DataDefinitionClass.REGULATORY_FLOOR_PD),
    ("APS113_SLOTTING_GOOD_LGD", "lgd",
     DataDefinitionClass.REGULATORY_FLOOR_LGD),
    ("APS113_INVOICE_LGD_FLOOR", "supervisory_value",
     DataDefinitionClass.REGULATORY_FLOOR_LGD),
    ("APRA_QPEX_CRE_IMPAIRED_2024Q4", "impaired_ratio",
     DataDefinitionClass.IMPAIRED_LOANS_RATIO),
    ("APRA_PERF_COMMERCIAL_NPL_2024Q4", "impaired_ratio",
     DataDefinitionClass.NPL_RATIO),
    ("RBA_FSR_HH_ARREARS_90PLUS_2024H2", "impaired_ratio",
     DataDefinitionClass.ARREARS_90_PLUS_DAYS),
    ("SP_SPIN_PRIME_RMBS_30PLUS_2024Q4", "default_rate",
     DataDefinitionClass.ARREARS_30_PLUS_DAYS),
    ("sp_spin_prime", "default_rate",
     DataDefinitionClass.ARREARS_30_PLUS_DAYS),
    ("LATROBE_BRIDGING_REALISED_LOSS", "lgd",
     DataDefinitionClass.REALISED_LOSS_RATE),
    ("QUALITAS_CRE_COMMENTARY_2024H2", "pd",
     DataDefinitionClass.QUALITATIVE_COMMENTARY),
    ("METRICS_CRE_COMMENTARY_2024H2", "pd",
     DataDefinitionClass.QUALITATIVE_COMMENTARY),
])
def test_infer_definition_class_known_sources(source_id, data_type, expected):
    assert _infer_definition_class(source_id, data_type) is expected


def test_infer_parameter_for_aps113_lgd_returns_lgd() -> None:
    """APS 113 LGD slotting must come back as parameter='lgd', not 'pd'."""
    cls = _infer_definition_class("APS113_RES_LGD_FLOOR", "supervisory_value")
    assert _infer_parameter(cls, "APS113_RES_LGD_FLOOR", "supervisory_value") == "lgd"


def test_infer_parameter_for_pillar3_lgd_returns_lgd() -> None:
    cls = _infer_definition_class("CBA_PILLAR3_RES_LGD_2024H2", "lgd")
    assert _infer_parameter(cls, "CBA_PILLAR3_RES_LGD_2024H2", "lgd") == "lgd"


def test_migrate_idempotent(tmp_path) -> None:
    """Running migration twice must not duplicate rows."""
    db_path = tmp_path / "bench.db"
    engine = create_engine_and_schema(str(db_path))
    reg = BenchmarkRegistry(engine, actor="test")
    load_seed_data(reg)

    s1, m1, _ = migrate(str(db_path))
    s2, m2, sk2 = migrate(str(db_path))
    assert m1 > 0
    assert m2 == 0, (
        "second migration inserted rows; idempotency broken "
        f"(scanned={s2}, migrated={m2}, skipped={sk2})"
    )


def test_migrate_seeded_reality_check_rows_have_definition_class(tmp_path) -> None:
    """End-to-end: seed -> migrate -> every reality-check row carries a class."""
    db_path = tmp_path / "bench.db"
    engine = create_engine_and_schema(str(db_path))
    reg = BenchmarkRegistry(engine, actor="test")
    load_seed_data(reg)
    migrate(str(db_path))

    # Read the migrated rows back via the registry; spot-check a handful.
    rows = reg.query_observations()
    assert rows, "no rows migrated"

    by_id = {r.source_id: r for r in rows}

    apra_qpex = by_id.get("APRA_QPEX_CRE_IMPAIRED_2024Q4")
    assert apra_qpex is not None
    assert apra_qpex.parameter == "impaired"
    assert apra_qpex.data_definition_class is DataDefinitionClass.IMPAIRED_LOANS_RATIO

    apra_npl = by_id.get("APRA_PERF_COMMERCIAL_NPL_2024Q4")
    assert apra_npl is not None
    assert apra_npl.parameter == "npl"
    assert apra_npl.data_definition_class is DataDefinitionClass.NPL_RATIO

    spin = by_id.get("SP_SPIN_PRIME_RMBS_30PLUS_2024Q4")
    assert spin is not None
    assert spin.parameter == "arrears"
    assert spin.data_definition_class is DataDefinitionClass.ARREARS_30_PLUS_DAYS

    staged_spin = by_id.get("sp_spin_prime")
    assert staged_spin is not None
    assert staged_spin.parameter == "arrears"
    assert staged_spin.data_definition_class is DataDefinitionClass.ARREARS_30_PLUS_DAYS

    qualitas = by_id.get("QUALITAS_CRE_COMMENTARY_2024H2")
    assert qualitas is not None
    assert qualitas.parameter == "commentary"
    assert qualitas.data_definition_class is DataDefinitionClass.QUALITATIVE_COMMENTARY

    cba_basel = by_id.get("CBA_PILLAR3_CRE_PD_2024H2")
    assert cba_basel is not None
    assert cba_basel.parameter == "pd"
    assert cba_basel.data_definition_class is DataDefinitionClass.BASEL_PD_ONE_YEAR
