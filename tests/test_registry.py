"""Tests for src/registry.py — versioning, segment queries, audit trail.

Covers the five Tier 2 design requirements from the user brief:
  1. get_by_segment() filters latest-version only
  2. supersede() is atomic (prior.superseded_by set in same txn as new insert)
  3. get_version_history() returns ascending version order
  4. Seed data covers 7+ segments with authentic AU values
  5. Every registry operation writes to audit_log
"""
from __future__ import annotations

import json
from datetime import date
from typing import Any

import pytest
from sqlalchemy import func, select

from src.db import AuditLog, Benchmark, create_engine_and_schema, make_session_factory
from src.models import (
    BenchmarkEntry,
    Component,
    Condition,
    DataType,
    QualityScore,
    SourceType,
)
from src.registry import BenchmarkRegistry
from src.seed_data import SEED_ENTRIES, load_seed_data
from ingestion.source_registry import SOURCE_URLS


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def registry_and_inspector():
    """Fresh in-memory registry + a separate read-only session for test inspection."""
    engine = create_engine_and_schema(":memory:")
    registry = BenchmarkRegistry(engine, actor="test")
    factory = make_session_factory(engine)
    yield registry, factory


def _sample(**overrides: Any) -> BenchmarkEntry:
    base: dict[str, Any] = {
        "source_id": "CBA_PILLAR3_RES_2024H2",
        "publisher": "Commonwealth Bank of Australia",
        "source_type": SourceType.PILLAR3,
        "data_type": DataType.PD,
        "asset_class": "residential_mortgage",
        "value": 0.0072,
        "value_date": date(2024, 12, 31),
        "period_years": 5,
        "geography": "AU",
        "url": "https://www.commbank.com.au/pillar3",
        "retrieval_date": date(2025, 3, 1),
        "quality_score": QualityScore.HIGH,
    }
    base.update(overrides)
    return BenchmarkEntry(**base)


def _audit_rows(factory) -> list[AuditLog]:
    with factory() as s:
        return list(s.scalars(select(AuditLog).order_by(AuditLog.pk)).all())


# ---------------------------------------------------------------------------
# Basic add / list
# ---------------------------------------------------------------------------

def test_add_and_list_roundtrip(registry_and_inspector) -> None:
    registry, _ = registry_and_inspector
    registry.add(_sample(source_id="A"))
    registry.add(_sample(source_id="B", value=0.009))

    entries = registry.list()
    assert {e.source_id for e in entries} == {"A", "B"}


# ---------------------------------------------------------------------------
# Requirement 1: get_by_segment() returns LATEST VERSION ONLY
# ---------------------------------------------------------------------------

def test_get_by_segment_returns_latest_version_only(registry_and_inspector) -> None:
    registry, factory = registry_and_inspector
    registry.add(_sample(source_id="CBA_RES", value=0.0072))
    registry.supersede(
        "CBA_RES",
        _sample(source_id="CBA_RES", value=0.0085, retrieval_date=date(2025, 9, 1)),
    )

    hits = registry.get_by_segment(
        asset_class="residential_mortgage", data_type=DataType.PD,
    )
    assert len(hits) == 1
    assert hits[0].version == 2
    assert hits[0].value == 0.0085


def test_get_by_segment_filters_by_condition_and_component(registry_and_inspector) -> None:
    """LGD decomposition queries hit only matching (condition, component) rows."""
    registry, _ = registry_and_inspector
    registry.add(_sample(
        source_id="HAIRCUT_NORMAL", data_type=DataType.LGD,
        asset_class="bridging_residential", value=0.10,
        condition=Condition.NORMAL, component=Component.HAIRCUT,
    ))
    registry.add(_sample(
        source_id="HAIRCUT_DOWNTURN", data_type=DataType.LGD,
        asset_class="bridging_residential", value=0.25,
        condition=Condition.DOWNTURN, component=Component.HAIRCUT,
    ))
    registry.add(_sample(
        source_id="RECOVERY_TIME", data_type=DataType.LGD,
        asset_class="bridging_residential", value=9.0,
        condition=Condition.NORMAL, component=Component.TIME_TO_RECOVERY,
    ))

    downturn_haircuts = registry.get_by_segment(
        asset_class="bridging_residential", data_type=DataType.LGD,
        condition=Condition.DOWNTURN, component=Component.HAIRCUT,
    )
    assert [e.source_id for e in downturn_haircuts] == ["HAIRCUT_DOWNTURN"]


# ---------------------------------------------------------------------------
# Requirement 2: supersede() is atomic
# ---------------------------------------------------------------------------

def test_supersede_creates_v2_and_points_v1(registry_and_inspector) -> None:
    registry, factory = registry_and_inspector
    registry.add(_sample(source_id="S", value=0.08))
    registry.supersede("S", _sample(source_id="S", value=0.09))

    with factory() as s:
        rows = s.scalars(
            select(Benchmark).where(Benchmark.source_id == "S").order_by(Benchmark.version)
        ).all()
    assert [r.version for r in rows] == [1, 2]
    assert rows[0].superseded_by == "S"
    assert rows[0].value == 0.08  # content preserved
    assert rows[1].superseded_by is None
    assert rows[1].value == 0.09


def test_supersede_atomic_both_or_neither(registry_and_inspector) -> None:
    """If the new entry is invalid (IntegrityError / mismatched ID), prior row is unchanged."""
    registry, factory = registry_and_inspector
    registry.add(_sample(source_id="S", value=0.08))

    # Pydantic rejects mismatched source_id with its own ValueError
    with pytest.raises(ValueError, match="does not match"):
        registry.supersede("S", _sample(source_id="WRONG_ID", value=0.09))

    with factory() as s:
        rows = s.scalars(
            select(Benchmark).where(Benchmark.source_id == "S")
        ).all()
    assert len(rows) == 1
    assert rows[0].superseded_by is None  # prior row untouched


def test_supersede_missing_source_raises(registry_and_inspector) -> None:
    registry, _ = registry_and_inspector
    with pytest.raises(ValueError, match="No active benchmark"):
        registry.supersede("NONEXISTENT", _sample(source_id="NONEXISTENT"))


# ---------------------------------------------------------------------------
# Requirement 3: get_version_history() ascending order
# ---------------------------------------------------------------------------

def test_get_version_history_ascending_across_three_versions(registry_and_inspector) -> None:
    registry, _ = registry_and_inspector
    registry.add(_sample(source_id="S", value=0.08))
    registry.supersede("S", _sample(source_id="S", value=0.085))
    registry.supersede("S", _sample(source_id="S", value=0.092))

    history = registry.get_version_history("S")
    assert [e.version for e in history] == [1, 2, 3]
    assert [e.value for e in history] == [0.08, 0.085, 0.092]
    assert history[0].superseded_by == "S"
    assert history[1].superseded_by == "S"
    assert history[2].superseded_by is None


# ---------------------------------------------------------------------------
# Requirement 4: seed data covers 7+ segments
# ---------------------------------------------------------------------------

def test_seed_covers_at_least_seven_segments(registry_and_inspector) -> None:
    registry, _ = registry_and_inspector
    count = load_seed_data(registry)

    entries = registry.list()
    assert count == len(entries) == len(SEED_ENTRIES)

    segments = {e.asset_class for e in entries}
    assert len(segments) >= 7, f"Expected >=7 segments, got {len(segments)}: {segments}"


def test_seed_includes_pc_flagship_cre_pd_values(registry_and_inspector) -> None:
    """The flagship CRE PD test needs CBA 2.5% available to the adjustment engine."""
    registry, _ = registry_and_inspector
    load_seed_data(registry)

    cre_pds = registry.get_by_segment(
        asset_class="commercial_property_investment", data_type=DataType.PD,
    )
    cba_cre = next(e for e in cre_pds if e.source_id.startswith("CBA_"))
    assert cba_cre.value == 0.025


# ---------------------------------------------------------------------------
# Requirement 5: every operation writes to audit_log
# ---------------------------------------------------------------------------

def test_every_operation_writes_audit_log(registry_and_inspector) -> None:
    registry, factory = registry_and_inspector

    def audit_count() -> int:
        with factory() as s:
            return s.scalars(select(func.count(AuditLog.pk))).one()

    before = audit_count()

    registry.add(_sample(source_id="X"))
    assert audit_count() == before + 1

    registry.list()
    assert audit_count() == before + 2

    registry.get_by_segment(
        asset_class="residential_mortgage", data_type=DataType.PD,
    )
    assert audit_count() == before + 3

    registry.get_by_source_type(SourceType.PILLAR3)
    assert audit_count() == before + 4

    registry.supersede("X", _sample(source_id="X", value=0.009))
    assert audit_count() == before + 5

    registry.get_version_history("X")
    assert audit_count() == before + 6

    registry.export(format="json")
    assert audit_count() == before + 7


def test_audit_log_captures_operation_and_actor(registry_and_inspector) -> None:
    registry, factory = registry_and_inspector
    registry.add(_sample(source_id="X"))

    rows = _audit_rows(factory)
    add_rows = [r for r in rows if r.operation == "add"]
    assert len(add_rows) == 1
    assert add_rows[0].entity_id == "X"
    assert add_rows[0].actor == "test"
    assert json.loads(add_rows[0].params_json)["data_type"] == "pd"


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

def test_export_json_returns_latest_rows(registry_and_inspector) -> None:
    registry, _ = registry_and_inspector
    registry.add(_sample(source_id="S", value=0.08))
    registry.supersede("S", _sample(source_id="S", value=0.09))

    payload = registry.export(format="json")
    rows = json.loads(payload)
    assert len(rows) == 1
    assert rows[0]["value"] == 0.09
    assert rows[0]["version"] == 2


def test_export_csv_has_header_and_one_row(registry_and_inspector) -> None:
    registry, _ = registry_and_inspector
    registry.add(_sample(source_id="S"))
    csv = registry.export(format="csv")
    lines = csv.strip().splitlines()
    assert lines[0].startswith("source_id,version,")
    assert len(lines) == 2  # header + one row


def test_source_registry_includes_macquarie_pillar3() -> None:
    spec = SOURCE_URLS["mqg_pillar3"]
    assert spec["cache_dir"] == "data/raw/pillar3/"
    assert spec["manual_download"] is False
    assert spec["files"][0]["filename_pattern"] == "MQG_{half}_{year}_Pillar3.pdf"


def test_source_registry_includes_rba_publications() -> None:
    assert "reserved_future" not in SOURCE_URLS
    expected = {
        "rba_fsr": "RBA_FSR_{period}.pdf",
        "rba_smp": "RBA_SMP_{quarter}_{year}.pdf",
        "rba_chart_pack": "RBA_ChartPack_{quarter}_{year}.pdf",
    }
    for key, pattern in expected.items():
        spec = SOURCE_URLS[key]
        assert spec["cache_dir"] == "data/raw/rba/"
        assert spec["manual_download"] is False
        assert spec["files"][0]["filename_pattern"] == pattern


def test_source_registry_includes_governance_publications() -> None:
    """APRA Insight + CFR are auto-monitored governance sources (manifest-deduped)."""
    assert "apra_insight" in SOURCE_URLS
    apra = SOURCE_URLS["apra_insight"]
    assert apra["cache_dir"] == "data/raw/apra/insight/"
    assert apra["manual_download"] is False
    assert apra["files"][0]["url"].startswith(
        "https://www.apra.gov.au/news-and-publications/apra-insight"
    )

    assert "cfr_publications" in SOURCE_URLS
    cfr = SOURCE_URLS["cfr_publications"]
    assert cfr["cache_dir"] == "data/raw/cfr/"
    assert cfr["manual_download"] is False
    assert cfr["files"][0]["url"].startswith("https://www.cfr.gov.au/publications/")
