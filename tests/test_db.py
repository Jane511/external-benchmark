"""Tests for src/db.py — schema, unique constraint, date round-trips, supersede."""
from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from sqlalchemy import inspect, select
from sqlalchemy.exc import IntegrityError

from src.db import (
    Adjustment,
    AuditLog,
    Benchmark,
    create_engine_and_schema,
    make_session_factory,
)


@pytest.fixture()
def session():
    engine = create_engine_and_schema(":memory:")
    factory = make_session_factory(engine)
    with factory() as s:
        yield s


def _benchmark_row(**overrides) -> Benchmark:
    base = dict(
        source_id="CBA_PILLAR3_RES_2024H2",
        version=1,
        publisher="Commonwealth Bank of Australia",
        source_type="pillar3",
        data_type="pd",
        asset_class="residential_mortgage",
        geography="AU",
        url="https://www.commbank.com.au/pillar3",
        notes="",
        value=0.0072,
        value_date=date(2024, 12, 31),
        period_years=5,
        condition=None,
        component=None,
        retrieval_date=date(2025, 3, 1),
        quality_score="HIGH",
        superseded_by=None,
    )
    base.update(overrides)
    return Benchmark(**base)


# ---------------------------------------------------------------------------

def test_all_tables_created(session) -> None:
    inspector = inspect(session.bind)
    tables = set(inspector.get_table_names())
    # Brief 1: raw_observations is the new write target; benchmarks /
    # adjustments / audit_log persist for backward-compatibility reads.
    assert tables == {
        "benchmarks", "adjustments", "audit_log", "raw_observations",
    }


def test_benchmark_insert_and_fetch_roundtrip(session) -> None:
    session.add(_benchmark_row())
    session.commit()

    row = session.scalars(select(Benchmark)).one()
    assert row.source_id == "CBA_PILLAR3_RES_2024H2"
    assert row.value == 0.0072
    assert row.value_date == date(2024, 12, 31)
    assert row.inserted_at is not None


def test_benchmark_unique_source_id_version_constraint(session) -> None:
    """Inserting a second row with the same (source_id, version) raises IntegrityError."""
    session.add(_benchmark_row(source_id="X", version=1))
    session.commit()

    session.add(_benchmark_row(source_id="X", version=1))
    with pytest.raises(IntegrityError):
        session.commit()


def test_benchmark_condition_and_component_columns_nullable(session) -> None:
    """Aggregate benchmarks leave condition/component NULL; decomposition entries set them."""
    agg = _benchmark_row(source_id="AGG_LGD", data_type="lgd", value=0.22)
    decomposed = _benchmark_row(
        source_id="METRO_RES_HAIRCUT_DOWNTURN",
        data_type="lgd",
        value=0.25,
        condition="downturn",
        component="haircut",
    )
    session.add_all([agg, decomposed])
    session.commit()

    rows = session.scalars(select(Benchmark).order_by(Benchmark.source_id)).all()
    assert len(rows) == 2
    assert rows[0].condition is None and rows[0].component is None
    assert rows[1].condition == "downturn" and rows[1].component == "haircut"


def test_supersede_pattern_updates_prior_row_only(session) -> None:
    """Insert v1, then insert v2 with same source_id, and point prior row at successor."""
    session.add(_benchmark_row(source_id="S", version=1, value=0.08))
    session.commit()

    session.add(_benchmark_row(source_id="S", version=2, value=0.09))
    prior = session.scalars(
        select(Benchmark).where(Benchmark.source_id == "S", Benchmark.version == 1)
    ).one()
    prior.superseded_by = "S"  # pointer-only update; content fields untouched
    session.commit()

    rows = session.scalars(
        select(Benchmark).where(Benchmark.source_id == "S").order_by(Benchmark.version)
    ).all()
    assert len(rows) == 2
    assert rows[0].superseded_by == "S"
    assert rows[0].value == 0.08  # content preserved
    assert rows[1].value == 0.09
    assert rows[1].superseded_by is None


def test_adjustment_insert_append_only(session) -> None:
    session.add(
        Adjustment(
            source_id="X",
            institution_type="private_credit",
            product="bridging_commercial",
            asset_class="commercial_property_investment",
            raw_value=0.025,
            adjusted_value=0.053762,
            steps_json='[{"name":"selection_bias","multiplier":1.7}]',
        )
    )
    session.commit()

    row = session.scalars(select(Adjustment)).one()
    assert row.adjusted_value == pytest.approx(0.053762)
    assert row.applied_at is not None


def test_audit_log_insert(session) -> None:
    session.add(
        AuditLog(
            operation="add_benchmark",
            entity_id="CBA_PILLAR3_RES_2024H2",
            params_json='{"version": 1}',
            result_summary="inserted",
            actor="analyst@example.com",
        )
    )
    session.commit()

    row = session.scalars(select(AuditLog)).one()
    assert row.operation == "add_benchmark"
    assert row.actor == "analyst@example.com"
    assert row.timestamp is not None


def test_timestamp_defaults_are_utc(session) -> None:
    """inserted_at / applied_at / timestamp all default via _utcnow()."""
    session.add(_benchmark_row(source_id="T"))
    session.add(
        Adjustment(
            source_id="T",
            institution_type="bank",
            product="residential_mortgage",
            asset_class="residential_mortgage",
            raw_value=0.008,
            adjusted_value=0.008,
            steps_json="[]",
        )
    )
    session.add(AuditLog(operation="noop", entity_id="T"))
    session.commit()

    b = session.scalars(select(Benchmark).where(Benchmark.source_id == "T")).one()
    a = session.scalars(select(Adjustment).where(Adjustment.source_id == "T")).one()
    al = session.scalars(select(AuditLog).where(AuditLog.entity_id == "T")).one()

    # SQLite strips tz info on read; just assert they're populated and recent.
    now_naive = datetime.now(timezone.utc).replace(tzinfo=None)
    for ts in (b.inserted_at, a.applied_at, al.timestamp):
        assert ts is not None
        assert (now_naive - ts).total_seconds() < 60
