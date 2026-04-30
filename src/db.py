"""SQLite storage layer for the External Benchmark Engine.

Three tables — `benchmarks`, `raw_observations`, `audit_log`.

Immutability contract (enforced by the registry layer, declared here):
- `benchmarks` content fields are write-once. Corrections insert a new
  row with `version = prior.version + 1`; the prior row's `superseded_by`
  is set to the new source_id. No other `UPDATE` to this table is allowed.
- `raw_observations` is append-only. Corrections are inserted as a new
  row with a fresher `as_of_date`; old rows are never mutated.
- `audit_log` is append-only.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

from sqlalchemy import (
    Date,
    DateTime,
    Float,
    Integer,
    String,
    UniqueConstraint,
    create_engine,
    inspect,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Benchmark(Base):
    """Raw external benchmark row. Versioned; never updated in place."""
    __tablename__ = "benchmarks"
    __table_args__ = (UniqueConstraint("source_id", "version", name="uq_source_version"),)

    pk: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    publisher: Mapped[str] = mapped_column(String, nullable=False)
    source_type: Mapped[str] = mapped_column(String, nullable=False, index=True)
    data_type: Mapped[str] = mapped_column(String, nullable=False, index=True)
    asset_class: Mapped[str] = mapped_column(String, nullable=False, index=True)
    geography: Mapped[str] = mapped_column(String, nullable=False)
    url: Mapped[str] = mapped_column(String, nullable=False)
    notes: Mapped[str] = mapped_column(String, default="")

    value: Mapped[float] = mapped_column(Float, nullable=False)
    value_date: Mapped[date] = mapped_column(Date, nullable=False)
    period_years: Mapped[int] = mapped_column(Integer, nullable=False)

    # LGD decomposition fields — nullable for aggregate benchmarks.
    condition: Mapped[Optional[str]] = mapped_column(String, nullable=True, index=True)
    component: Mapped[Optional[str]] = mapped_column(String, nullable=True, index=True)

    retrieval_date: Mapped[date] = mapped_column(Date, nullable=False)
    quality_score: Mapped[str] = mapped_column(String, nullable=False)

    superseded_by: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    inserted_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)


class RawObservationRow(Base):
    """Raw, source-attributable PD/LGD observation (Brief 1).

    Append-only. The engine no longer mutates source values — corrections
    are inserted as a new row with a fresher `as_of_date`. Consumers query
    by (segment, parameter, source_type) and decide which vintage to use.
    """
    __tablename__ = "raw_observations"

    pk: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    source_type: Mapped[str] = mapped_column(String, nullable=False, index=True)
    segment: Mapped[str] = mapped_column(String, nullable=False, index=True)
    product: Mapped[Optional[str]] = mapped_column(String, nullable=True, index=True)
    parameter: Mapped[str] = mapped_column(String, nullable=False, index=True)
    data_definition_class: Mapped[str] = mapped_column(
        String, nullable=False, index=True,
        server_default="basel_pd_one_year",
    )
    # value is nullable: parameter='commentary' rows carry value=None
    # (qualitative narrative only). All other parameters require a value;
    # the Pydantic RawObservation validator enforces that contract on the
    # write path, so this column-level nullable=True only relaxes storage.
    value: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    as_of_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    reporting_basis: Mapped[str] = mapped_column(String, nullable=False)
    methodology_note: Mapped[str] = mapped_column(String, nullable=False)

    sample_size_n: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    period_start: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    period_end: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    source_url: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    page_or_table_ref: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    inserted_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)


class AuditLog(Base):
    """One row per state-changing operation. Append-only."""
    __tablename__ = "audit_log"

    pk: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    operation: Mapped[str] = mapped_column(String, nullable=False, index=True)
    entity_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    params_json: Mapped[str] = mapped_column(String, default="{}")
    result_summary: Mapped[str] = mapped_column(String, default="")
    actor: Mapped[str] = mapped_column(String, default="system")
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)


def create_engine_and_schema(db_path: str | Path = ":memory:") -> Engine:
    """Create a SQLAlchemy engine and initialise all three tables.

    Pass `":memory:"` for ephemeral tests or a filesystem path for
    persistent storage.
    """
    url = "sqlite:///:memory:" if db_path == ":memory:" else f"sqlite:///{db_path}"
    engine = create_engine(url, echo=False, future=True)
    Base.metadata.create_all(engine)
    _ensure_data_definition_class_column(engine)
    return engine


def _ensure_data_definition_class_column(engine: Engine) -> None:
    """Add ``data_definition_class`` to legacy ``raw_observations`` tables.

    SQLite ``CREATE TABLE`` from ``Base.metadata.create_all`` is a no-op
    when the table already exists, so existing databases miss the new
    column. Inspect and ``ALTER TABLE`` in-place; back-fill defaults that
    line up with each row's ``parameter``:

      - ``parameter='pd'``  -> ``basel_pd_one_year`` (Big 4 Pillar 3 etc.)
      - ``parameter='lgd'`` -> ``realised_loss_rate``
      - everything else     -> ``basel_pd_one_year`` (placeholder; the
        migration script re-classifies via inference)
    """
    insp = inspect(engine)
    if "raw_observations" not in insp.get_table_names():
        return
    cols = {c["name"] for c in insp.get_columns("raw_observations")}
    if "data_definition_class" in cols:
        return
    with engine.begin() as conn:
        conn.execute(text(
            "ALTER TABLE raw_observations ADD COLUMN "
            "data_definition_class TEXT NOT NULL DEFAULT 'basel_pd_one_year'"
        ))
        conn.execute(text(
            "UPDATE raw_observations SET data_definition_class='realised_loss_rate' "
            "WHERE parameter='lgd'"
        ))


def make_session_factory(engine: Engine) -> sessionmaker:
    """Return a sessionmaker bound to `engine` — one per process is enough."""
    return sessionmaker(bind=engine, expire_on_commit=False, future=True)
