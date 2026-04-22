"""SQLite storage layer for the External Benchmark Engine.

Three tables — `benchmarks`, `adjustments`, `audit_log`.

Immutability contract (enforced by the registry layer, declared here):
- `benchmarks` content fields are write-once. Corrections insert a new
  row with `version = prior.version + 1`; the prior row's `superseded_by`
  is set to the new source_id. No other `UPDATE` to this table is allowed.
- `adjustments` and `audit_log` are append-only. What-if adjustments are
  in-memory only and must never reach `adjustments`.
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


class Adjustment(Base):
    """Append-only log of persisted adjustments. What-if results never land here."""
    __tablename__ = "adjustments"

    pk: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    institution_type: Mapped[str] = mapped_column(String, nullable=False, index=True)
    product: Mapped[str] = mapped_column(String, nullable=False)
    asset_class: Mapped[str] = mapped_column(String, nullable=False)

    raw_value: Mapped[float] = mapped_column(Float, nullable=False)
    adjusted_value: Mapped[float] = mapped_column(Float, nullable=False)

    # Serialised list[AdjustmentStep] — rehydrated by the registry layer.
    steps_json: Mapped[str] = mapped_column(String, nullable=False)
    applied_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)


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
    return engine


def make_session_factory(engine: Engine) -> sessionmaker:
    """Return a sessionmaker bound to `engine` — one per process is enough."""
    return sessionmaker(bind=engine, expire_on_commit=False, future=True)
