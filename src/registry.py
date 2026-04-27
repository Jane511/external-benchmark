"""BenchmarkRegistry — immutable storage with version supersession and audit trail.

The registry is the only layer permitted to write to the `benchmarks` table.
Content fields are never updated in place; corrections go through `supersede()`,
which atomically inserts a new version and points the prior row at its successor.

Every public method writes exactly one row to `audit_log`, including reads —
external benchmarks feed PD/LGD calibration under APRA/APG 113 and model-risk
governance typically requires full access provenance, not just write provenance.
"""
from __future__ import annotations

import json
from datetime import date
from typing import Any, Literal, Optional

import pandas as pd
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from src.db import AuditLog, Benchmark, RawObservationRow, make_session_factory
from src.models import (
    BenchmarkEntry,
    Component,
    Condition,
    DataType,
    QualityScore,
    RawObservation,
    SourceType,
)


def _entry_to_row(entry: BenchmarkEntry) -> Benchmark:
    """Convert a validated Pydantic entry into a SQLAlchemy row."""
    return Benchmark(
        source_id=entry.source_id,
        version=entry.version,
        publisher=entry.publisher,
        source_type=entry.source_type.value,
        data_type=entry.data_type.value,
        asset_class=entry.asset_class,
        geography=entry.geography,
        url=entry.url,
        notes=entry.notes,
        value=entry.value,
        value_date=entry.value_date,
        period_years=entry.period_years,
        condition=entry.condition.value if entry.condition else None,
        component=entry.component.value if entry.component else None,
        retrieval_date=entry.retrieval_date,
        quality_score=entry.quality_score.value,
        superseded_by=entry.superseded_by,
    )


def _row_to_entry(row: Benchmark) -> BenchmarkEntry:
    """Rehydrate a Pydantic entry from a SQLAlchemy row (runs validators)."""
    return BenchmarkEntry(
        source_id=row.source_id,
        version=row.version,
        publisher=row.publisher,
        source_type=SourceType(row.source_type),
        data_type=DataType(row.data_type),
        asset_class=row.asset_class,
        geography=row.geography,
        url=row.url,
        notes=row.notes or "",
        value=row.value,
        value_date=row.value_date,
        period_years=row.period_years,
        condition=Condition(row.condition) if row.condition else None,
        component=Component(row.component) if row.component else None,
        retrieval_date=row.retrieval_date,
        quality_score=QualityScore(row.quality_score),
        superseded_by=row.superseded_by,
    )


class BenchmarkRegistry:
    """Wraps db.py with versioning, audit logging, and segment queries."""

    def __init__(self, engine: Engine, actor: str = "system") -> None:
        self._engine = engine
        self._factory = make_session_factory(engine)
        self._actor = actor

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _audit(
        self,
        session: Session,
        operation: str,
        entity_id: str,
        params: dict[str, Any],
        summary: str,
    ) -> None:
        session.add(
            AuditLog(
                operation=operation,
                entity_id=entity_id,
                params_json=json.dumps(params, default=str),
                result_summary=summary,
                actor=self._actor,
            )
        )

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def add(self, entry: BenchmarkEntry) -> None:
        """Insert a new benchmark. Raises IntegrityError on duplicate (source_id, version)."""
        with self._factory() as s:
            s.add(_entry_to_row(entry))
            self._audit(
                s, "add", entry.source_id,
                {"version": entry.version, "data_type": entry.data_type.value},
                "inserted",
            )
            s.commit()

    def supersede(self, source_id: str, new_entry: BenchmarkEntry) -> BenchmarkEntry:
        """Atomically insert new_entry as version prior.version+1 and point prior at successor.

        Both operations happen in a single transaction — if either fails, neither
        persists. `new_entry.source_id` must match `source_id`. `new_entry.version`
        on input is ignored; the registry computes prior.version+1.
        """
        if new_entry.source_id != source_id:
            raise ValueError(
                f"new_entry.source_id={new_entry.source_id!r} does not match "
                f"source_id={source_id!r}"
            )
        with self._factory() as s:
            prior = s.scalars(
                select(Benchmark).where(
                    Benchmark.source_id == source_id,
                    Benchmark.superseded_by.is_(None),
                )
            ).one_or_none()
            if prior is None:
                raise ValueError(
                    f"No active benchmark with source_id={source_id!r} to supersede"
                )

            new_row = _entry_to_row(new_entry)
            new_row.version = prior.version + 1
            new_row.superseded_by = None

            prior.superseded_by = source_id  # pointer-only update on prior row

            s.add(new_row)
            self._audit(
                s, "supersede", source_id,
                {"from_version": prior.version, "to_version": new_row.version},
                f"v{prior.version} -> v{new_row.version}",
            )
            s.commit()
            s.refresh(new_row)
            return _row_to_entry(new_row)

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def list(self, *, latest_only: bool = True) -> list[BenchmarkEntry]:
        """Return latest-version rows by default; pass latest_only=False for full history."""
        with self._factory() as s:
            stmt = select(Benchmark)
            if latest_only:
                stmt = stmt.where(Benchmark.superseded_by.is_(None))
            rows = s.scalars(
                stmt.order_by(Benchmark.source_id, Benchmark.version)
            ).all()
            self._audit(
                s, "list", "*",
                {"latest_only": latest_only}, f"{len(rows)} rows",
            )
            s.commit()
            return [_row_to_entry(r) for r in rows]

    def get_by_source_type(self, source_type: SourceType) -> list[BenchmarkEntry]:
        """Latest-version rows with matching source_type."""
        with self._factory() as s:
            rows = s.scalars(
                select(Benchmark).where(
                    Benchmark.source_type == source_type.value,
                    Benchmark.superseded_by.is_(None),
                ).order_by(Benchmark.source_id)
            ).all()
            self._audit(
                s, "get_by_source_type", source_type.value,
                {}, f"{len(rows)} rows",
            )
            s.commit()
            return [_row_to_entry(r) for r in rows]

    def get_by_segment(
        self,
        asset_class: str,
        data_type: DataType,
        condition: Optional[Condition] = None,
        component: Optional[Component] = None,
    ) -> list[BenchmarkEntry]:
        """Latest-version rows for a segment, filtered optionally by LGD decomposition.

        Filtering:
          - asset_class + data_type always applied
          - condition / component only applied when explicitly passed (None = no filter)
          - always excludes superseded rows
        """
        with self._factory() as s:
            stmt = select(Benchmark).where(
                Benchmark.asset_class == asset_class,
                Benchmark.data_type == data_type.value,
                Benchmark.superseded_by.is_(None),
            )
            if condition is not None:
                stmt = stmt.where(Benchmark.condition == condition.value)
            if component is not None:
                stmt = stmt.where(Benchmark.component == component.value)
            rows = s.scalars(stmt.order_by(Benchmark.source_id)).all()

            entity = f"{asset_class}/{data_type.value}"
            params: dict[str, Any] = {
                "condition": condition.value if condition else None,
                "component": component.value if component else None,
            }
            self._audit(
                s, "get_by_segment", entity, params, f"{len(rows)} rows",
            )
            s.commit()
            return [_row_to_entry(r) for r in rows]

    def get_version_history(self, source_id: str) -> list[BenchmarkEntry]:
        """All versions (active + superseded) in ascending version order.

        Used by the annual-review drift narrative in governance.py.
        """
        with self._factory() as s:
            rows = s.scalars(
                select(Benchmark)
                .where(Benchmark.source_id == source_id)
                .order_by(Benchmark.version.asc())
            ).all()
            self._audit(
                s, "get_version_history", source_id,
                {}, f"{len(rows)} versions",
            )
            s.commit()
            return [_row_to_entry(r) for r in rows]

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------

    def export(
        self,
        format: Literal["json", "csv"] = "json",
        *,
        latest_only: bool = True,
    ) -> str:
        """Serialise latest (or full-history) rows as JSON or CSV via pandas."""
        with self._factory() as s:
            stmt = select(Benchmark)
            if latest_only:
                stmt = stmt.where(Benchmark.superseded_by.is_(None))
            rows = s.scalars(stmt.order_by(Benchmark.source_id, Benchmark.version)).all()

            records = [_row_to_dict(r) for r in rows]
            self._audit(
                s, "export", "*",
                {"format": format, "latest_only": latest_only, "count": len(records)},
                f"{len(records)} rows exported as {format}",
            )
            s.commit()

        df = pd.DataFrame(records)
        if format == "json":
            return df.to_json(orient="records", date_format="iso")
        if format == "csv":
            return df.to_csv(index=False)
        raise ValueError(f"Unknown format: {format!r}")


# ---------------------------------------------------------------------------
# RawObservation helpers (Brief 1)
# ---------------------------------------------------------------------------

def _obs_to_row(obs: RawObservation) -> RawObservationRow:
    return RawObservationRow(
        source_id=obs.source_id,
        source_type=obs.source_type.value,
        segment=obs.segment,
        product=obs.product,
        parameter=obs.parameter,
        value=obs.value,
        as_of_date=obs.as_of_date,
        reporting_basis=obs.reporting_basis,
        methodology_note=obs.methodology_note,
        sample_size_n=obs.sample_size_n,
        period_start=obs.period_start,
        period_end=obs.period_end,
        source_url=obs.source_url,
        page_or_table_ref=obs.page_or_table_ref,
    )


def _row_to_obs(row: RawObservationRow) -> RawObservation:
    return RawObservation(
        source_id=row.source_id,
        source_type=SourceType(row.source_type),
        segment=row.segment,
        product=row.product,
        parameter=row.parameter,
        value=row.value,
        as_of_date=row.as_of_date,
        reporting_basis=row.reporting_basis,
        methodology_note=row.methodology_note,
        sample_size_n=row.sample_size_n,
        period_start=row.period_start,
        period_end=row.period_end,
        source_url=row.source_url,
        page_or_table_ref=row.page_or_table_ref,
    )


# Patch the BenchmarkRegistry class with raw-observation methods.
def _add_observation(self: "BenchmarkRegistry", obs: RawObservation) -> None:
    """Insert a single RawObservation. Append-only — no version supersession."""
    with self._factory() as s:
        s.add(_obs_to_row(obs))
        self._audit(
            s, "add_observation", obs.source_id,
            {"segment": obs.segment, "parameter": obs.parameter,
             "as_of_date": obs.as_of_date.isoformat()},
            "inserted",
        )
        s.commit()


def _add_observations(self: "BenchmarkRegistry", obs_list: list[RawObservation]) -> int:
    """Bulk insert. Returns count inserted."""
    with self._factory() as s:
        for obs in obs_list:
            s.add(_obs_to_row(obs))
        self._audit(
            s, "add_observations", "*",
            {"count": len(obs_list)}, f"{len(obs_list)} inserted",
        )
        s.commit()
        return len(obs_list)


def _query_observations(
    self: "BenchmarkRegistry",
    *,
    segment: Optional[str] = None,
    product: Optional[str] = None,
    source_type: Optional[SourceType] = None,
    parameter: Optional[str] = None,
    since: Optional[date] = None,
) -> list[RawObservation]:
    """Filter raw observations. Latest-vintage filtering is the consumer's job."""
    with self._factory() as s:
        stmt = select(RawObservationRow)
        if segment is not None:
            stmt = stmt.where(RawObservationRow.segment == segment)
        if product is not None:
            stmt = stmt.where(RawObservationRow.product == product)
        if source_type is not None:
            stmt = stmt.where(RawObservationRow.source_type == source_type.value)
        if parameter is not None:
            stmt = stmt.where(RawObservationRow.parameter == parameter)
        if since is not None:
            stmt = stmt.where(RawObservationRow.as_of_date >= since)
        rows = s.scalars(
            stmt.order_by(RawObservationRow.segment, RawObservationRow.source_id,
                          RawObservationRow.as_of_date.desc())
        ).all()
        self._audit(
            s, "query_observations",
            f"{segment or '*'}/{parameter or '*'}",
            {
                "segment": segment, "product": product,
                "source_type": source_type.value if source_type else None,
                "parameter": parameter,
                "since": since.isoformat() if since else None,
            },
            f"{len(rows)} rows",
        )
        s.commit()
        return [_row_to_obs(r) for r in rows]


def _list_segments(self: "BenchmarkRegistry") -> list[str]:
    """Distinct canonical segment IDs that have at least one observation."""
    with self._factory() as s:
        stmt = select(RawObservationRow.segment).distinct().order_by(
            RawObservationRow.segment
        )
        segments = [row for row in s.scalars(stmt).all()]
        self._audit(s, "list_segments", "*", {}, f"{len(segments)} segments")
        s.commit()
        return segments


# Bind helpers as methods on BenchmarkRegistry without rewriting the class body.
BenchmarkRegistry.add_observation = _add_observation       # type: ignore[attr-defined]
BenchmarkRegistry.add_observations = _add_observations     # type: ignore[attr-defined]
BenchmarkRegistry.query_observations = _query_observations  # type: ignore[attr-defined]
BenchmarkRegistry.list_segments = _list_segments           # type: ignore[attr-defined]


def _row_to_dict(row: Benchmark) -> dict[str, Any]:
    """Flat serialisable dict for export; keeps enum values as strings."""
    return {
        "source_id": row.source_id,
        "version": row.version,
        "publisher": row.publisher,
        "source_type": row.source_type,
        "data_type": row.data_type,
        "asset_class": row.asset_class,
        "value": row.value,
        "value_date": row.value_date.isoformat() if row.value_date else None,
        "period_years": row.period_years,
        "geography": row.geography,
        "url": row.url,
        "condition": row.condition,
        "component": row.component,
        "retrieval_date": row.retrieval_date.isoformat() if row.retrieval_date else None,
        "quality_score": row.quality_score,
        "notes": row.notes,
        "superseded_by": row.superseded_by,
    }
