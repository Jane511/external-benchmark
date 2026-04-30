"""Collapse legacy commercial_property_investment rows into the canonical CRE segment.

Usage:
    python scripts/migrate_collapse_cre_segments.py --db benchmarks.db
    python scripts/migrate_collapse_cre_segments.py --db benchmarks.db --apply
"""
from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timezone

from sqlalchemy import select

from src.db import AuditLog, RawObservationRow, create_engine_and_schema, make_session_factory


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def migrate(db_path: str, *, apply: bool) -> tuple[int, int]:
    engine = create_engine_and_schema(db_path)
    factory = make_session_factory(engine)
    deleted = 0
    kept = 0
    with factory() as session:
        rows = session.scalars(
            select(RawObservationRow).where(
                RawObservationRow.segment == "commercial_property_investment"
            )
        ).all()
        for row in rows:
            dup = session.scalars(
                select(RawObservationRow).where(
                    RawObservationRow.source_id == row.source_id,
                    RawObservationRow.parameter == row.parameter,
                    RawObservationRow.as_of_date == row.as_of_date,
                    RawObservationRow.segment == "commercial_property",
                )
            ).first()
            if dup is None:
                kept += 1
                continue
            deleted += 1
            session.add(
                AuditLog(
                    operation="collapse_cre_segment",
                    entity_id=row.source_id,
                    params_json=json.dumps(
                        {
                            "from_segment": row.segment,
                            "to_segment": "commercial_property",
                            "as_of_date": row.as_of_date.isoformat(),
                            "apply": apply,
                        }
                    ),
                    result_summary="deleted duplicate legacy CRE row" if apply else "would delete",
                    actor="migration",
                    timestamp=_utcnow(),
                )
            )
            if apply:
                session.delete(row)
        session.commit()
    return deleted, kept


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="benchmarks.db")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    deleted, kept = migrate(args.db, apply=args.apply)
    mode = "applied" if args.apply else "dry-run"
    print(f"{mode}: duplicate_deleted={deleted} unmatched_legacy_rows={kept}")
    print(f"completed={date.today().isoformat()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
