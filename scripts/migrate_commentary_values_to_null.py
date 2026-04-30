"""Migrate commentary rows in raw_observations from value=0.0 to value=None.

Background
----------
Pre-P1.1, qualitative commentary observations (Qualitas + Metrics) were
encoded with ``value = 0.0`` and ``parameter = 'commentary'``. The zero
leaked into every aggregate that didn't filter by parameter, producing
a 24% spread on commercial_property in ``validation_flags.csv`` while the
Markdown report (which filtered) reported 21% on the same data.

Post-P1.1, commentary rows store ``value = None`` and the Pydantic
``RawObservation`` validator rejects any other value when
``parameter == 'commentary'``. This script back-fills existing databases.

Usage
-----
    python scripts/migrate_commentary_values_to_null.py --db benchmarks.db

    # Dry-run (default):
    python scripts/migrate_commentary_values_to_null.py --db benchmarks.db

    # Actually apply:
    python scripts/migrate_commentary_values_to_null.py --db benchmarks.db --apply

Each affected row produces one ``audit_log`` entry (operation =
``migrate_commentary_value_to_null``).
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from sqlalchemy import select, update

from src.db import (
    AuditLog,
    RawObservationRow,
    create_engine_and_schema,
    make_session_factory,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def migrate(db_path: str, *, apply: bool) -> tuple[int, int]:
    """Returns (rows_to_update, rows_actually_updated)."""
    engine = create_engine_and_schema(db_path)
    factory = make_session_factory(engine)
    pending = 0
    updated = 0
    with factory() as session:
        rows = session.scalars(
            select(RawObservationRow).where(
                RawObservationRow.parameter == "commentary",
                RawObservationRow.value.is_not(None),
            )
        ).all()
        pending = len(rows)
        if apply and rows:
            for row in rows:
                prior_value = row.value
                row.value = None
                session.add(AuditLog(
                    operation="migrate_commentary_value_to_null",
                    entity_id=row.source_id,
                    params_json=json.dumps({
                        "segment": row.segment,
                        "as_of_date": row.as_of_date.isoformat(),
                        "prior_value": prior_value,
                    }),
                    result_summary="value set to NULL",
                    actor="migrate_commentary_values_to_null",
                    timestamp=_utcnow(),
                ))
                updated += 1
            session.commit()
    return pending, updated


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, help="Path to benchmarks.db")
    parser.add_argument("--apply", action="store_true",
                        help="Actually update rows (default: dry-run).")
    args = parser.parse_args()

    pending, updated = migrate(args.db, apply=args.apply)
    if not args.apply:
        print(f"DRY RUN — {pending} commentary row(s) would be set to value=NULL.")
    else:
        print(f"Updated {updated} commentary row(s) to value=NULL.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
