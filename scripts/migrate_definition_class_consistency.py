"""Re-classify APS 113 LGD rows from regulatory_floor_pd to regulatory_floor_lgd.

Background
----------
Pre-P2.2, the migration mapped APS 113 slotting / floor LGD rows through
``DataDefinitionClass.REGULATORY_FLOOR_PD`` because no LGD-specific
class existed. The Pydantic ``RawObservation`` validator now requires
PD parameters to pair with PD-class definitions and LGD parameters to
pair with LGD-class definitions; ``REGULATORY_FLOOR_LGD`` is the new
LGD-side class.

This script walks ``raw_observations`` and reassigns any
``parameter='lgd' AND data_definition_class='regulatory_floor_pd'`` row
to ``regulatory_floor_lgd``. Idempotent: running twice is a no-op.

Usage
-----
    python scripts/migrate_definition_class_consistency.py --db benchmarks.db
    python scripts/migrate_definition_class_consistency.py --db benchmarks.db --apply

Each affected row writes one ``audit_log`` entry.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from sqlalchemy import select

from src.db import (
    AuditLog,
    RawObservationRow,
    create_engine_and_schema,
    make_session_factory,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def migrate(db_path: str, *, apply: bool) -> tuple[int, int]:
    engine = create_engine_and_schema(db_path)
    factory = make_session_factory(engine)
    pending = 0
    updated = 0
    with factory() as session:
        rows = session.scalars(
            select(RawObservationRow).where(
                RawObservationRow.parameter == "lgd",
                RawObservationRow.data_definition_class == "regulatory_floor_pd",
            )
        ).all()
        pending = len(rows)
        if apply and rows:
            for row in rows:
                row.data_definition_class = "regulatory_floor_lgd"
                session.add(AuditLog(
                    operation="migrate_definition_class_to_floor_lgd",
                    entity_id=row.source_id,
                    params_json=json.dumps({
                        "segment": row.segment,
                        "as_of_date": row.as_of_date.isoformat(),
                        "prior_class": "regulatory_floor_pd",
                        "new_class": "regulatory_floor_lgd",
                    }),
                    result_summary="reclassified",
                    actor="migrate_definition_class_consistency",
                    timestamp=_utcnow(),
                ))
                updated += 1
            session.commit()
    return pending, updated


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    pending, updated = migrate(args.db, apply=args.apply)
    if not args.apply:
        print(f"DRY RUN — {pending} row(s) would be re-classified to regulatory_floor_lgd.")
    else:
        print(f"Re-classified {updated} row(s) to regulatory_floor_lgd.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
