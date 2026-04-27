"""One-shot migration: BenchmarkEntry rows -> RawObservation rows.

Usage:
    python scripts/migrate_to_raw_observations.py [--db PATH]

Reads the existing `benchmarks` table (legacy BenchmarkEntry shape) and
inserts a parallel `raw_observations` row for every PD or LGD entry.
The raw values are taken as-published (legacy entries already store the
raw figure — adjustment was never persisted to this table).

Idempotent: rows already present in `raw_observations` for the same
(source_id, segment, parameter, as_of_date) are skipped.

Source-type mapping from legacy SourceType -> raw-only SourceType:
    PILLAR3        -> BANK_PILLAR3 if source_id is one of {cba,nab,wbc,anz};
                      otherwise NON_BANK_LISTED
    APRA_ADI       -> APRA_PERFORMANCE
    LISTED_PEER    -> NON_BANK_LISTED
    INSOLVENCY     -> ASIC_INSOLVENCY
    INDUSTRY_BODY  -> ABS_BUSINESS_COUNTS
    RATING_AGENCY  -> RATING_AGENCY_INDEX
    RBA            -> RBA_AGGREGATE
    BUREAU         -> NON_BANK_LISTED   (placeholder; bureau adapters live elsewhere)
    ICC_TRADE      -> NON_BANK_LISTED   (placeholder)
    REGULATORY     -> APRA_PERFORMANCE  (placeholder)
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path
from typing import Optional

from sqlalchemy import select

from src.db import (
    Benchmark,
    RawObservationRow,
    create_engine_and_schema,
    make_session_factory,
)
from src.models import DataType, RawObservation, SourceType


logger = logging.getLogger(__name__)


_BIG4 = {"cba", "nab", "wbc", "anz"}

# Legacy source_type -> raw-only source_type. PILLAR3 is resolved per-row
# (BANK_PILLAR3 for Big 4, NON_BANK_LISTED otherwise).
_TYPE_MAP: dict[SourceType, SourceType] = {
    SourceType.APRA_ADI: SourceType.APRA_PERFORMANCE,
    SourceType.LISTED_PEER: SourceType.NON_BANK_LISTED,
    SourceType.INSOLVENCY: SourceType.ASIC_INSOLVENCY,
    SourceType.INDUSTRY_BODY: SourceType.ABS_BUSINESS_COUNTS,
    SourceType.RATING_AGENCY: SourceType.RATING_AGENCY_INDEX,
    SourceType.RBA: SourceType.RBA_AGGREGATE,
    SourceType.BUREAU: SourceType.NON_BANK_LISTED,
    SourceType.ICC_TRADE: SourceType.NON_BANK_LISTED,
    SourceType.REGULATORY: SourceType.APRA_PERFORMANCE,
}


def _map_source_type(legacy: SourceType, source_id: str) -> SourceType:
    if legacy == SourceType.PILLAR3:
        return (
            SourceType.BANK_PILLAR3
            if source_id.lower() in _BIG4
            else SourceType.NON_BANK_LISTED
        )
    return _TYPE_MAP.get(legacy, legacy)


def _map_parameter(data_type: str) -> Optional[str]:
    """Only PD / LGD migrate. Default-rate / impaired-ratio / etc. stay legacy-only."""
    if data_type == DataType.PD.value:
        return "pd"
    if data_type == DataType.LGD.value:
        return "lgd"
    return None


def _row_already_migrated(session, row: Benchmark, parameter: str) -> bool:
    existing = session.scalars(
        select(RawObservationRow).where(
            RawObservationRow.source_id == row.source_id,
            RawObservationRow.segment == row.asset_class,
            RawObservationRow.parameter == parameter,
            RawObservationRow.as_of_date == row.value_date,
        )
    ).first()
    return existing is not None


def migrate(db_path: str | Path) -> tuple[int, int, int]:
    """Run migration. Returns (scanned, migrated, skipped)."""
    engine = create_engine_and_schema(db_path)
    factory = make_session_factory(engine)

    scanned = migrated = skipped = 0
    with factory() as s:
        legacy_rows = s.scalars(
            select(Benchmark).where(Benchmark.superseded_by.is_(None))
        ).all()

        for row in legacy_rows:
            scanned += 1
            parameter = _map_parameter(row.data_type)
            if parameter is None:
                skipped += 1
                continue
            if _row_already_migrated(s, row, parameter):
                skipped += 1
                continue
            try:
                obs = RawObservation(
                    source_id=row.source_id,
                    source_type=_map_source_type(SourceType(row.source_type), row.source_id),
                    segment=row.asset_class,
                    product=None,
                    parameter=parameter,
                    value=row.value,
                    as_of_date=row.value_date,
                    reporting_basis=f"legacy BenchmarkEntry v{row.version}",
                    methodology_note=row.notes or f"migrated from {row.publisher}",
                    sample_size_n=None,
                    period_start=None,
                    period_end=row.value_date,
                    source_url=row.url,
                    page_or_table_ref=None,
                )
            except Exception as exc:  # pragma: no cover — logged for review
                logger.warning(
                    "skip %s/%s: validation failed (%s)",
                    row.source_id, row.data_type, exc,
                )
                skipped += 1
                continue

            from src.registry import _obs_to_row  # local import — avoid cycles
            s.add(_obs_to_row(obs))
            migrated += 1

        s.commit()

    return scanned, migrated, skipped


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--db", default="benchmarks.db",
        help="Path to SQLite DB (default: benchmarks.db)",
    )
    args = p.parse_args()

    db_path = Path(args.db)
    if db_path.name != ":memory:" and not db_path.exists():
        logger.error("DB file not found: %s", db_path)
        return 2

    scanned, migrated, skipped = migrate(str(db_path) if db_path.name != ":memory:" else ":memory:")
    logger.info("scanned=%d migrated=%d skipped=%d", scanned, migrated, skipped)
    logger.info("migration complete on %s", date.today().isoformat())
    return 0


if __name__ == "__main__":
    sys.exit(main())
