"""One-shot migration: BenchmarkEntry rows -> RawObservation rows.

Usage:
    python scripts/migrate_to_raw_observations.py [--db PATH]

Reads the existing `benchmarks` table (legacy BenchmarkEntry shape) and
inserts a parallel `raw_observations` row for every PD, LGD, impaired,
NPL, arrears, loss-rate or qualitative-commentary entry.

Each migrated row carries an inferred `data_definition_class` so
downstream consumers can distinguish Basel PD from arrears from impaired
from qualitative commentary without having to grep methodology notes.

Idempotent: rows already present in `raw_observations` for the same
(source_id, segment, parameter, as_of_date) are skipped.

Source-type mapping from legacy SourceType -> raw-only SourceType:
    PILLAR3        -> BANK_PILLAR3 if source_id is one of {cba,nab,wbc,anz,mqg};
                      otherwise NON_BANK_LISTED
    APRA_ADI       -> APRA_PERFORMANCE
    LISTED_PEER    -> NON_BANK_LISTED
    RATING_AGENCY  -> RATING_AGENCY_INDEX
    RBA            -> RBA_AGGREGATE
    BUREAU         -> NON_BANK_LISTED   (placeholder; bureau adapters live elsewhere)
    REGULATORY     -> APRA_PERFORMANCE  (placeholder)
    INSOLVENCY     -> kept as INSOLVENCY (legacy passthrough)
    INDUSTRY_BODY  -> kept as INDUSTRY_BODY (legacy passthrough — covers AFIA etc.)

Definition-class inference: drives the new `data_definition_class` field
from the source_id pattern, with a fallback by `data_type` for legacy
rows that don't match any known publisher. See `_infer_definition_class`.
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path
from typing import Optional

# Make the project root importable when run as a script.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from sqlalchemy import select  # noqa: E402

from src.db import (  # noqa: E402
    Benchmark,
    RawObservationRow,
    create_engine_and_schema,
    make_session_factory,
)
from src.models import (  # noqa: E402
    DataDefinitionClass,
    DataType,
    RawObservation,
    SourceType,
)
from ingestion.external_indices.sp_spin_adapter import SpSpinAdapter  # noqa: E402


logger = logging.getLogger(__name__)


_BANK_PILLAR3 = {"cba", "nab", "wbc", "anz", "mqg", "macquarie"}

# Legacy source_type -> raw-only source_type. PILLAR3 is resolved per-row
# (BANK_PILLAR3 for Big 4, NON_BANK_LISTED otherwise).
_TYPE_MAP: dict[SourceType, SourceType] = {
    SourceType.APRA_ADI: SourceType.APRA_PERFORMANCE,
    SourceType.LISTED_PEER: SourceType.NON_BANK_LISTED,
    SourceType.RATING_AGENCY: SourceType.RATING_AGENCY_INDEX,
    SourceType.RBA: SourceType.RBA_AGGREGATE,
    SourceType.BUREAU: SourceType.NON_BANK_LISTED,
    SourceType.REGULATORY: SourceType.APRA_PERFORMANCE,
    # INSOLVENCY and INDUSTRY_BODY pass through unchanged (legacy enums
    # retained — they cover non-ABS/ASIC sources too, e.g. AFIA).
}


def _map_source_type(legacy: SourceType, source_id: str) -> SourceType:
    if legacy == SourceType.PILLAR3:
        sid = source_id.lower()
        head = sid.replace("-", "_").split("_", 1)[0]
        if sid in _BANK_PILLAR3 or head in _BANK_PILLAR3 or sid.startswith("macquarie_bank_"):
            return SourceType.BANK_PILLAR3
        return SourceType.NON_BANK_LISTED
    return _TYPE_MAP.get(legacy, legacy)


def _infer_definition_class(
    source_id: str, data_type: str
) -> Optional[DataDefinitionClass]:
    """Best-effort source_id -> DataDefinitionClass mapping.

    Returns None for rows that don't match any reality-check pattern and
    aren't a plain PD/LGD/IMPAIRED/DEFAULT_RATE — those are skipped by
    the migration as outside the raw-observation contract.
    """
    sid = source_id.upper()

    # Qualitative commentary takes priority — value is 0.0 by convention.
    if "COMMENTARY" in sid and ("QUALITAS" in sid or "METRICS" in sid):
        return DataDefinitionClass.QUALITATIVE_COMMENTARY

    # APS 113 — regulatory floor (covers PD and LGD slotting + floors).
    if "APS113" in sid:
        return DataDefinitionClass.REGULATORY_FLOOR_PD

    # APRA QPEX — system-wide impaired ratio.
    if "QPEX" in sid:
        return DataDefinitionClass.IMPAIRED_LOANS_RATIO

    # APRA quarterly ADI performance — NPL.
    if "APRA_PERF" in sid:
        return DataDefinitionClass.NPL_RATIO

    # RBA FSR / Securitisation aggregates — arrears (30+ if name says so).
    if "RBA_FSR" in sid or "SECURITISATION" in sid:
        if "30PLUS" in sid:
            return DataDefinitionClass.ARREARS_30_PLUS_DAYS
        return DataDefinitionClass.ARREARS_90_PLUS_DAYS

    # Rating-agency RMBS arrears indices — published as 30+ DPD.
    if "SPIN" in sid or "DINKUM" in sid or "MOODYS_AU_RMBS" in sid:
        return DataDefinitionClass.ARREARS_30_PLUS_DAYS

    # La Trobe realised loss disclosure.
    if "LATROBE" in sid and "REALISED_LOSS" in sid:
        return DataDefinitionClass.REALISED_LOSS_RATE

    # Big 4 + Judo Pillar 3 — Basel PD when PD; closest LGD class otherwise.
    if "PILLAR3" in sid:
        if data_type == DataType.PD.value:
            return DataDefinitionClass.BASEL_PD_ONE_YEAR
        if data_type == DataType.LGD.value:
            # No dedicated Basel-LGD class yet; REALISED_LOSS_RATE is the
            # closest available class for migrated Pillar 3 LGD rows.
            return DataDefinitionClass.REALISED_LOSS_RATE

    # Default fallback by data_type (legacy generic rows).
    if data_type == DataType.PD.value:
        return DataDefinitionClass.BASEL_PD_ONE_YEAR
    if data_type == DataType.LGD.value:
        return DataDefinitionClass.REALISED_LOSS_RATE
    if data_type == DataType.IMPAIRED_RATIO.value:
        return DataDefinitionClass.IMPAIRED_LOANS_RATIO
    if data_type == DataType.DEFAULT_RATE.value:
        return DataDefinitionClass.ARREARS_90_PLUS_DAYS

    return None


def _infer_parameter(
    definition_class: DataDefinitionClass, source_id: str, data_type: str
) -> str:
    """Map definition class -> parameter category.

    REGULATORY_FLOOR_PD is special: it covers both PD floors and LGD
    floors (APS 113 has both). Decide by source_id / data_type.
    """
    if definition_class is DataDefinitionClass.REGULATORY_FLOOR_PD:
        sid = source_id.upper()
        if data_type == DataType.LGD.value or "_LGD" in sid:
            return "lgd"
        return "pd"

    return {
        DataDefinitionClass.BASEL_PD_ONE_YEAR: "pd",
        DataDefinitionClass.ARREARS_30_PLUS_DAYS: "arrears",
        DataDefinitionClass.ARREARS_90_PLUS_DAYS: "arrears",
        DataDefinitionClass.IMPAIRED_LOANS_RATIO: "impaired",
        DataDefinitionClass.NPL_RATIO: "npl",
        DataDefinitionClass.LOSS_EXPENSE_RATE: "loss_rate",
        DataDefinitionClass.REALISED_LOSS_RATE: "lgd"
            if data_type == DataType.LGD.value
            else "loss_rate",
        DataDefinitionClass.QUALITATIVE_COMMENTARY: "commentary",
    }[definition_class]


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


def _raw_observation_already_present(session, obs: RawObservation) -> bool:
    existing = session.scalars(
        select(RawObservationRow).where(
            RawObservationRow.source_id == obs.source_id,
            RawObservationRow.segment == obs.segment,
            RawObservationRow.parameter == obs.parameter,
            RawObservationRow.as_of_date == obs.as_of_date,
        )
    ).first()
    return existing is not None


def _migrate_staged_spin_pdfs(session) -> int:
    """Parse manually staged S&P SPIN PDFs into raw_observations.

    The downloader deliberately does not fetch SPIN automatically because
    each release URL is generated. Analysts stage PDFs under
    data/raw/external_indices/sp_spin; this hook makes the standard
    migration command pick them up idempotently.
    """
    spin_dir = _REPO_ROOT / "data" / "raw" / "external_indices" / "sp_spin"
    if not spin_dir.exists():
        return 0

    adapter = SpSpinAdapter()
    inserted = 0
    for pdf_path in sorted(spin_dir.glob("*.pdf")):
        df = adapter.normalise(pdf_path)
        if df.empty:
            continue
        for record in df.to_dict(orient="records"):
            try:
                obs = RawObservation(**record)
            except Exception as exc:  # pragma: no cover - logged for analyst review
                logger.warning("skip SPIN row from %s: %s", pdf_path.name, exc)
                continue
            if _raw_observation_already_present(session, obs):
                continue
            from src.registry import _obs_to_row  # local import — avoid cycles
            session.add(_obs_to_row(obs))
            inserted += 1
    return inserted


def migrate(db_path: str | Path) -> tuple[int, int, int]:
    """Run migration. Returns (scanned, migrated, skipped)."""
    engine = create_engine_and_schema(db_path)
    factory = make_session_factory(engine)

    scanned = migrated = skipped = 0
    with factory() as s:
        migrated += _migrate_staged_spin_pdfs(s)

        legacy_rows = s.scalars(
            select(Benchmark).where(Benchmark.superseded_by.is_(None))
        ).all()

        for row in legacy_rows:
            scanned += 1

            definition_class = _infer_definition_class(row.source_id, row.data_type)
            if definition_class is None:
                skipped += 1
                continue
            parameter = _infer_parameter(
                definition_class, row.source_id, row.data_type
            )

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
                    data_definition_class=definition_class,
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
