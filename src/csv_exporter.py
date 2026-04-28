"""CSV exporters — one row per observation / band / file, no aggregation.

Downstream consumers (PD project, LGD project, dashboard tools) read these
CSVs as the engine's machine-readable contract. They mirror exactly what
the Markdown / HTML / DOCX report renders, with no editorial layer.

Four CSVs are produced into ``outputs/csv/`` (or any directory the caller
chooses):

* ``raw_observations.csv``      — every row in the ``raw_observations`` table.
                                   One row per observation. Includes
                                   ``data_definition_class`` so consumers
                                   can filter by definition family.
* ``validation_flags.csv``      — per-segment cross-source flags (spread,
                                   outliers, vintage, Big-4-vs-non-bank).
* ``reality_check_bands.csv``   — per-product upper / lower band table
                                   flattened from
                                   ``config/reality_check_bands.yaml``.
* ``raw_data_inventory.csv``    — every file currently staged in
                                   ``data/raw/`` with size and modified
                                   timestamp. Lets dashboards show "what
                                   raw publications are on disk?".

Each function returns the ``Path`` of the CSV it wrote, so callers can
build manifests / report-summaries from the return values.
"""
from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from src.observations import PeerObservations
from src.reality_check import RealityCheckBandLibrary, load_reality_check_bands
from src.registry import BenchmarkRegistry
from src.validation import is_big4_source_id


DEFAULT_OUTPUT_DIR = Path("outputs/csv")
DEFAULT_RAW_DATA_DIR = Path("data/raw")


# ---------------------------------------------------------------------------
# Raw observations
# ---------------------------------------------------------------------------

_RAW_OBS_COLUMNS = [
    "source_id",
    "source_type",
    "is_big4",
    "segment",
    "product",
    "parameter",
    "data_definition_class",
    "value",
    "as_of_date",
    "reporting_basis",
    "methodology_note",
    "sample_size_n",
    "period_start",
    "period_end",
    "source_url",
    "page_or_table_ref",
]


def export_raw_observations(
    registry: BenchmarkRegistry,
    out_dir: Path = DEFAULT_OUTPUT_DIR,
) -> Path:
    """Export every raw observation as a single flat CSV."""
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "raw_observations.csv"
    rows = registry.query_observations()
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=_RAW_OBS_COLUMNS)
        w.writeheader()
        for o in rows:
            w.writerow({
                "source_id": o.source_id,
                "source_type": o.source_type.value,
                "is_big4": "true" if is_big4_source_id(o.source_id) else "false",
                "segment": o.segment,
                "product": o.product or "",
                "parameter": o.parameter,
                "data_definition_class": o.data_definition_class.value,
                "value": f"{o.value:.6f}",
                "as_of_date": o.as_of_date.isoformat(),
                "reporting_basis": o.reporting_basis,
                "methodology_note": o.methodology_note,
                "sample_size_n": o.sample_size_n if o.sample_size_n is not None else "",
                "period_start": o.period_start.isoformat() if o.period_start else "",
                "period_end": o.period_end.isoformat() if o.period_end else "",
                "source_url": o.source_url or "",
                "page_or_table_ref": o.page_or_table_ref or "",
            })
    return path


# ---------------------------------------------------------------------------
# Validation flags (per-segment)
# ---------------------------------------------------------------------------

_VALIDATION_COLUMNS = [
    "segment",
    "n_sources",
    "spread_pct",
    "big4_spread_pct",
    "bank_vs_nonbank_ratio",
    "outlier_sources",
    "stale_sources",
]


def export_validation_flags(
    registry: BenchmarkRegistry,
    out_dir: Path = DEFAULT_OUTPUT_DIR,
) -> Path:
    """Export per-segment validation flags as CSV."""
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "validation_flags.csv"
    peer = PeerObservations(registry)
    segments = peer.all_segments()
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=_VALIDATION_COLUMNS)
        w.writeheader()
        for seg in segments:
            obs_set = peer.for_segment(seg, only_pd=False)
            f = obs_set.validation_flags
            w.writerow({
                "segment": seg,
                "n_sources": f.n_sources,
                "spread_pct": _fmt(f.spread_pct),
                "big4_spread_pct": _fmt(f.big4_spread_pct),
                "bank_vs_nonbank_ratio": _fmt(f.bank_vs_nonbank_ratio),
                "outlier_sources": "|".join(f.outlier_sources),
                "stale_sources": "|".join(f.stale_sources),
            })
    return path


def _fmt(v: float | None) -> str:
    return "" if v is None else f"{v:.4f}"


# ---------------------------------------------------------------------------
# Reality-check bands (per-product)
# ---------------------------------------------------------------------------

_BAND_COLUMNS = [
    "product",
    "lower_band_pd",
    "upper_band_pd",
    "lower_sources",
    "upper_sources",
    "rationale",
    "last_review_date",
    "next_review_due",
]


def export_reality_check_bands(
    library: RealityCheckBandLibrary | None = None,
    out_dir: Path = DEFAULT_OUTPUT_DIR,
) -> Path:
    """Export per-product reality-check bands as CSV.

    Each band's `rationale` is multi-line markdown — flattened to one
    line with literal `\\n` separators so spreadsheet tools stay happy.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "reality_check_bands.csv"
    lib = library or load_reality_check_bands()
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=_BAND_COLUMNS)
        w.writeheader()
        for product in lib.all_products():
            band = lib.for_product(product)
            assert band is not None
            w.writerow({
                "product": product,
                "lower_band_pd": f"{band.lower_band_pd:.6f}",
                "upper_band_pd": f"{band.upper_band_pd:.6f}",
                "lower_sources": "|".join(band.lower_sources),
                "upper_sources": "|".join(band.upper_sources),
                "rationale": band.rationale.strip().replace("\n", "\\n"),
                "last_review_date": lib.last_review_date,
                "next_review_due": lib.next_review_due,
            })
    return path


# ---------------------------------------------------------------------------
# Raw-data inventory (file manifest)
# ---------------------------------------------------------------------------

_INVENTORY_COLUMNS = [
    "source_family",
    "subfamily",
    "filename",
    "relative_path",
    "size_bytes",
    "modified_utc",
    "kind",
]

# How to classify files by extension. Anything not listed shows up as "other".
_KIND_BY_SUFFIX: dict[str, str] = {
    ".pdf": "pdf",
    ".xlsx": "xlsx",
    ".xls": "xls",
    ".csv": "csv",
    ".html": "html_snapshot",
    ".md": "manual_note",
    ".txt": "text",
    ".zip": "archive",
}


def export_raw_data_inventory(
    raw_dir: Path = DEFAULT_RAW_DATA_DIR,
    out_dir: Path = DEFAULT_OUTPUT_DIR,
) -> Path:
    """Walk ``data/raw/`` and record every file as one CSV row.

    Includes ``_MANUAL.md`` and ``*_GATE.md`` notes alongside real PDFs /
    XLSXs so dashboards can show "fetched", "manual", and "gated" cleanly.
    Skips ``.gitkeep``, hidden files, and ``__pycache__``.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "raw_data_inventory.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=_INVENTORY_COLUMNS)
        w.writeheader()
        if not raw_dir.exists():
            return path
        for fp in sorted(_walk_raw_files(raw_dir)):
            rel = fp.relative_to(raw_dir)
            parts = rel.parts
            # parts always ends with the filename, so subfamily exists only
            # when the path has >= 3 components (family / subfamily / file).
            family = parts[0] if len(parts) > 0 else ""
            subfamily = parts[1] if len(parts) >= 3 else ""
            stat = fp.stat()
            kind = _classify_file(fp)
            w.writerow({
                "source_family": family,
                "subfamily": subfamily,
                "filename": fp.name,
                "relative_path": str(rel).replace("\\", "/"),
                "size_bytes": stat.st_size,
                "modified_utc": datetime.fromtimestamp(
                    stat.st_mtime, tz=timezone.utc
                ).isoformat(timespec="seconds"),
                "kind": kind,
            })
    return path


def _walk_raw_files(root: Path) -> Iterable[Path]:
    """Yield every regular file under root, ignoring noise files / dirs."""
    skip_names = {".gitkeep", ".DS_Store", "Thumbs.db"}
    for fp in root.rglob("*"):
        if not fp.is_file():
            continue
        if fp.name in skip_names or fp.name.startswith("."):
            continue
        if "__pycache__" in fp.parts:
            continue
        yield fp


def _classify_file(fp: Path) -> str:
    suffix = fp.suffix.lower()
    if fp.name.endswith("_MANUAL.md") or "GATE.md" in fp.name:
        return "manual_note"
    return _KIND_BY_SUFFIX.get(suffix, "other")


# ---------------------------------------------------------------------------
# Convenience: emit all four CSVs in one call
# ---------------------------------------------------------------------------

def export_all_csvs(
    registry: BenchmarkRegistry,
    *,
    out_dir: Path = DEFAULT_OUTPUT_DIR,
    raw_dir: Path = DEFAULT_RAW_DATA_DIR,
    library: RealityCheckBandLibrary | None = None,
) -> dict[str, Path]:
    """Emit all four CSVs; return a dict keyed by short name."""
    return {
        "raw_observations": export_raw_observations(registry, out_dir),
        "validation_flags": export_validation_flags(registry, out_dir),
        "reality_check_bands": export_reality_check_bands(library, out_dir),
        "raw_data_inventory": export_raw_data_inventory(raw_dir, out_dir),
    }
