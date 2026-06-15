"""CSV exporters for private-credit model inputs.

The default bundle is deliberately small and model-ready:

* ``pd_inputs.csv``
* ``lgd_inputs.csv``
* ``expected_loss_inputs.csv``
* ``stress_testing_inputs.csv``
* ``portfolio_monitor_inputs.csv``

Legacy raw/audit CSV functions remain available in this module for
debugging, but ``export_all_csvs`` now emits only the five direct-input
files above.
"""
from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

from src.models import cohort_for
from src.model_inputs import build_model_input_bundle
from src.observations import PeerObservations
from src.reality_check import RealityCheckBandLibrary, load_reality_check_bands
from src.registry import BenchmarkRegistry
from src.trend import build_segment_trends
from src.validation import PEER_RATIO_DEFINITION, is_big4_source_id


DEFAULT_OUTPUT_DIR = Path("output/data")
DEFAULT_RAW_DATA_DIR = Path("data/raw")


_MODEL_INPUT_COLUMNS: dict[str, list[str]] = {
    "pd_inputs": [
        "segment",
        "segment_label",
        "product",
        "source_id",
        "source_type",
        "pd_decimal",
        "as_of_date",
    ],
    "lgd_inputs": [
        "segment",
        "segment_label",
        "product",
        "source_id",
        "source_type",
        "lgd_decimal",
        "as_of_date",
    ],
    "expected_loss_inputs": [
        "segment",
        "segment_label",
        "product",
        "pd_decimal",
        "lgd_decimal",
        "expected_loss_rate_decimal",
        "expected_loss_rate_bps",
        "pd_source_count",
        "lgd_source_count",
        "as_of_date",
    ],
    "stress_testing_inputs": [
        "segment",
        "segment_label",
        "product",
        "scenario",
        "scenario_label",
        "base_pd_decimal",
        "base_lgd_decimal",
        "base_expected_loss_rate_decimal",
        "pd_stress_multiplier",
        "lgd_stress_multiplier",
        "ccf_stress_multiplier",
        "pd_upper_band_decimal",
        "stressed_pd_decimal",
        "stressed_lgd_decimal",
        "stressed_expected_loss_rate_decimal",
        "stressed_el_rate_incl_ead_decimal",
        "macro_path",
        "as_of_date",
    ],
    "portfolio_monitor_inputs": [
        "segment",
        "segment_label",
        "product",
        "arrears_decimal",
        "arrears_source_count",
        "npl_decimal",
        "npl_source_count",
        "impaired_decimal",
        "impaired_source_count",
        "loss_rate_decimal",
        "loss_rate_source_count",
        "monitor_source_count",
        "as_of_date",
    ],
}


def export_model_input_csvs(
    registry: BenchmarkRegistry,
    *,
    out_dir: Path = DEFAULT_OUTPUT_DIR,
    library: RealityCheckBandLibrary | None = None,
) -> dict[str, Path]:
    """Emit the five direct input CSVs for credit models."""

    out_dir.mkdir(parents=True, exist_ok=True)
    bundle = build_model_input_bundle(registry, library=library)
    paths: dict[str, Path] = {}
    for name, rows in bundle.items():
        columns = _MODEL_INPUT_COLUMNS[name]
        path = out_dir / f"{name}.csv"
        _write_rows(path, columns, rows)
        paths[name] = path
    return paths


def _write_rows(
    path: Path,
    columns: list[str],
    rows: list[dict[str, object]],
) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


# ---------------------------------------------------------------------------
# Raw observations
# ---------------------------------------------------------------------------

_RAW_OBS_COLUMNS = [
    "source_id",
    "source_type",
    "cohort",
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
    """Export every raw observation as a single flat CSV.

    Commentary rows carry an empty ``value`` cell. ``cohort`` is the
    peer-grouping label used by validation arithmetic — see
    :class:`src.models.Cohort`.
    """
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
                "cohort": cohort_for(o.source_type, o.source_id).value,
                "is_big4": "true" if is_big4_source_id(o.source_id) else "false",
                "segment": o.segment,
                "product": o.product or "",
                "parameter": o.parameter,
                "data_definition_class": o.data_definition_class.value,
                "value": "" if o.value is None else f"{o.value:.6f}",
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
    "spread_decimal",
    "big4_spread_decimal",
    "peer_big4_vs_non_bank_ratio",
    "outlier_sources",
    "stale_sources",
    "frozen_dataset_banner",
]


_VALIDATION_UNITS_HEADER = (
    "# units: spread_decimal and big4_spread_decimal are 0-1 decimals "
    "(e.g. 0.21 = 21%); peer_big4_vs_non_bank_ratio is a unitless ratio. "
    "definition: " + PEER_RATIO_DEFINITION
)


def export_validation_flags(
    registry: BenchmarkRegistry,
    out_dir: Path = DEFAULT_OUTPUT_DIR,
    *,
    refresh_schedules: Optional[dict[str, int]] = None,
    refresh_pipeline_quiet: bool = False,
) -> Path:
    """Export per-segment validation flags as CSV.

    The first non-blank line is a ``# units:`` comment row that documents
    the decimal-vs-percent convention so spreadsheet importers can ignore
    it (`csv` reader treats lines starting with ``#`` as data, but
    Excel's "skip rows starting with" import option handles it). Source
    lists are deduped and stable-ordered.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "validation_flags.csv"
    peer = PeerObservations(
        registry,
        refresh_schedules=refresh_schedules,
        refresh_pipeline_quiet=refresh_pipeline_quiet,
    )
    segments = peer.all_segments()
    with path.open("w", newline="", encoding="utf-8") as fh:
        fh.write(_VALIDATION_UNITS_HEADER + "\n")
        w = csv.DictWriter(fh, fieldnames=_VALIDATION_COLUMNS)
        w.writeheader()
        for seg in segments:
            obs_set = peer.for_segment(seg, only_pd=False)
            f = obs_set.validation_flags
            w.writerow({
                "segment": seg,
                "n_sources": f.n_sources,
                "spread_decimal": _fmt(f.spread_pct),
                "big4_spread_decimal": _fmt(f.big4_spread_pct),
                "peer_big4_vs_non_bank_ratio": _fmt(f.peer_big4_vs_non_bank_ratio),
                "outlier_sources": "|".join(f.outlier_sources),
                "stale_sources": "|".join(f.stale_sources),
                "frozen_dataset_banner": f.frozen_dataset_banner or "",
            })
    return path


_VALIDATION_FLAG_SOURCES_COLUMNS = [
    "segment",
    "flag_type",
    "source_id",
]


def export_validation_flag_sources(
    registry: BenchmarkRegistry,
    out_dir: Path = DEFAULT_OUTPUT_DIR,
    *,
    refresh_schedules: Optional[dict[str, int]] = None,
    refresh_pipeline_quiet: bool = False,
) -> Path:
    """Long-form companion to ``validation_flags.csv``.

    Each row is one (segment, flag_type, source_id) tuple — easy to pivot
    in Excel without parsing pipe-delimited cells. ``flag_type`` is one of
    ``outlier`` or ``stale``.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "validation_flag_sources.csv"
    peer = PeerObservations(
        registry,
        refresh_schedules=refresh_schedules,
        refresh_pipeline_quiet=refresh_pipeline_quiet,
    )
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=_VALIDATION_FLAG_SOURCES_COLUMNS)
        w.writeheader()
        for seg in peer.all_segments():
            obs_set = peer.for_segment(seg, only_pd=False)
            f = obs_set.validation_flags
            for sid in f.outlier_sources:
                w.writerow({"segment": seg, "flag_type": "outlier", "source_id": sid})
            for sid in f.stale_sources:
                w.writerow({"segment": seg, "flag_type": "stale", "source_id": sid})
    return path


def _fmt(v: float | None) -> str:
    return "" if v is None else f"{v:.4f}"


# ---------------------------------------------------------------------------
# Segment trend
# ---------------------------------------------------------------------------

_TREND_COLUMNS = [
    "segment",
    "parameter",
    "source_id",
    "current_value",
    "current_as_of",
    "prior_value",
    "prior_as_of",
    "delta",
    "pct_change",
]


def export_segment_trend(
    registry: BenchmarkRegistry,
    out_dir: Path = DEFAULT_OUTPUT_DIR,
) -> Path:
    """Export latest-vs-prior trend rows at source level.

    Commentary rows are skipped (no numeric trend defined).
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "segment_trend.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=_TREND_COLUMNS)
        w.writeheader()
        for row in build_segment_trends(registry):
            if row.current_value is None or row.prior_value is None:
                continue
            w.writerow({
                "segment": row.segment,
                "parameter": row.parameter,
                "source_id": row.source_id,
                "current_value": f"{row.current_value:.6f}",
                "current_as_of": row.current_as_of.isoformat(),
                "prior_value": f"{row.prior_value:.6f}",
                "prior_as_of": row.prior_as_of.isoformat(),
                "delta": "" if row.delta is None else f"{row.delta:.6f}",
                "pct_change": "" if row.pct_change is None else f"{row.pct_change:.6f}",
            })
    return path


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
# Convenience: emit the default model-input CSVs in one call
# ---------------------------------------------------------------------------

def export_all_csvs(
    registry: BenchmarkRegistry,
    *,
    out_dir: Path = DEFAULT_OUTPUT_DIR,
    raw_dir: Path = DEFAULT_RAW_DATA_DIR,
    library: RealityCheckBandLibrary | None = None,
    refresh_schedules: Optional[dict[str, int]] = None,
    refresh_pipeline_quiet: bool = False,
) -> dict[str, Path]:
    """Emit the five model-input CSVs; return a dict keyed by short name.

    ``raw_dir``, ``refresh_schedules``, and ``refresh_pipeline_quiet`` are
    retained for backwards-compatible call sites. They are not used by the
    lean model-input bundle.
    """
    _ = (raw_dir, refresh_schedules, refresh_pipeline_quiet)
    return export_model_input_csvs(registry, out_dir=out_dir, library=library)
