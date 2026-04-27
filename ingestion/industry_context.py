"""Loader for the `industry-analysis` sibling project's canonical parquet contracts.

The upstream `industry-analysis` repo writes four required contract exports
plus two optional explainability panels to `data/exports/`. This module
reads those parquet files into DataFrames without opinion — downstream
Report 2 code shapes them into committee-ready content.

Required contracts:
    industry_risk_scores.parquet
    property_market_overlays.parquet
    downturn_overlay_table.parquet
    macro_regime_flags.parquet

Optional (loaded if present, ignored if absent):
    business_cycle_panel.parquet
    property_cycle_panel.parquet

Design:
    - One public function, `load_industry_analysis_exports(data_dir)`.
    - Returns a dict keyed by parquet stem (e.g. "industry_risk_scores").
    - Raises `FileNotFoundError` for the directory and `MissingExportError`
      for any required contract that isn't on disk. Surfacing a named
      error gives the CLI a clean branch to tell the user which contract
      is missing, rather than a KeyError later in render.
    - `freshness_report()` is a separate helper — callers can warn or
      bail on stale exports without coupling that policy into the loader.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd


REQUIRED_CONTRACTS: tuple[str, ...] = (
    "industry_risk_scores",
    "property_market_overlays",
    "downturn_overlay_table",
    "macro_regime_flags",
)

OPTIONAL_PANELS: tuple[str, ...] = (
    "business_cycle_panel",
    "property_cycle_panel",
)

DEFAULT_STALE_DAYS = 90


class MissingExportError(FileNotFoundError):
    """Raised when one or more required parquet contracts are missing."""


@dataclass(frozen=True)
class FreshnessFinding:
    name: str
    path: Path
    mtime: datetime
    age_days: float
    is_stale: bool


def load_industry_analysis_exports(
    data_dir: str | Path,
    *,
    required: tuple[str, ...] = REQUIRED_CONTRACTS,
    optional: tuple[str, ...] = OPTIONAL_PANELS,
) -> dict[str, pd.DataFrame]:
    """Load canonical industry-analysis parquet contracts into DataFrames.

    Parameters
    ----------
    data_dir
        Path to the `industry-analysis/data/exports/` directory.
    required
        Contract basenames that MUST be present. Missing any of these
        raises `MissingExportError` with all missing names listed.
    optional
        Panel basenames loaded when present; skipped silently when not.

    Returns
    -------
    dict[str, pd.DataFrame]
        Keyed by parquet stem (no `.parquet` suffix). The four required
        contracts are always present; optional panels present only if
        their file was on disk.
    """
    dir_path = Path(data_dir)
    if not dir_path.exists():
        raise FileNotFoundError(
            f"industry-analysis exports directory not found: {dir_path}. "
            "Point `--data-dir` at `industry-analysis/data/exports/` or "
            "re-run the industry-analysis contract export pipeline."
        )

    on_disk = {p.stem: p for p in dir_path.glob("*.parquet")}

    missing = [name for name in required if name not in on_disk]
    if missing:
        raise MissingExportError(
            "Missing required industry-analysis contract exports: "
            f"{', '.join(missing)}. "
            f"Looked in: {dir_path}. "
            "Re-run `scripts/export_contracts.py` in the industry-analysis repo."
        )

    frames: dict[str, pd.DataFrame] = {}
    for name in required:
        frames[name] = pd.read_parquet(on_disk[name])
    for name in optional:
        if name in on_disk:
            frames[name] = pd.read_parquet(on_disk[name])
    return frames


def freshness_report(
    data_dir: str | Path,
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    contracts: tuple[str, ...] = REQUIRED_CONTRACTS,
) -> list[FreshnessFinding]:
    """Return a per-contract age report; flags entries older than `stale_days`.

    Caller decides what to do with stale findings (warn, error, proceed).
    The loader stays policy-free; this helper is where freshness policy
    lives.
    """
    dir_path = Path(data_dir)
    now = datetime.now(timezone.utc)
    cutoff = timedelta(days=stale_days)
    findings: list[FreshnessFinding] = []
    for name in contracts:
        fp = dir_path / f"{name}.parquet"
        if not fp.exists():
            continue
        mtime = datetime.fromtimestamp(fp.stat().st_mtime, tz=timezone.utc)
        age = now - mtime
        findings.append(
            FreshnessFinding(
                name=name,
                path=fp,
                mtime=mtime,
                age_days=age.total_seconds() / 86400.0,
                is_stale=age > cutoff,
            )
        )
    return findings


def summarise_exports(frames: dict[str, pd.DataFrame]) -> dict[str, dict[str, object]]:
    """One-line shape/columns summary per loaded frame. Used in tests and CLI `--verify`."""
    return {
        name: {
            "rows": int(df.shape[0]),
            "cols": int(df.shape[1]),
            "columns": list(df.columns),
        }
        for name, df in frames.items()
    }


def resolve_as_of_date(frames: dict[str, pd.DataFrame]) -> Optional[str]:
    """Pull the canonical as_of_date from macro_regime_flags (single-row frame).

    Returns the string as stored in the parquet (e.g. "2026-03-16") or
    None if the column or row is absent. Used by the report to derive a
    default period label.
    """
    macro = frames.get("macro_regime_flags")
    if macro is None or macro.empty or "as_of_date" not in macro.columns:
        return None
    return str(macro["as_of_date"].iloc[0])
