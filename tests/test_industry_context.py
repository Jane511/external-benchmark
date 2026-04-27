"""Tests for ingestion/industry_context.py — the industry-analysis loader.

Uses a shared fixture to build a tiny in-memory set of parquet files
matching the canonical contract schemas, so these tests don't depend on
the sibling project being present. A separate `live_exports_dir` fixture
reads the real sibling-repo exports when available (skipped otherwise)
to lock down integration against the actual contract surface.
"""
from __future__ import annotations

import os
import time
from pathlib import Path

import pandas as pd
import pytest

from ingestion.industry_context import (
    REQUIRED_CONTRACTS,
    MissingExportError,
    freshness_report,
    load_industry_analysis_exports,
    resolve_as_of_date,
    summarise_exports,
)


# ---------------------------------------------------------------------------
# Synthetic-fixture helpers — a 4-row + 3-row + 4-row + 1-row schema-faithful set
# ---------------------------------------------------------------------------


def _write_industry_risk(path: Path) -> None:
    df = pd.DataFrame({
        "industry": ["Agriculture, Forestry And Fishing", "Manufacturing",
                     "Construction", "Healthcare And Social Assistance"],
        "classification_risk_score": [3.75, 3.75, 2.75, 2.00],
        "macro_risk_score":          [3.20, 3.20, 2.60, 2.00],
        "industry_base_risk_score":  [3.50, 3.50, 2.68, 2.00],
        "industry_base_risk_level":  ["Elevated", "Elevated", "Medium", "Low"],
        "cash_rate_latest_pct":      [3.85, 3.85, 3.85, 3.85],
        "cash_rate_change_1y_pctpts":[-0.25, -0.25, -0.25, -0.25],
    })
    df.to_parquet(path, index=False)


def _write_property(path: Path) -> None:
    df = pd.DataFrame({
        "property_segment":     ["Offices", "Education buildings",
                                 "Retail buildings", "Short term accommodation"],
        "cycle_stage":          ["downturn", "slowing", "neutral", "growth"],
        "market_softness_score":[4.30, 3.25, 3.15, 2.85],
        "region_risk_score":    [4.03, 3.38, 2.95, 2.55],
        "region_risk_band":     ["High", "Elevated", "Medium", "Medium"],
        "approvals_change_pct": [-35.72, -21.37, 68.47, 113.70],
        "commencements_signal": ["Proxy"] * 4,
        "completions_signal":   ["Proxy"] * 4,
        "market_softness_band": ["soft", "softening", "normal", "supportive"],
    })
    df.to_parquet(path, index=False)


def _write_downturn(path: Path) -> None:
    df = pd.DataFrame({
        "scenario":              ["base", "mild", "moderate", "severe"],
        "pd_multiplier":         [1.0, 1.2, 1.5, 2.0],
        "lgd_multiplier":        [1.0, 1.1, 1.2, 1.3],
        "ccf_multiplier":        [1.00, 1.05, 1.10, 1.20],
        "property_value_haircut":[0.00, 0.05, 0.10, 0.20],
        "notes":                 ["base", "mild", "moderate", "severe"],
        "as_of_date":            ["2026-03-16"] * 4,
    })
    df.to_parquet(path, index=False)


def _write_macro(path: Path) -> None:
    df = pd.DataFrame({
        "as_of_date":                ["2026-03-16"],
        "cash_rate_regime":          ["neutral_easing"],
        "arrears_environment_level": ["Low"],
        "arrears_trend":             ["Improving"],
        "macro_regime_flag":         ["base"],
        "source_dataset":            ["test fixture"],
    })
    df.to_parquet(path, index=False)


@pytest.fixture()
def exports_dir(tmp_path: Path) -> Path:
    """tmp dir populated with the four required parquet contracts."""
    d = tmp_path / "exports"
    d.mkdir()
    _write_industry_risk(d / "industry_risk_scores.parquet")
    _write_property(d / "property_market_overlays.parquet")
    _write_downturn(d / "downturn_overlay_table.parquet")
    _write_macro(d / "macro_regime_flags.parquet")
    return d


# Optional — connect to the real sibling-repo exports when they exist.
_LIVE_EXPORTS = Path(
    "D:/Jane/Job Search/Github/credit-risk-portfolio_bank/"
    "credit risk models commercial/industry-analysis/data/exports"
)


@pytest.fixture()
def live_exports_dir() -> Path:
    if not _LIVE_EXPORTS.exists():
        pytest.skip("industry-analysis sibling repo not available on this machine")
    return _LIVE_EXPORTS


# ---------------------------------------------------------------------------
# Loader tests
# ---------------------------------------------------------------------------


def test_load_returns_all_required_contracts(exports_dir: Path) -> None:
    frames = load_industry_analysis_exports(exports_dir)
    for name in REQUIRED_CONTRACTS:
        assert name in frames, f"missing required contract: {name}"


def test_load_shapes_match_fixture(exports_dir: Path) -> None:
    frames = load_industry_analysis_exports(exports_dir)
    assert frames["industry_risk_scores"].shape == (4, 7)
    assert frames["property_market_overlays"].shape == (4, 9)
    assert frames["downturn_overlay_table"].shape == (4, 7)
    assert frames["macro_regime_flags"].shape == (1, 6)


def test_load_columns_match_expected_schema(exports_dir: Path) -> None:
    frames = load_industry_analysis_exports(exports_dir)
    assert "industry_base_risk_score" in frames["industry_risk_scores"].columns
    assert "cycle_stage" in frames["property_market_overlays"].columns
    assert {"pd_multiplier", "lgd_multiplier", "ccf_multiplier",
            "property_value_haircut"} <= set(frames["downturn_overlay_table"].columns)
    assert "macro_regime_flag" in frames["macro_regime_flags"].columns


def test_load_missing_directory_raises_filenotfound(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist"
    with pytest.raises(FileNotFoundError):
        load_industry_analysis_exports(missing)


def test_load_missing_required_contract_raises(tmp_path: Path) -> None:
    d = tmp_path / "partial"
    d.mkdir()
    _write_industry_risk(d / "industry_risk_scores.parquet")
    # Deliberately omit the other three contracts.
    with pytest.raises(MissingExportError) as ei:
        load_industry_analysis_exports(d)
    msg = str(ei.value)
    for expected in ("property_market_overlays", "downturn_overlay_table",
                     "macro_regime_flags"):
        assert expected in msg, f"{expected} not mentioned in error: {msg}"


def test_load_includes_optional_panels_when_present(exports_dir: Path) -> None:
    (exports_dir / "business_cycle_panel.parquet").write_bytes(
        (exports_dir / "macro_regime_flags.parquet").read_bytes()
    )
    frames = load_industry_analysis_exports(exports_dir)
    assert "business_cycle_panel" in frames


def test_summarise_exports_reports_shapes(exports_dir: Path) -> None:
    frames = load_industry_analysis_exports(exports_dir)
    summary = summarise_exports(frames)
    assert summary["industry_risk_scores"]["rows"] == 4
    assert summary["industry_risk_scores"]["cols"] == 7
    assert "industry_base_risk_score" in summary["industry_risk_scores"]["columns"]


def test_resolve_as_of_date_returns_macro_date(exports_dir: Path) -> None:
    frames = load_industry_analysis_exports(exports_dir)
    assert resolve_as_of_date(frames) == "2026-03-16"


def test_resolve_as_of_date_returns_none_when_missing() -> None:
    empty = {"macro_regime_flags": pd.DataFrame()}
    assert resolve_as_of_date(empty) is None


# ---------------------------------------------------------------------------
# Freshness tests
# ---------------------------------------------------------------------------


def test_freshness_flags_old_files(tmp_path: Path) -> None:
    d = tmp_path / "old"
    d.mkdir()
    _write_macro(d / "macro_regime_flags.parquet")
    old_time = time.time() - (200 * 86400)
    os.utime(d / "macro_regime_flags.parquet", (old_time, old_time))

    findings = freshness_report(d, stale_days=90,
                                contracts=("macro_regime_flags",))
    assert findings[0].is_stale is True
    assert findings[0].age_days > 180


def test_freshness_passes_recent_files(exports_dir: Path) -> None:
    findings = freshness_report(exports_dir, stale_days=90)
    for f in findings:
        assert f.is_stale is False


# ---------------------------------------------------------------------------
# Live integration — locked to the real sibling project when available.
# ---------------------------------------------------------------------------


def test_live_exports_load_clean(live_exports_dir: Path) -> None:
    """Smoke-integration: the real parquet contracts load without error."""
    frames = load_industry_analysis_exports(live_exports_dir)
    for name in REQUIRED_CONTRACTS:
        assert name in frames
        assert not frames[name].empty


def test_live_industry_has_expected_key_columns(live_exports_dir: Path) -> None:
    frames = load_industry_analysis_exports(live_exports_dir)
    assert "industry_base_risk_score" in frames["industry_risk_scores"].columns
    # cycle_stage values must all be in the known vocabulary.
    stages = set(frames["property_market_overlays"]["cycle_stage"].unique())
    allowed = {"downturn", "slowing", "neutral", "growth"}
    assert stages <= allowed, f"unexpected cycle_stage values: {stages - allowed}"
