"""Tests for reports/environment_report.py — Report 2 structure and renderers.

The `generate()` dict is the contract between the report composer and its
renderers. Tests primarily assert on that dict. One smoke test per
renderer (docx/html/board-md/technical-md) confirms the produced file
exists and contains section headings; full visual review is manual.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from reports.environment_report import (
    CYCLE_STAGE_DISPLAY,
    DOWNTURN_SCENARIO_NARRATIVE,
    EnvironmentReport,
    METHODOLOGY_NOTES,
)
from tests.test_industry_context import exports_dir, live_exports_dir  # noqa: F401


@pytest.fixture()
def report(exports_dir: Path) -> EnvironmentReport:
    return EnvironmentReport.from_data_dir(exports_dir, period_label="Q1 2026")


# ---------------------------------------------------------------------------
# Meta + section-presence
# ---------------------------------------------------------------------------


def test_generate_has_all_expected_sections(report: EnvironmentReport) -> None:
    data = report.generate()
    for key in ("meta", "executive_summary", "industry_risk_outlook",
                "property_market_outlook", "downturn_scenarios",
                "methodology_notes"):
        assert key in data, f"missing section: {key}"


def test_meta_surfaces_data_as_of(report: EnvironmentReport) -> None:
    data = report.generate()
    assert data["meta"]["data_as_of"] == "2026-03-16"
    assert data["meta"]["period_label"] == "Q1 2026"
    assert "industry_risk_scores" in data["meta"]["loaded_frames"]


def test_period_derivation_from_macro_as_of_date(exports_dir: Path) -> None:
    """When no period is passed, it should derive from macro_regime_flags."""
    r = EnvironmentReport.from_data_dir(exports_dir)   # no period_label
    assert r.generate()["meta"]["period_label"] == "Q1 2026"


# ---------------------------------------------------------------------------
# Section 1 — Executive Summary
# ---------------------------------------------------------------------------


def test_executive_summary_regime_is_base_from_fixture(report: EnvironmentReport) -> None:
    exec_ = report.generate()["executive_summary"]
    assert exec_["regime_flag"] == "base"
    assert "benign" in exec_["regime_interpretation"].lower()


def test_executive_summary_counts_elevated_industries(report: EnvironmentReport) -> None:
    exec_ = report.generate()["executive_summary"]
    # Fixture has 2 of 4 industries at "Elevated"
    assert exec_["elevated_industry_count"] == 2
    assert exec_["industry_count"] == 4


def test_executive_summary_highlights_downturn_growth_segments(report: EnvironmentReport) -> None:
    exec_ = report.generate()["executive_summary"]
    bullets_text = "\n".join(exec_["bullets"])
    assert "Downturn segments" in bullets_text
    assert "Growth segments" in bullets_text
    # Fixture top industry is Agriculture or Manufacturing (tied at 3.50).
    assert exec_["top_industry_score"] == pytest.approx(3.50)


# ---------------------------------------------------------------------------
# Section 2 — Industry Risk Outlook
# ---------------------------------------------------------------------------


def test_industry_rows_sorted_by_base_risk_desc(report: EnvironmentReport) -> None:
    rows = report.generate()["industry_risk_outlook"]["rows"]
    scores = [r["base_risk"] for r in rows]
    assert scores == sorted(scores, reverse=True)
    assert rows[0]["rank"] == 1
    assert rows[-1]["rank"] == len(rows)


def test_top3_commentary_has_three_entries(report: EnvironmentReport) -> None:
    iro = report.generate()["industry_risk_outlook"]
    assert len(iro["top3_commentary"]) == 3


def test_construction_caveat_referenced(report: EnvironmentReport) -> None:
    iro = report.generate()["industry_risk_outlook"]
    assert "Construction" in iro["construction_caveat"]
    assert "structural" in iro["construction_caveat"].lower()


def test_cash_rate_conditioner_surfaced(report: EnvironmentReport) -> None:
    iro = report.generate()["industry_risk_outlook"]
    assert iro["cash_rate_latest"] == pytest.approx(3.85)
    assert iro["cash_rate_change_1y"] == pytest.approx(-0.25)


# ---------------------------------------------------------------------------
# Section 3 — Property Market Outlook
# ---------------------------------------------------------------------------


def test_property_groups_by_cycle_stage_in_expected_order(report: EnvironmentReport) -> None:
    groups = report.generate()["property_market_outlook"]["groups"]
    stages = [g["stage"] for g in groups]
    # Fixture has one segment per stage in the order downturn/slowing/neutral/growth.
    assert stages == ["downturn", "slowing", "neutral", "growth"]
    for g in groups:
        assert g["stage_display"] == CYCLE_STAGE_DISPLAY[g["stage"]]
        assert g["rows"], f"no rows for stage {g['stage']}"


def test_property_commentary_includes_most_at_risk_and_tailwinds(report: EnvironmentReport) -> None:
    pmo = report.generate()["property_market_outlook"]
    joined = "\n".join(pmo["commentary"])
    assert "Most at risk" in joined
    assert "tailwinds" in joined.lower()
    assert "Proxy from approvals" in joined or "proxy" in joined.lower()


# ---------------------------------------------------------------------------
# Section 4 — Downturn Scenarios
# ---------------------------------------------------------------------------


def test_downturn_rows_have_all_four_scenarios(report: EnvironmentReport) -> None:
    dwn = report.generate()["downturn_scenarios"]
    scenarios = [r["scenario"] for r in dwn["rows"]]
    assert set(scenarios) == {"base", "mild", "moderate", "severe"}


def test_downturn_monotonicity_flags_pass_for_well_formed_data(report: EnvironmentReport) -> None:
    dwn = report.generate()["downturn_scenarios"]
    assert dwn["monotonic_pd"] is True
    assert dwn["monotonic_lgd"] is True
    assert dwn["monotonic_ccf"] is True
    assert dwn["monotonic_haircut"] is True


def test_downturn_interpretation_attached_per_row(report: EnvironmentReport) -> None:
    rows = report.generate()["downturn_scenarios"]["rows"]
    for r in rows:
        assert r["interpretation"] == DOWNTURN_SCENARIO_NARRATIVE[r["scenario"]]
        assert r["interpretation"]   # non-empty


def test_monotonicity_fails_when_multipliers_regress(tmp_path: Path) -> None:
    """Inject a broken downturn table; monotonic_pd should be False."""
    from tests.test_industry_context import (
        _write_industry_risk, _write_property, _write_macro,
    )

    d = tmp_path / "broken"
    d.mkdir()
    _write_industry_risk(d / "industry_risk_scores.parquet")
    _write_property(d / "property_market_overlays.parquet")
    _write_macro(d / "macro_regime_flags.parquet")
    pd.DataFrame({
        "scenario":               ["base", "mild", "moderate", "severe"],
        "pd_multiplier":          [1.0, 1.2, 1.1, 2.0],   # mild > moderate — violation
        "lgd_multiplier":         [1.0, 1.1, 1.2, 1.3],
        "ccf_multiplier":         [1.0, 1.05, 1.10, 1.20],
        "property_value_haircut": [0.00, 0.05, 0.10, 0.20],
        "notes":                  ["b", "m", "mo", "s"],
        "as_of_date":             ["2026-03-16"] * 4,
    }).to_parquet(d / "downturn_overlay_table.parquet", index=False)

    r = EnvironmentReport.from_data_dir(d, period_label="Q1 2026")
    assert r.generate()["downturn_scenarios"]["monotonic_pd"] is False
    assert r.generate()["downturn_scenarios"]["monotonic_lgd"] is True


# ---------------------------------------------------------------------------
# Section 5 — Methodology Notes
# ---------------------------------------------------------------------------


def test_methodology_surfaces_construction_review_options_and_path(
    report: EnvironmentReport,
) -> None:
    mn = report.generate()["methodology_notes"]
    assert "structural" in mn["structural_vs_current_state"].lower()
    assert "Construction" in mn["construction_review"]
    # All three options appear in "options_considered".
    opts = mn["options_considered"].lower()
    assert "accept" in opts and "industry-stress" in opts and "document the limitation" in opts
    # Report takes option 3 (document the limitation).
    assert "option 3" in mn["path_taken"].lower() or \
           "document" in mn["path_taken"].lower()


# ---------------------------------------------------------------------------
# Renderer smoke tests
# ---------------------------------------------------------------------------


def test_render_markdown_board_writes_file_with_sections(
    report: EnvironmentReport, tmp_path: Path,
) -> None:
    out = tmp_path / "report_board.md"
    report.to_board_markdown(out)
    text = out.read_text(encoding="utf-8")
    for heading in ("1. Executive Summary", "2. Industry Risk Outlook",
                    "3. Property Market Outlook", "4. Downturn Scenario Overlays",
                    "5. Methodology Notes"):
        assert heading in text, f"missing section {heading}"
    assert "Board Summary" in text


def test_render_markdown_technical_writes_file_with_sections(
    report: EnvironmentReport, tmp_path: Path,
) -> None:
    out = tmp_path / "report_tech.md"
    report.to_markdown(out)
    text = out.read_text(encoding="utf-8")
    assert "Technical Appendix" in text
    assert "Data provenance" in text
    assert "Monotonicity audit" in text
    assert "Options considered upstream" in text


def test_render_html_writes_valid_structure(
    report: EnvironmentReport, tmp_path: Path,
) -> None:
    out = tmp_path / "report.html"
    report.to_html(out)
    html = out.read_text(encoding="utf-8")
    assert "<!DOCTYPE html>" in html
    assert "id='executive-summary'" in html
    assert "id='downturn-scenarios'" in html
    assert "id='methodology-notes'" in html
    # No leaked format placeholders
    assert "{period}" not in html
    assert "{" not in html.split("<style>", 1)[0]  # no placeholder before CSS


def test_render_docx_produces_nonempty_file(
    report: EnvironmentReport, tmp_path: Path,
) -> None:
    pytest.importorskip("docx")
    out = tmp_path / "report.docx"
    report.to_docx(out)
    assert out.exists() and out.stat().st_size > 5000   # non-trivial docx zip


# ---------------------------------------------------------------------------
# Integration against live industry-analysis exports
# ---------------------------------------------------------------------------


def test_live_report_generates_end_to_end(live_exports_dir: Path) -> None:
    """Full generate() path against the real sibling-repo contracts."""
    r = EnvironmentReport.from_data_dir(live_exports_dir, period_label="Q1 2026")
    data = r.generate()
    # Expected 9 industries, 11 property segments in live data as of 2026-03-16.
    assert len(data["industry_risk_outlook"]["rows"]) >= 5
    assert data["property_market_outlook"]["total_segments"] >= 5
    assert data["executive_summary"]["regime_flag"]
    assert data["downturn_scenarios"]["monotonic_pd"] is True
