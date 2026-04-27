"""Report 2 — Environment and Industry Overlay Report.

Companion to Report 1 (`benchmark_report.BenchmarkCalibrationReport`).
Report 1 is pure engine data (registry, adjustments, calibration feeds).
Report 2 sits next to it with external environmental context sourced from
the `industry-analysis` sibling project's canonical parquet contracts:

    industry_risk_scores        — per-ANZSIC-division structural risk score
    property_market_overlays    — per-segment property cycle stage + softness
    downturn_overlay_table      — PD/LGD/CCF multipliers for base/mild/moderate/severe
    macro_regime_flags          — single-row summary of cash-rate regime + arrears environment

This module builds a `generate()` dict with five sections; thin renderers
in `render_environment_{docx,html,md}.py` format that dict for each
output type. Pattern mirrors `BenchmarkCalibrationReport` so the two
reports stay consistent in shape for the committee audience.

Audience and purpose:
    Primary — MRC and Credit Committee reviewers reading Report 1 want
    one-page context on what the industry/macro backdrop looks like when
    signing off calibration. Report 2 delivers that context without
    requiring them to open the upstream industry-analysis project.
    Secondary — model validators auditing the calibration can trace the
    environmental assumptions being used in the PD/LGD overlay chain.

Caveats surfaced in Section 5:
    - Industry base risk scores are structural, not current-state. The
      upstream Construction methodology review item (three options:
      accept as-is, add sector-stress overlay, document the limitation)
      is reproduced verbatim so downstream consumers see the same caveat
      as industry-analysis readers.
    - Property commencements/completions in this cycle are proxied from
      approvals, not directly observed.
"""
from __future__ import annotations

import html as _html
import statistics
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd

from ingestion.industry_context import (
    load_industry_analysis_exports,
    freshness_report,
    resolve_as_of_date,
)


REPORT_TITLE = "External Benchmark — Environment & Industry Overlay Report — {period}"
REPORT_SUBTITLE = "Companion to Report 1 · Sourced from industry-analysis v1 contracts"

# Section order is fixed: exec summary first, then risk outlook, then
# property, then downturn, then methodology. Anchors used in HTML.
SECTION_ORDER: tuple[tuple[str, str], ...] = (
    ("executive_summary",      "1. Executive Summary"),
    ("industry_risk_outlook",  "2. Industry Risk Outlook"),
    ("property_market_outlook","3. Property Market Outlook"),
    ("downturn_scenarios",     "4. Downturn Scenario Overlays"),
    ("methodology_notes",      "5. Methodology Notes"),
)

# Display labels for cycle_stage; upstream writes these lower-case.
CYCLE_STAGE_ORDER: tuple[str, ...] = ("downturn", "slowing", "neutral", "growth")
CYCLE_STAGE_DISPLAY: dict[str, str] = {
    "downturn": "Downturn",
    "slowing":  "Slowing",
    "neutral":  "Neutral",
    "growth":   "Growth",
}

# One-line narrative per cycle stage — used under the grouped table.
CYCLE_STAGE_NARRATIVE: dict[str, str] = {
    "downturn": "Contracting segments — approvals trending down materially, market softness elevated. Highest portfolio risk; consider limit tightening and LVR haircuts.",
    "slowing":  "Past-peak segments — approvals flat to soft, market softness rising. Watch list; no immediate tightening required but concentration should be monitored.",
    "neutral":  "Mid-cycle segments — approvals mixed, softness neither supportive nor stressed. Standard credit appetite applies.",
    "growth":   "Expanding segments — approvals trending up materially, market softness low. Opportunity-side; assess whether current limits capture the tailwind without overshoot.",
}

# Downturn scenario descriptions. Spec calls these out explicitly: "Mild = X,
# Moderate = Y, Severe = Z" with interpretation of what each means.
DOWNTURN_SCENARIO_NARRATIVE: dict[str, str] = {
    "base":     "Current staged environment. Multipliers = 1.00 by construction; represents the starting point before any downturn overlay is applied.",
    "mild":     "A short, shallow downturn — typical of a mid-cycle pullback. Use for conservative portfolio calibration when the book is running on optimistic through-the-cycle assumptions.",
    "moderate": "A deeper, more prolonged stress consistent with a material macro slowdown. Use as the primary stressed-pricing input and for ECL staging sensitivities.",
    "severe":   "Tail-risk scenario. Not calibrated to any specific regulatory stress (e.g. APRA CPG 220). Use for concentration-risk stress testing and what-if analysis, not for capital.",
}

# Methodology section copy. Reproduces the upstream Construction caveat.
METHODOLOGY_NOTES: dict[str, str] = {
    "intro": (
        "Report 2 is a read-only overlay view of the `industry-analysis` project's "
        "canonical parquet exports. Benchmark-engine code does not modify or "
        "recompute those contracts; changes to the underlying scoring methodology "
        "must be raised in that upstream repo."
    ),
    "structural_vs_current_state": (
        "Industry base risk scores reflect structural classification factors "
        "(cyclicality, market concentration) blended with a single macro conditioner "
        "(cash-rate regime and one-year change). They do not incorporate real-time "
        "sector-stress signals such as ASIC insolvency flows, subcontractor arrears, "
        "or sector-specific collapse events."
    ),
    "construction_review": (
        "Construction is the leading example: the structural score places it in the "
        "'Medium' band, but market narrative over 2024–2026 (Porter Davis, Probuild, "
        "Clough collapses; subcontractor arrears at multi-year highs; fixed-price + "
        "materials-inflation squeeze) suggests 'Elevated' is the more honest current-"
        "state read. This divergence is a known methodology-review item in industry-"
        "analysis, not a data error."
    ),
    "options_considered": (
        "The upstream review lists three options for next methodology iteration: "
        "(1) accept the structural-vs-current-state design as-is; "
        "(2) add an industry-stress overlay that lifts the base score when ASIC "
        "insolvency rates exceed a threshold for a given ANZSIC division; "
        "(3) document the limitation in the methodology manual and downstream "
        "consumer documentation."
    ),
    "path_taken": (
        "This report takes **Option 3 — document the limitation**. It surfaces the "
        "Construction caveat to committee readers explicitly, without altering the "
        "upstream risk scores or applying a local override. A future methodology "
        "cycle may choose Option 1 or 2; until then, treat the industry risk table "
        "as a structural backdrop, and pair it with the ABS/ASIC failure-rate "
        "context in Report 1 §4 (Industry Context) for a fuller picture."
    ),
    "property_proxy_caveat": (
        "In the current industry-analysis cycle, property-segment `commencements_signal` "
        "and `completions_signal` are proxied from approvals trend because direct "
        "commencements/completions series are not yet wired into the pipeline. Read "
        "the `approvals_change_pct` column as the leading indicator; the two signal "
        "columns are flagged 'Proxy from approvals trend' for transparency."
    ),
}


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------


class EnvironmentReport:
    """Compose Report 2 sections and render to DOCX/HTML/Markdown.

    Keep the class thin: heavy formatting lives in `render_environment_*.py`.
    The `generate()` dict is the contract between this class and every
    renderer; tests assert on that dict rather than on rendered output.
    """

    def __init__(
        self,
        frames: dict[str, pd.DataFrame],
        *,
        period_label: Optional[str] = None,
        data_dir: Optional[Path | str] = None,
        freshness: Optional[list] = None,
    ) -> None:
        self._frames = frames
        self._data_dir = Path(data_dir) if data_dir else None
        self._freshness = freshness or []
        self._period = period_label or self._default_period_label()

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_data_dir(
        cls,
        data_dir: str | Path,
        *,
        period_label: Optional[str] = None,
        stale_days: int = 90,
    ) -> "EnvironmentReport":
        """Load parquet contracts from `data_dir` and return a ready-to-render report."""
        frames = load_industry_analysis_exports(data_dir)
        fresh = freshness_report(data_dir, stale_days=stale_days)
        return cls(
            frames,
            period_label=period_label,
            data_dir=data_dir,
            freshness=fresh,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(self) -> dict[str, Any]:
        """Return a structured dict containing all five sections + meta."""
        return {
            "meta":                     self._build_meta(),
            "executive_summary":        self._build_executive_summary(),
            "industry_risk_outlook":    self._build_industry_risk_outlook(),
            "property_market_outlook":  self._build_property_market_outlook(),
            "downturn_scenarios":       self._build_downturn_scenarios(),
            "methodology_notes":        self._build_methodology_notes(),
        }

    def to_docx(self, path: Path | str) -> Path:
        from reports.render_environment_docx import render_docx
        return render_docx(self.generate(), path)

    def to_html(self, path: Path | str) -> Path:
        from reports.render_environment_html import render_html
        return render_html(self.generate(), path)

    def to_markdown(self, path: Path | str) -> Path:
        """Technical markdown variant: full tables + commentary + methodology."""
        from reports.render_environment_md import render_technical_markdown
        return render_technical_markdown(self.generate(), path)

    def to_board_markdown(self, path: Path | str) -> Path:
        """Board-ready markdown variant: trimmed tables + high-level commentary."""
        from reports.render_environment_md import render_board_markdown
        return render_board_markdown(self.generate(), path)

    # ------------------------------------------------------------------
    # Period / meta
    # ------------------------------------------------------------------

    def _default_period_label(self) -> str:
        """Derive period from macro_regime_flags.as_of_date when possible.

        Falls back to today's quarter. The underlying data is typically
        published quarterly; aligning the period label with the data's
        as-of date avoids committee confusion when report generation
        happens in a quarter that's already past the data snapshot.
        """
        as_of_str = resolve_as_of_date(self._frames)
        if as_of_str:
            try:
                d = datetime.strptime(as_of_str, "%Y-%m-%d").date()
            except ValueError:
                d = date.today()
        else:
            d = date.today()
        quarter = (d.month - 1) // 3 + 1
        return f"Q{quarter} {d.year}"

    def _build_meta(self) -> dict[str, Any]:
        as_of = resolve_as_of_date(self._frames)
        return {
            "report_title":       REPORT_TITLE.format(period=self._period),
            "report_subtitle":    REPORT_SUBTITLE,
            "period_label":       self._period,
            "generated_at":       datetime.now(timezone.utc).isoformat(),
            "data_as_of":         as_of,
            "data_dir":           str(self._data_dir) if self._data_dir else None,
            "loaded_frames":      sorted(self._frames.keys()),
            "freshness_findings": [
                {
                    "name":      f.name,
                    "age_days":  round(f.age_days, 1),
                    "is_stale":  f.is_stale,
                    "mtime":     f.mtime.isoformat(),
                }
                for f in self._freshness
            ],
        }

    # ------------------------------------------------------------------
    # Section 1 — Executive Summary
    # ------------------------------------------------------------------

    def _build_executive_summary(self) -> dict[str, Any]:
        """High-level findings from industry risk scores + property overlays + macro flag."""
        industry = self._frames["industry_risk_scores"]
        property_df = self._frames["property_market_overlays"]
        macro = self._frames["macro_regime_flags"]

        # Industry risk headlines
        ind_sorted = industry.sort_values("industry_base_risk_score", ascending=False)
        top_industry = ind_sorted.iloc[0]
        bottom_industry = ind_sorted.iloc[-1]
        elevated_count = int((industry["industry_base_risk_level"] == "Elevated").sum())

        # Property cycle headlines
        stage_counts = property_df["cycle_stage"].value_counts().to_dict()
        downturn_segments = property_df.loc[
            property_df["cycle_stage"] == "downturn", "property_segment"
        ].tolist()
        growth_segments = property_df.loc[
            property_df["cycle_stage"] == "growth", "property_segment"
        ].tolist()

        # Macro regime (single row)
        macro_row = macro.iloc[0].to_dict() if not macro.empty else {}
        regime_flag = macro_row.get("macro_regime_flag", "unknown")
        cash_regime = macro_row.get("cash_rate_regime", "unknown")
        arrears_env = macro_row.get("arrears_environment_level", "unknown")
        arrears_trend = macro_row.get("arrears_trend", "unknown")

        headline_bullets = [
            (
                f"Macro regime flag is **{regime_flag}** "
                f"(cash-rate regime: {cash_regime}; arrears environment: "
                f"{arrears_env}, trending {arrears_trend.lower() if isinstance(arrears_trend, str) else arrears_trend})."
            ),
            (
                f"{elevated_count} of {len(industry)} industries carry an 'Elevated' "
                f"base risk level; top by structural score is "
                f"**{top_industry['industry']}** ({top_industry['industry_base_risk_score']:.2f}, "
                f"{top_industry['industry_base_risk_level']})."
            ),
            (
                f"Property cycle shows {stage_counts.get('downturn', 0)} segment(s) in "
                f"Downturn, {stage_counts.get('slowing', 0)} Slowing, "
                f"{stage_counts.get('neutral', 0)} Neutral, "
                f"{stage_counts.get('growth', 0)} Growth."
            ),
        ]
        if downturn_segments:
            headline_bullets.append(
                "Downturn segments: " + ", ".join(str(s) for s in downturn_segments) + "."
            )
        if growth_segments:
            headline_bullets.append(
                "Growth segments: " + ", ".join(str(s) for s in growth_segments) + "."
            )

        # Regime interpretation — a plain-English read of the flag.
        regime_interpretation = _interpret_macro_regime(regime_flag, cash_regime, arrears_env, arrears_trend)

        return {
            "bullets":                headline_bullets,
            "regime_flag":            regime_flag,
            "regime_interpretation":  regime_interpretation,
            "elevated_industry_count": elevated_count,
            "industry_count":         int(len(industry)),
            "top_industry":           str(top_industry["industry"]),
            "top_industry_score":     float(top_industry["industry_base_risk_score"]),
            "bottom_industry":        str(bottom_industry["industry"]),
            "bottom_industry_score":  float(bottom_industry["industry_base_risk_score"]),
            "stage_counts":           {k: int(v) for k, v in stage_counts.items()},
        }

    # ------------------------------------------------------------------
    # Section 2 — Industry Risk Outlook
    # ------------------------------------------------------------------

    def _build_industry_risk_outlook(self) -> dict[str, Any]:
        industry = self._frames["industry_risk_scores"].copy()
        industry = industry.sort_values("industry_base_risk_score", ascending=False).reset_index(drop=True)

        # Render-ready rows. Floats formatted at render time to keep the
        # dict numeric for tests and programmatic consumers.
        rows = []
        for i, r in industry.iterrows():
            rows.append({
                "rank":                int(i) + 1,
                "industry":            str(r["industry"]),
                "classification_risk": float(r["classification_risk_score"]),
                "macro_risk":          float(r["macro_risk_score"]),
                "base_risk":           float(r["industry_base_risk_score"]),
                "level":               str(r["industry_base_risk_level"]),
            })

        top3 = rows[:3]
        # Driver commentary: macro vs classification split for each top-3 row.
        top3_commentary = []
        for row in top3:
            macro_share = row["macro_risk"] / row["base_risk"] if row["base_risk"] else 0
            if macro_share > 0.95:
                driver = "macro and structural factors roughly balanced"
            elif row["classification_risk"] > row["macro_risk"]:
                driver = "primarily structural (cyclicality / concentration)"
            else:
                driver = "primarily macro (cash-rate regime conditioning)"
            top3_commentary.append(
                f"**{row['industry']}** (score {row['base_risk']:.2f}, {row['level']}): "
                f"{driver}. Classification component {row['classification_risk']:.2f}, "
                f"macro component {row['macro_risk']:.2f}."
            )

        # Macro conditioner — all rows share cash_rate_latest_pct by design;
        # surface that transparently so readers understand the uniformity.
        cash_rate_latest = float(industry["cash_rate_latest_pct"].iloc[0])
        cash_rate_change = float(industry["cash_rate_change_1y_pctpts"].iloc[0])

        return {
            "rows":                rows,
            "top3_commentary":     top3_commentary,
            "cash_rate_latest":    cash_rate_latest,
            "cash_rate_change_1y": cash_rate_change,
            "construction_caveat": METHODOLOGY_NOTES["construction_review"],
        }

    # ------------------------------------------------------------------
    # Section 3 — Property Market Outlook
    # ------------------------------------------------------------------

    def _build_property_market_outlook(self) -> dict[str, Any]:
        pmo = self._frames["property_market_overlays"].copy()

        # Group by cycle_stage; within each group order by softness desc.
        groups: list[dict[str, Any]] = []
        for stage in CYCLE_STAGE_ORDER:
            sub = pmo[pmo["cycle_stage"] == stage].copy()
            if sub.empty:
                continue
            sub = sub.sort_values("market_softness_score", ascending=False)
            rows = [{
                "property_segment":    str(r["property_segment"]),
                "softness_score":      float(r["market_softness_score"]),
                "softness_band":       str(r["market_softness_band"]),
                "region_risk_score":   float(r["region_risk_score"]),
                "region_risk_band":    str(r["region_risk_band"]),
                "approvals_change_pct": float(r["approvals_change_pct"]),
            } for _, r in sub.iterrows()]
            groups.append({
                "stage":         stage,
                "stage_display": CYCLE_STAGE_DISPLAY[stage],
                "narrative":     CYCLE_STAGE_NARRATIVE[stage],
                "rows":          rows,
            })

        # Risk concentration commentary: which segments are most softness-stressed
        # vs which are benefiting from tailwinds (approvals strongly positive).
        pmo_sorted_soft = pmo.sort_values("market_softness_score", ascending=False)
        most_at_risk = pmo_sorted_soft.head(3).to_dict("records")
        most_at_risk_names = [
            f"{r['property_segment']} (softness {r['market_softness_score']:.2f}, "
            f"{r['cycle_stage']})"
            for r in most_at_risk
        ]

        pmo_sorted_approv = pmo.sort_values("approvals_change_pct", ascending=False)
        tailwind_rows = pmo_sorted_approv.head(3).to_dict("records")
        tailwind_names = [
            f"{r['property_segment']} (approvals {r['approvals_change_pct']:+.1f}%, "
            f"{r['cycle_stage']})"
            for r in tailwind_rows
        ]

        commentary = [
            "**Most at risk (highest market softness):** " + "; ".join(most_at_risk_names) + ".",
            "**Strongest tailwinds (highest approvals momentum):** " + "; ".join(tailwind_names) + ".",
            METHODOLOGY_NOTES["property_proxy_caveat"],
        ]

        return {
            "groups":                  groups,
            "commentary":              commentary,
            "total_segments":          int(len(pmo)),
            "most_at_risk_segments":   most_at_risk_names,
            "tailwind_segments":       tailwind_names,
        }

    # ------------------------------------------------------------------
    # Section 4 — Downturn Scenario Overlays
    # ------------------------------------------------------------------

    def _build_downturn_scenarios(self) -> dict[str, Any]:
        dwn = self._frames["downturn_overlay_table"].copy()

        rows = []
        for _, r in dwn.iterrows():
            rows.append({
                "scenario":               str(r["scenario"]),
                "scenario_display":       str(r["scenario"]).title(),
                "pd_multiplier":          float(r["pd_multiplier"]),
                "lgd_multiplier":         float(r["lgd_multiplier"]),
                "ccf_multiplier":         float(r["ccf_multiplier"]),
                "property_value_haircut": float(r["property_value_haircut"]),
                "notes":                  str(r["notes"]),
                "as_of_date":             str(r["as_of_date"]),
                "interpretation":         DOWNTURN_SCENARIO_NARRATIVE.get(
                    str(r["scenario"]), ""
                ),
            })

        # Monotonicity check — multipliers should increase base -> severe.
        # Surface a pass/fail finding rather than asserting; any violation
        # is a contract-test failure upstream, but we flag it in the report
        # so committee readers see if something has gone wrong in between.
        monotonic = _check_monotonicity(rows)

        return {
            "rows":                rows,
            "monotonic_pd":        monotonic["pd"],
            "monotonic_lgd":       monotonic["lgd"],
            "monotonic_ccf":       monotonic["ccf"],
            "monotonic_haircut":   monotonic["haircut"],
            "scenario_narratives": DOWNTURN_SCENARIO_NARRATIVE,
        }

    # ------------------------------------------------------------------
    # Section 5 — Methodology Notes
    # ------------------------------------------------------------------

    def _build_methodology_notes(self) -> dict[str, Any]:
        return {
            "intro":                        METHODOLOGY_NOTES["intro"],
            "structural_vs_current_state":  METHODOLOGY_NOTES["structural_vs_current_state"],
            "construction_review":          METHODOLOGY_NOTES["construction_review"],
            "options_considered":           METHODOLOGY_NOTES["options_considered"],
            "path_taken":                   METHODOLOGY_NOTES["path_taken"],
            "property_proxy_caveat":        METHODOLOGY_NOTES["property_proxy_caveat"],
            "upstream_project":             "industry-analysis (v1 contracts)",
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _interpret_macro_regime(
    flag: Any, cash_regime: Any, arrears_env: Any, arrears_trend: Any,
) -> str:
    """Plain-English read of the macro flag for the exec summary narrative."""
    flag_str = str(flag).lower() if flag is not None else ""
    if flag_str == "base":
        return (
            "Benign macro backdrop — current-state signals support the base-scenario "
            "overlay. No automatic uplift applied to stressed pricing inputs this cycle."
        )
    if flag_str == "mild":
        return (
            "Early-stress signals present — favour the mild downturn overlay as the "
            "conservative-case calibration anchor."
        )
    if flag_str == "moderate":
        return (
            "Stressed environment — moderate overlay is the primary stressed-scenario "
            "input; expect material lift in PD/LGD multipliers applied downstream."
        )
    if flag_str == "severe":
        return (
            "Tail-risk environment — severe overlay should be treated as the active "
            "scenario; engage committee before proceeding with calibration."
        )
    return (
        f"Macro regime flag '{flag}' not in the expected "
        "{base, mild, moderate, severe} vocabulary — verify upstream contract "
        "before interpreting."
    )


def _check_monotonicity(rows: list[dict[str, Any]]) -> dict[str, bool]:
    """Assert base <= mild <= moderate <= severe on each multiplier column."""
    order = {"base": 0, "mild": 1, "moderate": 2, "severe": 3}
    ordered = sorted(rows, key=lambda r: order.get(r["scenario"], 99))
    def _mono(series: list[float]) -> bool:
        return all(a <= b for a, b in zip(series, series[1:]))
    return {
        "pd":      _mono([r["pd_multiplier"] for r in ordered]),
        "lgd":     _mono([r["lgd_multiplier"] for r in ordered]),
        "ccf":     _mono([r["ccf_multiplier"] for r in ordered]),
        "haircut": _mono([r["property_value_haircut"] for r in ordered]),
    }
