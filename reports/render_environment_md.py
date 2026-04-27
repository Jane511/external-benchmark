"""Markdown renderer for Report 2. Two variants:

    render_board_markdown       — Board / ExCo audience. Trimmed tables, no
                                  per-industry macro splits, no monotonicity
                                  audit, construction caveat short-form.
    render_technical_markdown   — MRC / validation audience. Full tables,
                                  commentary, full Construction write-up,
                                  monotonicity check, freshness findings.

Both variants are deterministic (no datetimes in row ordering, all sorts
stable) so diffs in git review show only real data changes.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable


def render_board_markdown(data: dict[str, Any], path: Path | str) -> Path:
    text = _render_board(data)
    path = Path(path)
    path.write_text(text, encoding="utf-8")
    return path


def render_technical_markdown(data: dict[str, Any], path: Path | str) -> Path:
    text = _render_technical(data)
    path = Path(path)
    path.write_text(text, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Board variant
# ---------------------------------------------------------------------------


def _render_board(data: dict[str, Any]) -> str:
    meta = data["meta"]
    exec_ = data["executive_summary"]
    iro = data["industry_risk_outlook"]
    pmo = data["property_market_outlook"]
    dwn = data["downturn_scenarios"]
    mn = data["methodology_notes"]

    out: list[str] = []
    out.append(f"# {meta['report_title']} — Board Summary\n")
    out.append(f"_{meta['report_subtitle']} · Generated {meta['generated_at'][:10]}"
               + (f" · Data as-of {meta['data_as_of']}" if meta.get('data_as_of') else "")
               + "_\n")

    # 1. Executive Summary
    out.append("## 1. Executive Summary\n")
    out.append(exec_["regime_interpretation"])
    out.append("")
    for line in exec_["bullets"]:
        out.append(f"- {line}")
    out.append("")
    out.append(
        f"> **Flagship read.** Macro regime is **{exec_['regime_flag']}**. "
        f"{exec_['elevated_industry_count']} of {exec_['industry_count']} "
        "industries sit at 'Elevated' base risk.\n"
    )

    # 2. Industry Risk Outlook — board version trims columns (no classification/macro split).
    out.append("## 2. Industry Risk Outlook\n")
    out.append(
        f"All {len(iro['rows'])} ANZSIC divisions ranked by structural base "
        f"risk score. Macro conditioner: cash rate {iro['cash_rate_latest']:.2f}% "
        f"({iro['cash_rate_change_1y']:+.2f} pp YoY).\n"
    )
    out.append(_md_table(
        ["Rank", "Industry", "Base score", "Level"],
        [[r["rank"], r["industry"], f"{r['base_risk']:.2f}", r["level"]]
         for r in iro["rows"]],
    ))
    out.append("**Top-3 drivers**\n")
    for bullet in iro["top3_commentary"]:
        out.append(f"- {bullet}")
    out.append("")
    out.append(f"> **Methodology note.** {iro['construction_caveat']}\n")

    # 3. Property Market Outlook — board version: one grouped table
    out.append("## 3. Property Market Outlook\n")
    out.append(
        f"{pmo['total_segments']} property segments grouped by cycle stage.\n"
    )
    for group in pmo["groups"]:
        out.append(f"### {group['stage_display']} ({len(group['rows'])} segments)\n")
        out.append(f"_{group['narrative']}_\n")
        out.append(_md_table(
            ["Segment", "Softness", "Band", "Approvals Δ%"],
            [[r["property_segment"], f"{r['softness_score']:.2f}",
              r["softness_band"], f"{r['approvals_change_pct']:+.1f}"]
             for r in group["rows"]],
        ))
    for bullet in pmo["commentary"][:2]:     # drop the proxy caveat — moved to §5
        out.append(f"- {bullet}")
    out.append("")

    # 4. Downturn Scenarios
    out.append("## 4. Downturn Scenario Overlays\n")
    out.append(_md_table(
        ["Scenario", "PD ×", "LGD ×", "CCF ×", "Property haircut"],
        [[r["scenario_display"],
          f"{r['pd_multiplier']:.2f}",
          f"{r['lgd_multiplier']:.2f}",
          f"{r['ccf_multiplier']:.2f}",
          f"{r['property_value_haircut']:.2f}"]
         for r in dwn["rows"]],
    ))
    out.append("**Interpretation**\n")
    for r in dwn["rows"]:
        out.append(f"- **{r['scenario_display']}** — {r['interpretation']}")
    out.append("")

    # 5. Methodology Notes — board version: short-form Construction callout
    out.append("## 5. Methodology Notes\n")
    out.append(mn["intro"])
    out.append("")
    out.append("**Structural vs current-state risk**")
    out.append(mn["structural_vs_current_state"])
    out.append("")
    out.append(f"> {mn['construction_review']}")
    out.append("")
    out.append(f"**Path taken.** {mn['path_taken']}")
    out.append("")
    out.append(f"_See Technical variant for the full methodology write-up and the three options considered upstream._")
    out.append("")

    out.append(f"_Generated by External Benchmark Engine — Report 2 (Board variant). Upstream: {mn['upstream_project']}._")
    return "\n".join(out)


# ---------------------------------------------------------------------------
# Technical variant
# ---------------------------------------------------------------------------


def _render_technical(data: dict[str, Any]) -> str:
    meta = data["meta"]
    exec_ = data["executive_summary"]
    iro = data["industry_risk_outlook"]
    pmo = data["property_market_outlook"]
    dwn = data["downturn_scenarios"]
    mn = data["methodology_notes"]

    out: list[str] = []
    out.append(f"# {meta['report_title']} — Technical Appendix\n")
    out.append(f"_{meta['report_subtitle']} · Generated {meta['generated_at'][:10]}"
               + (f" · Data as-of {meta['data_as_of']}" if meta.get('data_as_of') else "")
               + "_\n")

    # Data provenance block — only in technical variant.
    out.append("## Data provenance\n")
    out.append(f"- **Source project:** {mn['upstream_project']}")
    out.append(f"- **Data directory:** `{meta.get('data_dir') or '(unset)'}`")
    out.append(f"- **Data as-of:** {meta.get('data_as_of') or '(not present)'}")
    out.append(f"- **Frames loaded:** {', '.join(meta.get('loaded_frames', []))}")
    if meta.get("freshness_findings"):
        out.append("")
        out.append("**Freshness check:**\n")
        out.append(_md_table(
            ["Contract", "Age (days)", "Stale?"],
            [[f["name"], f"{f['age_days']:.1f}", "YES" if f["is_stale"] else "no"]
             for f in meta["freshness_findings"]],
        ))
    out.append("")

    # 1. Executive Summary
    out.append("## 1. Executive Summary\n")
    out.append(exec_["regime_interpretation"])
    out.append("")
    for line in exec_["bullets"]:
        out.append(f"- {line}")
    out.append("")

    # 2. Industry Risk Outlook — full columns
    out.append("## 2. Industry Risk Outlook\n")
    out.append(
        f"All {len(iro['rows'])} ANZSIC divisions ordered descending by "
        f"`industry_base_risk_score`. Base score is a weighted blend of "
        f"`classification_risk_score` (structural: cyclicality, concentration) "
        f"and `macro_risk_score` (cash-rate regime + one-year change). "
        f"Macro conditioner this cycle: cash rate "
        f"{iro['cash_rate_latest']:.2f}% ({iro['cash_rate_change_1y']:+.2f} pp over 1 year). "
        "Every industry row shares the same macro component by design.\n"
    )
    out.append(_md_table(
        ["Rank", "Industry", "Classification", "Macro", "Base", "Level"],
        [[r["rank"], r["industry"],
          f"{r['classification_risk']:.2f}",
          f"{r['macro_risk']:.2f}",
          f"{r['base_risk']:.2f}",
          r["level"]]
         for r in iro["rows"]],
    ))
    out.append("### Top-3 drivers\n")
    for bullet in iro["top3_commentary"]:
        out.append(f"- {bullet}")
    out.append("")
    out.append(f"> **Construction caveat.** {iro['construction_caveat']}\n")

    # 3. Property Market Outlook — full columns per stage
    out.append("## 3. Property Market Outlook\n")
    out.append(
        f"{pmo['total_segments']} property segments grouped by cycle stage "
        "(Downturn → Slowing → Neutral → Growth); within each stage ordered "
        "by softness desc.\n"
    )
    for group in pmo["groups"]:
        out.append(f"### {group['stage_display']} ({len(group['rows'])} segments)\n")
        out.append(f"_{group['narrative']}_\n")
        out.append(_md_table(
            ["Segment", "Softness", "Band", "Region risk", "Region band", "Approvals Δ%"],
            [[r["property_segment"], f"{r['softness_score']:.2f}",
              r["softness_band"], f"{r['region_risk_score']:.2f}",
              r["region_risk_band"], f"{r['approvals_change_pct']:+.1f}"]
             for r in group["rows"]],
        ))
    out.append("### Concentration and tailwinds\n")
    for bullet in pmo["commentary"]:
        out.append(f"- {bullet}")
    out.append("")

    # 4. Downturn scenarios — full columns + monotonicity audit
    out.append("## 4. Downturn Scenario Overlays\n")
    out.append(_md_table(
        ["Scenario", "PD ×", "LGD ×", "CCF ×", "Property haircut", "Notes", "As-of"],
        [[r["scenario_display"],
          f"{r['pd_multiplier']:.2f}",
          f"{r['lgd_multiplier']:.2f}",
          f"{r['ccf_multiplier']:.2f}",
          f"{r['property_value_haircut']:.2f}",
          _clean(r["notes"]),
          r["as_of_date"]]
         for r in dwn["rows"]],
    ))
    out.append("### Interpretation\n")
    for r in dwn["rows"]:
        out.append(f"- **{r['scenario_display']}** — {r['interpretation']}")
    out.append("")
    mono_ok = all([dwn["monotonic_pd"], dwn["monotonic_lgd"],
                   dwn["monotonic_ccf"], dwn["monotonic_haircut"]])
    out.append("### Monotonicity audit\n")
    if mono_ok:
        out.append("- PASS. PD, LGD, CCF, and property haircut all increase "
                   "monotonically base → severe. Contract invariant held.")
    else:
        out.append("- **FAIL.** One or more multiplier columns is not monotonic "
                   "across base → severe. Investigate upstream contract.")
    out.append("")

    # 5. Methodology Notes — full text, all four blocks
    out.append("## 5. Methodology Notes\n")
    out.append(mn["intro"])
    out.append("")
    out.append("### Structural vs current-state\n")
    out.append(mn["structural_vs_current_state"])
    out.append("")
    out.append("### Construction review item\n")
    out.append(f"> {mn['construction_review']}")
    out.append("")
    out.append("### Options considered upstream\n")
    out.append(mn["options_considered"])
    out.append("")
    out.append("### Path taken by this report\n")
    out.append(f"> {mn['path_taken']}")
    out.append("")
    out.append("### Property-signal proxy caveat\n")
    out.append(mn["property_proxy_caveat"])
    out.append("")

    out.append(f"---\n_Generated by External Benchmark Engine — Report 2 (Technical variant). Upstream: {mn['upstream_project']}._")
    return "\n".join(out)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _md_table(headers: list[str], rows: Iterable[list[Any]]) -> str:
    out = ["| " + " | ".join(_clean(h) for h in headers) + " |"]
    out.append("|" + "|".join("---" for _ in headers) + "|")
    for row in rows:
        out.append("| " + " | ".join(_clean(c) for c in row) + " |")
    return "\n".join(out) + "\n"


def _clean(val: Any) -> str:
    """Keep markdown tables valid — escape pipes and flatten newlines."""
    s = "" if val is None else str(val)
    return s.replace("|", "\\|").replace("\n", " ").strip()
