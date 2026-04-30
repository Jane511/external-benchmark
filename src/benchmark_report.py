"""Report 1 — External Benchmark RAW Observation Summary.

After the Brief 1 refactor the engine publishes raw, source-attributable
observations only. No definition alignment, institution adjustments, LGD
overlays, or cross-source triangulation. This report reflects that.

Section structure:
    1. Executive summary (count of sources, segments, vintages)
    2. Per-source raw observations by segment
    3. Cross-source validation summary (spread, outliers, vintage)
    4. Big 4 vs non-bank disclosure spread (informational only)
    5. Provenance & methodology footnotes

A prominent banner near the top makes the contract explicit: this is a
RAW report. Adjustments — definition alignment, selection bias, downturn
overlays — are applied by the consuming project (PD workbook for PD,
LGD project for LGD, etc.).

Three output formats:
    to_markdown   committee-friendly; git-reviewable (default)
    to_html       single self-contained file, inline CSS, no JS
    to_docx       optional, requires python-docx (extras: reports)
"""
from __future__ import annotations

import html
import json
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

from src.observations import ObservationSet, PeerObservations
from src.registry import BenchmarkRegistry
from src.segment_glossary import SEGMENT_GLOSSARY
from src.source_naming import (
    ACRONYM_GLOSSARY,
    cohort_label,
    friendly_name,
    parameter_label,
    segment_label,
)
from src.trend import build_segment_trends
from src.validation import (
    BIG4_SOURCE_IDS,
    PEER_RATIO_DEFINITION,
    ValidationFlags,
    is_big4_source_id,
)


RAW_ONLY_BANNER = (
    "**The engine publishes raw, source-attributable observations only.** "
    "No adjustments — definition alignment, selection bias, downturn overlays — "
    "are applied. These have moved to consuming projects (PD workbook for PD, "
    "LGD project for LGD, etc.) so each use case can manage its own complete "
    "adjustment chain. Consumers of this report apply their own adjustments per "
    "their model documentation."
)


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class BenchmarkCalibrationReport:
    """Compose the raw-only sections from a registry + observations API.

    Constructor signature is intentionally narrow — the report depends on
    the registry and the PeerObservations facade, nothing else. The
    engine publishes raw observations only; downstream consumers own
    every adjustment / triangulation / calibration decision.
    """

    def __init__(
        self,
        registry: BenchmarkRegistry,
        peer_observations: Optional[PeerObservations] = None,
        *,
        period_label: Optional[str] = None,
        raw_data_dir: Optional[Path | str] = None,
        refresh_schedules: Optional[dict[str, int]] = None,
        refresh_pipeline_quiet: bool = False,
    ) -> None:
        self._registry = registry
        self._peer = peer_observations or PeerObservations(
            registry,
            refresh_schedules=refresh_schedules,
            refresh_pipeline_quiet=refresh_pipeline_quiet,
        )
        self._period = period_label or self._default_period_label()
        # Section 6 (raw-data inventory) walks this directory. ``None``
        # disables the section entirely (used by unit-test fixtures that
        # don't stage any raw files).
        self._raw_data_dir: Optional[Path] = (
            Path(raw_data_dir) if raw_data_dir is not None else None
        )
        self._refresh_schedules = refresh_schedules
        self._refresh_pipeline_quiet = refresh_pipeline_quiet

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(self) -> dict[str, Any]:
        """Return a structured dict with all sections — used by tests + renderers."""
        observation_sets = self._collect_observation_sets()
        segment_trend = self._build_segment_trend()
        out: dict[str, Any] = {
            "meta":                    self._build_meta(observation_sets),
            "banner":                  RAW_ONLY_BANNER,
            "acronym_glossary":        self._build_acronym_glossary(),
            "segment_glossary":        self._build_segment_glossary(
                observation_sets, segment_trend,
            ),
            "executive_summary":       self._build_executive_summary(observation_sets),
            "pd_overview":             self._build_pd_overview(observation_sets),
            "per_source_observations": self._build_per_source_observations(observation_sets),
            "validation_summary":      self._build_validation_summary(observation_sets),
            "big4_vs_nonbank_spread":  self._build_big4_vs_nonbank_spread(observation_sets),
            "provenance":              self._build_provenance(observation_sets),
            "segment_trend":           segment_trend,
        }
        if self._raw_data_dir is not None:
            out["supporting_documentation"] = self._build_source_documentation()
            out["raw_data_inventory"] = self._build_raw_data_inventory()
        return out

    def to_markdown(self) -> str:
        """Render committee-friendly Markdown."""
        data = self.generate()
        lines: list[str] = []
        meta = data["meta"]

        lines.append(f"# External Benchmark Report — {self._period}")
        lines.append(
            f"_Generated: {meta['generated_at']} · Data as-of: "
            f"{_fmt_data_as_of(meta['data_as_of_min'], meta['data_as_of_max'])}_"
        )
        lines.append("")
        lines.append(f"> {data['banner']}")
        lines.append("")

        # 0a. Acronym glossary — short, always rendered.
        acronym_glossary = data.get("acronym_glossary") or []
        if acronym_glossary:
            lines.append("## 0a. Glossary of terms")
            lines.append("")
            lines.append("| Term | Meaning |")
            lines.append("| --- | --- |")
            for row in acronym_glossary:
                lines.append(f"| {row['term']} | {row['definition']} |")
            lines.append("")

        glossary = data["segment_glossary"]
        if glossary:
            lines.append("## 0b. Segment definitions")
            lines.append("")
            lines.append("| Segment | Definition |")
            lines.append("| --- | --- |")
            for row in glossary:
                lines.append(f"| {row['segment']} | {row['definition']} |")
            lines.append("")

        # 1
        lines.append("## 1. Executive Summary")
        narrative = data["executive_summary"].get("narrative", "")
        if narrative:
            lines.append(narrative)
            lines.append("")
        for bullet in data["executive_summary"]["lines"]:
            lines.append(f"- {bullet}")
        lines.append("")

        # 2 — restructured: per segment, per parameter, with cohort
        # medians under each parameter table. Latest value + median over
        # all available vintages per source. Friendly publisher names.
        lines.append("## 2. Latest figures by segment and metric")
        lines.append("")

        # 2a — PD overview at a glance. One row per segment with the
        # peer median PD or "—" if no peer has published a PD for it,
        # plus a list of what *other* metrics that segment has so the
        # reader doesn't think the data is missing — it just isn't a PD.
        pd_overview = data.get("pd_overview") or []
        if pd_overview:
            lines.append("### 2a. PD overview by segment")
            lines.append("")
            lines.append(
                "_Quick guide to where peer PDs exist. Segments with "
                "\"—\" in both PD columns are not published as a PD by "
                "any peer — typically because the segment is a "
                "system-wide aggregate (regulator NPL / arrears) or a "
                "specialist book where peers publish loss expense / "
                "realised loss instead. The full breakout is in the "
                "per-metric tables below._"
            )
            lines.append("")
            lines.append("| Segment | Big 4 median PD | Big 4 N | Non-bank peer median PD | Non-bank N | Other metrics in this segment |")
            lines.append("| --- | ---:| ---:| ---:| ---:| --- |")
            for row in pd_overview:
                lines.append(
                    f"| {row['segment_label']} "
                    f"| {_fmt_pct_value(row['big4_median_pd'])} "
                    f"| {row['big4_count']} "
                    f"| {_fmt_pct_value(row['nonbank_median_pd'])} "
                    f"| {row['nonbank_count']} "
                    f"| {', '.join(row['other_metrics']) or '—'} |"
                )
            lines.append("")

        lines.append(
            "_The breakouts below show every source's latest figure plus "
            "the median across every vintage we have for that source. "
            "Within a metric, peer banks come first, then non-bank peers, "
            "then references (regulators, rating agencies, regulatory "
            "floors). A **Cohort medians** sub-table follows each metric._"
        )
        lines.append("")
        if not data["per_source_observations"]:
            lines.append("_No observations recorded._")
        for seg_block in data["per_source_observations"]:
            lines.append(f"### {seg_block['segment_label']}")
            lines.append("")
            for param_block in seg_block["by_parameter"]:
                lines.append(f"#### {param_block['label']}")
                lines.append("")
                lines.append("| Cohort | Source | Latest | As-of | Median (all vintages) | # Vintages |")
                lines.append("| --- | --- | ---:| --- | ---:| ---:|")
                last_cohort: str | None = None
                for r in param_block["rows"]:
                    cohort_cell = (
                        r["cohort_label"] if r["cohort"] != last_cohort else ""
                    )
                    last_cohort = r["cohort"]
                    lines.append(
                        f"| {cohort_cell} "
                        f"| {r['friendly_name']} "
                        f"| {_fmt_observation_value(r['latest_value'])} "
                        f"| {r['latest_as_of']} "
                        f"| {_fmt_observation_value(r['median_value'])} "
                        f"| {r['n_vintages']} |"
                    )
                lines.append("")
                medians = param_block["cohort_medians"]
                if medians:
                    lines.append("_Cohort medians (across the latest value of every source):_")
                    lines.append("")
                    lines.append("| Cohort | N sources | Median of latest |")
                    lines.append("| --- | ---:| ---:|")
                    for m in medians:
                        lines.append(
                            f"| {m['cohort_label']} | {m['n']} | "
                            f"{_fmt_observation_value(m['median'])} |"
                        )
                    lines.append("")
        lines.append("")

        # 3
        lines.append("## 3. Cross-source validation summary")
        lines.append("")
        # If any segment carries a frozen-dataset banner, surface it once
        # at the top of the table and suppress the per-row stale list
        # (already blank by contract when the banner is set).
        banner_text = next(
            (v.get("frozen_dataset_banner") for v in data["validation_summary"]
             if v.get("frozen_dataset_banner")),
            "",
        )
        if banner_text:
            lines.append(f"_{banner_text}_")
            lines.append("")
        lines.append("| Segment | N | Spread % | Big 4 spread % | Peer Big 4/Non-bank ratio | Outliers | Stale sources |")
        lines.append("| --- | ---:| ---:| ---:| ---:| --- | --- |")
        for vrow in data["validation_summary"]:
            outliers = ", ".join(friendly_name(s) for s in vrow["outlier_sources"]) or "-"
            stales = ", ".join(friendly_name(s) for s in vrow["stale_sources"]) or "-"
            lines.append(
                f"| {segment_label(vrow['segment'])} "
                f"| {vrow['n_sources']} "
                f"| {_fmt_pct(vrow['spread_pct'])} "
                f"| {_fmt_pct(vrow['big4_spread_pct'])} "
                f"| {_fmt_ratio(vrow['peer_big4_vs_non_bank_ratio'])} "
                f"| {outliers} "
                f"| {stales} |"
            )
        lines.append("")

        # 4
        lines.append("## 4. Big 4 vs non-bank disclosure spread (informational only)")
        lines.append("")
        lines.append(
            "_The values below are raw published figures from the two peer "
            "cohorts. The engine does NOT recommend any uplift or adjustment "
            "from this spread. Consuming projects decide how (or whether) to "
            "use it._"
        )
        lines.append("")
        lines.append(f"_{PEER_RATIO_DEFINITION}_")
        lines.append("")
        lines.append("| Segment | Big 4 median | Non-bank median | Ratio | Big 4 N | Non-bank N |")
        lines.append("| --- | ---:| ---:| ---:| ---:| ---:|")
        for srow in data["big4_vs_nonbank_spread"]:
            lines.append(
                f"| {segment_label(srow['segment'])} "
                f"| {_fmt_pct_value(srow['big4_median'])} "
                f"| {_fmt_pct_value(srow['nonbank_median'])} "
                f"| {_fmt_ratio(srow['ratio'])} "
                f"| {srow['big4_count']} "
                f"| {srow['nonbank_count']} |"
            )
        lines.append("")
        # (Reference anchors are surfaced inside Section 2 already, under
        # the regulator / rating-agency / regulatory-floor cohort rows.
        # The duplicate "4a. Reference anchors" table has been dropped.)

        # 5
        lines.append("## 5. Provenance & methodology footnotes")
        for prov in data["provenance"]:
            lines.append(
                f"- **{friendly_name(prov['source_id'])}** "
                f"({prov['source_type']}): {prov['reporting_basis']} — "
                f"{prov['source_url'] or 'n/a'}"
            )
        lines.append("")

        if data.get("supporting_documentation"):
            docs = data["supporting_documentation"]
            lines.append("## Supporting documentation")
            lines.append("")
            if docs["lead_text"]:
                lines.append(docs["lead_text"])
                lines.append("")
            lines.append("| Source | Publisher | Period | Cached file | Retrieved | URL |")
            lines.append("| --- | --- | --- | --- | --- | --- |")
            for row in docs["documents"]:
                lines.append(
                    f"| {row['source']} "
                    f"| {row['publisher']} "
                    f"| {row['period']} "
                    f"| {row['local_cached_file']} "
                    f"| {row['retrieval_date']} "
                    f"| {row['url']} |"
                )
            lines.append("")

            commentary = docs.get("recent_regulator_commentary") or _empty_regulator_commentary()
            lines.append("### Recent regulator commentary")
            lines.append("")
            if commentary.get("empty_message"):
                lines.append(commentary["empty_message"])
                lines.append("")
            else:
                for label, key in (
                    ("APRA Insight", "apra_insight"),
                    ("CFR publications", "cfr_publications"),
                ):
                    items = commentary.get(key) or []
                    lines.append(f"**{label}**")
                    lines.append("")
                    if not items:
                        lines.append("- _none captured_")
                    else:
                        for item in items:
                            stamp = item.get("published_date") or "????-??-??"
                            title = item.get("title") or "(untitled)"
                            lines.append(f"- {stamp} — {title}")
                    lines.append("")

        # 6 — Raw-data inventory (only when raw_data_dir was provided)
        if "raw_data_inventory" in data:
            inv = data["raw_data_inventory"]
            lines.append("## 6. Raw data inventory")
            lines.append("")
            lines.append(
                f"_Walk of `{inv['root']}` — {inv['n_files']} file(s) staged "
                f"across {len(inv['by_family'])} source families. Includes "
                "`_MANUAL.md` / `*_GATE.md` notes for sources that require "
                "manual download._"
            )
            lines.append("")
            if not inv["by_family"]:
                lines.append("_No raw data files staged on disk._")
            for family_block in inv["by_family"]:
                lines.append(f"### {family_block['family']}")
                lines.append("")
                lines.append("| File | Subfolder | Kind | Size | Modified (UTC) |")
                lines.append("| --- | --- | --- | ---:| --- |")
                for f in family_block["files"]:
                    lines.append(
                        f"| {f['filename']} "
                        f"| {f['subfamily'] or '-'} "
                        f"| {f['kind']} "
                        f"| {f['size_bytes']:,} "
                        f"| {f['modified_utc']} |"
                    )
                lines.append("")
            lines.append("")

        # 7 — Trend vs prior cycle
        if data["segment_trend"]:
            lines.append("## 7. Trend vs prior cycle")
            lines.append("")
            lines.append(
                "_Rows appear only where the same source has at least two "
                "raw-observation vintages for the same segment and parameter._"
            )
            lines.append("")
            lines.append("| Segment | Metric | Source | Current | Current as-of | Prior | Prior as-of | Delta | % change |")
            lines.append("| --- | --- | --- | ---:| --- | ---:| --- | ---:| ---:|")
            for row in data["segment_trend"]:
                lines.append(
                    f"| {segment_label(row['segment'])} "
                    f"| {parameter_label(row['parameter'])} "
                    f"| {friendly_name(row['source_id'])} "
                    f"| {_fmt_pct_value(row['current_value'])} "
                    f"| {row['current_as_of']} "
                    f"| {_fmt_pct_value(row['prior_value'])} "
                    f"| {row['prior_as_of']} "
                    f"| {_fmt_signed_pct_value(row['delta'])} "
                    f"| {_fmt_pct(row['pct_change'])} |"
                )
            lines.append("")

        return "\n".join(lines)

    def to_html(self) -> str:
        """Single self-contained HTML file, inline CSS, no JS."""
        md = self.to_markdown()
        body = _markdown_to_basic_html(md)
        return (
            "<!doctype html><html><head><meta charset='utf-8'>"
            f"<title>External Benchmark Report — {html.escape(self._period)}</title>"
            "<style>"
            "body{font-family:-apple-system,Segoe UI,Helvetica,sans-serif;"
            "max-width:1100px;margin:2em auto;padding:0 1em;color:#222;}"
            "table{border-collapse:collapse;margin:1em 0;}"
            "th,td{border:1px solid #ccc;padding:6px 10px;text-align:left;}"
            "th{background:#f5f5f5;}"
            ".banner{background:#fff7d6;border-left:4px solid #d4a900;"
            "padding:12px;margin:1em 0;}"
            "</style></head><body>"
            f"{body}"
            "</body></html>"
        )

    def to_docx(self, path: Path | str) -> Path:
        """Render to Word via python-docx (optional dependency)."""
        try:
            from src.docx_helpers import (
                new_document, add_heading, add_paragraph, add_table, add_bullet,
            )
        except ImportError as exc:
            raise ImportError(
                "to_docx requires the 'reports' extras (python-docx). "
                "Install with: pip install -e .[reports]"
            ) from exc

        data = self.generate()
        title = f"External Benchmark Report — {self._period}"
        subtitle = (
            f"Generated {data['meta']['generated_at']} | Data as-of "
            f"{_fmt_data_as_of(data['meta']['data_as_of_min'], data['meta']['data_as_of_max'])}"
        )
        doc = new_document(title, subtitle=subtitle)

        add_paragraph(doc, data["banner"], italic=True)

        acronym_glossary = data.get("acronym_glossary") or []
        if acronym_glossary:
            add_heading(doc, "0a. Glossary of terms", level=2)
            add_table(
                doc,
                headers=["term", "meaning"],
                rows=[[row["term"], row["definition"]] for row in acronym_glossary],
            )

        glossary = data["segment_glossary"]
        if glossary:
            add_heading(doc, "0b. Segment definitions", level=2)
            add_table(
                doc,
                headers=["segment", "definition"],
                rows=[[row["segment"], row["definition"]] for row in glossary],
            )

        add_heading(doc, "1. Executive Summary", level=2)
        narrative = data["executive_summary"].get("narrative", "")
        if narrative:
            add_paragraph(doc, narrative)
        for line in data["executive_summary"]["lines"]:
            add_bullet(doc, line)

        add_heading(doc, "2. Latest figures by segment and metric", level=2)

        pd_overview = data.get("pd_overview") or []
        if pd_overview:
            add_heading(doc, "2a. PD overview by segment", level=3)
            add_paragraph(
                doc,
                "Quick guide to where peer PDs exist. Segments with em-"
                "dash in both PD columns are not published as a PD by any "
                "peer — typically because the segment is a system-wide "
                "aggregate or a specialist book where peers publish loss "
                "expense / realised loss instead.",
                italic=True,
            )
            add_table(
                doc,
                headers=["segment", "big4_median_pd", "big4_n",
                         "nonbank_median_pd", "nonbank_n", "other_metrics"],
                rows=[
                    [row["segment_label"],
                     _fmt_pct_value(row["big4_median_pd"]),
                     str(row["big4_count"]),
                     _fmt_pct_value(row["nonbank_median_pd"]),
                     str(row["nonbank_count"]),
                     ", ".join(row["other_metrics"]) or "—"]
                    for row in pd_overview
                ],
            )

        add_paragraph(
            doc,
            "The breakouts below show every source's latest figure plus "
            "the median across every vintage. Within a metric, peer banks "
            "come first, then non-bank peers, then references.",
            italic=True,
        )
        for seg_block in data["per_source_observations"]:
            add_heading(doc, seg_block["segment_label"], level=3)
            for param_block in seg_block["by_parameter"]:
                add_heading(doc, param_block["label"], level=4)
                add_table(
                    doc,
                    headers=["cohort", "source", "latest", "as_of",
                             "median", "n_vintages"],
                    rows=[
                        [r["cohort_label"], r["friendly_name"],
                         _fmt_observation_value(r["latest_value"]),
                         r["latest_as_of"],
                         _fmt_observation_value(r["median_value"]),
                         str(r["n_vintages"])]
                        for r in param_block["rows"]
                    ],
                )
                medians = param_block["cohort_medians"]
                if medians:
                    add_paragraph(doc, "Cohort medians (across latest values):", italic=True)
                    add_table(
                        doc,
                        headers=["cohort", "n_sources", "median_of_latest"],
                        rows=[
                            [m["cohort_label"], str(m["n"]),
                             _fmt_observation_value(m["median"])]
                            for m in medians
                        ],
                    )

        add_heading(doc, "3. Cross-source validation summary", level=2)
        banner_text = next(
            (v.get("frozen_dataset_banner") for v in data["validation_summary"]
             if v.get("frozen_dataset_banner")),
            "",
        )
        if banner_text:
            add_paragraph(doc, banner_text, italic=True)
        add_table(
            doc,
            headers=["segment", "n_sources", "spread_pct", "big4_spread_pct",
                     "peer_big4_vs_non_bank_ratio", "outliers", "stale"],
            rows=[
                [segment_label(v["segment"]), str(v["n_sources"]),
                 _fmt_pct(v["spread_pct"]),
                 _fmt_pct(v["big4_spread_pct"]),
                 _fmt_ratio(v["peer_big4_vs_non_bank_ratio"]),
                 ", ".join(friendly_name(s) for s in v["outlier_sources"]) or "-",
                 ", ".join(friendly_name(s) for s in v["stale_sources"]) or "-"]
                for v in data["validation_summary"]
            ],
        )

        add_heading(doc, "4. Big 4 vs non-bank disclosure spread (informational only)", level=2)
        add_paragraph(
            doc,
            "Values are raw published figures from each peer cohort. The "
            "engine does NOT recommend any uplift or adjustment from this "
            "spread.",
            italic=True,
        )
        add_paragraph(doc, PEER_RATIO_DEFINITION, italic=True)
        add_table(
            doc,
            headers=["segment", "big4_median", "nonbank_median", "ratio",
                     "big4_count", "nonbank_count"],
            rows=[
                [segment_label(s["segment"]),
                 _fmt_pct_value(s["big4_median"]),
                 _fmt_pct_value(s["nonbank_median"]),
                 _fmt_ratio(s["ratio"]),
                 str(s["big4_count"]), str(s["nonbank_count"])]
                for s in data["big4_vs_nonbank_spread"]
            ],
        )

        # Section 4a (reference anchors) intentionally not rendered in
        # docx for the same reason as the markdown: regulator / rating-
        # agency / regulatory-floor rows already appear in Section 2.

        add_heading(doc, "5. Provenance & methodology footnotes", level=2)
        for p in data["provenance"]:
            add_bullet(
                doc,
                f"{friendly_name(p['source_id'])} ({p['source_type']}): "
                f"{p['reporting_basis']} — {p['source_url'] or 'n/a'}",
            )

        if data.get("supporting_documentation"):
            docs = data["supporting_documentation"]
            add_heading(doc, "Supporting documentation", level=2)
            if docs["lead_text"]:
                add_paragraph(doc, docs["lead_text"])
            add_table(
                doc,
                headers=["source", "publisher", "period", "cached_file", "retrieved", "url"],
                rows=[
                    [
                        r["source"], r["publisher"], r["period"],
                        r["local_cached_file"], r["retrieval_date"], r["url"],
                    ]
                    for r in docs["documents"]
                ],
            )
            commentary = docs.get("recent_regulator_commentary") or _empty_regulator_commentary()
            add_heading(doc, "Recent regulator commentary", level=3)
            if commentary.get("empty_message"):
                add_paragraph(doc, commentary["empty_message"], italic=True)
            else:
                for label, key in (
                    ("APRA Insight", "apra_insight"),
                    ("CFR publications", "cfr_publications"),
                ):
                    items = commentary.get(key) or []
                    add_paragraph(doc, label, italic=True)
                    if not items:
                        add_bullet(doc, "(none captured)")
                    else:
                        for item in items:
                            stamp = item.get("published_date") or "????-??-??"
                            title = item.get("title") or "(untitled)"
                            add_bullet(doc, f"{stamp} — {title}")

        if "raw_data_inventory" in data:
            inv = data["raw_data_inventory"]
            add_heading(doc, "6. Raw data inventory", level=2)
            add_paragraph(
                doc,
                f"Walk of {inv['root']} — {inv['n_files']} file(s) staged "
                f"across {len(inv['by_family'])} source families. Includes "
                "_MANUAL.md / *_GATE.md notes for sources that require "
                "manual download.",
                italic=True,
            )
            for family_block in inv["by_family"]:
                add_heading(doc, family_block["family"], level=3)
                add_table(
                    doc,
                    headers=["filename", "subfolder", "kind",
                             "size_bytes", "modified_utc"],
                    rows=[
                        [f["filename"], f["subfamily"] or "-", f["kind"],
                         f"{f['size_bytes']:,}", f["modified_utc"]]
                        for f in family_block["files"]
                    ],
                )

        if data["segment_trend"]:
            add_heading(doc, "7. Trend vs prior cycle", level=2)
            add_paragraph(
                doc,
                "Rows appear only where the same source has at least two "
                "raw-observation vintages for the same segment and parameter.",
                italic=True,
            )
            add_table(
                doc,
                headers=[
                    "segment", "metric", "source", "current", "current_as_of",
                    "prior", "prior_as_of", "delta", "pct_change",
                ],
                rows=[
                    [
                        segment_label(row["segment"]),
                        parameter_label(row["parameter"]),
                        friendly_name(row["source_id"]),
                        _fmt_pct_value(row["current_value"]),
                        row["current_as_of"],
                        _fmt_pct_value(row["prior_value"]),
                        row["prior_as_of"],
                        _fmt_signed_pct_value(row["delta"]),
                        _fmt_pct(row["pct_change"]),
                    ]
                    for row in data["segment_trend"]
                ],
            )

        out = Path(path)
        doc.save(str(out))
        return out

    # ------------------------------------------------------------------
    # Section builders
    # ------------------------------------------------------------------

    def _collect_observation_sets(self) -> list[ObservationSet]:
        """One ObservationSet per canonical segment that has observations.

        Pre-P1.1, this defaulted to ``only_pd=True`` while the CSV exporter
        used ``only_pd=False``. The two outputs computed spreads / medians
        from different denominators (commentary rows leaked into the CSV).
        Both now use the same filter (everything) — commentary rows
        (value=None) are excluded from numerical aggregates by the
        cohort-aware validation logic, not by parameter filtering.
        """
        return [self._peer.for_segment(seg, only_pd=False)
                for seg in self._peer.all_segments()]

    def _build_meta(self, sets: list[ObservationSet]) -> dict[str, Any]:
        all_observations = self._registry.query_observations()
        total_obs = len(all_observations)
        sources = {o.source_id for o in all_observations}
        segments = {o.segment for o in all_observations}
        as_of_dates = sorted({o.as_of_date.isoformat() for o in all_observations})
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "period_label": self._period,
            "n_segments": len(segments),
            "n_observations": total_obs,
            "n_sources": len(sources),
            "data_as_of_min": as_of_dates[0] if as_of_dates else "",
            "data_as_of_max": as_of_dates[-1] if as_of_dates else "",
        }

    def _build_segment_glossary(
        self,
        sets: list[ObservationSet],
        segment_trend: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        rendered_segments = {
            s.segment for s in sets if s.observations
        }
        rendered_segments.update(row["segment"] for row in segment_trend)
        return [
            {
                "segment": segment,
                "definition": SEGMENT_GLOSSARY.get(
                    segment, "Definition pending for this canonical segment."
                ),
            }
            for segment in sorted(rendered_segments)
        ]

    def _build_acronym_glossary(self) -> list[dict[str, str]]:
        """Two-column acronym glossary, ordered as defined in source_naming."""
        return [{"term": term, "definition": definition}
                for term, definition in ACRONYM_GLOSSARY.items()]

    def _build_executive_summary(self, sets: list[ObservationSet]) -> dict[str, Any]:
        from src.models import Cohort, cohort_for

        all_observations = self._registry.query_observations()
        total_obs = len(all_observations)
        segments = {o.segment for o in all_observations}
        sources = {o.source_id for o in all_observations}
        # Cohort-correct counts: peer_big4 vs all-other-peers vs reference.
        cohorts_by_source: dict[str, Cohort] = {}
        for obs in all_observations:
            cohorts_by_source.setdefault(obs.source_id, cohort_for(obs.source_type, obs.source_id))
        big4 = {sid for sid, coh in cohorts_by_source.items() if coh is Cohort.PEER_BIG4}
        non_bank = {sid for sid, coh in cohorts_by_source.items() if coh is Cohort.PEER_NON_BANK}
        other_major = {sid for sid, coh in cohorts_by_source.items() if coh is Cohort.PEER_OTHER_MAJOR_BANK}
        reference = {
            sid for sid, coh in cohorts_by_source.items()
            if coh in (
                Cohort.REGULATOR_AGGREGATE, Cohort.RATING_AGENCY,
                Cohort.REGULATORY_FLOOR, Cohort.INDUSTRY_BODY,
            )
        }
        lines = [
            f"{total_obs} raw observations across {len(segments)} canonical segments.",
            f"{len(sources)} distinct sources: {len(big4)} Big 4 peers + "
            f"{len(other_major)} other major bank (Macquarie) + {len(non_bank)} "
            f"non-bank peers + {len(reference)} reference / regulator / "
            "rating-agency / industry-body.",
            "Every value in this report is the source-published raw figure. "
            "No multipliers, no triangulation, no adjustment.",
        ]
        return {
            "lines": lines,
            "narrative": self._build_executive_narrative(sets),
        }

    def _build_executive_narrative(self, sets: list[ObservationSet]) -> str:
        """Two-to-three sentence prose summary derived from segment data.

        Numbers are pulled from the observation set, not hard-coded, so the
        paragraph stays honest as the underlying data shifts.
        """
        # Headline segment: the one with the most peer sources, or
        # commercial_property if present (committee-relevant default).
        peer_sets = [
            s for s in sets if s.observations and s.validation_flags.n_sources > 0
        ]
        if not peer_sets:
            return "No peer observations are available for this period."
        headline = next(
            (s for s in peer_sets if s.segment == "commercial_property"),
            max(peer_sets, key=lambda s: s.validation_flags.n_sources),
        )
        big4_obs = headline.by_source_type(big4_only=True)
        big4_vals = sorted(
            o.value for o in big4_obs if o.value is not None
        )
        sentences: list[str] = []
        if big4_vals:
            seg_label = headline.segment.replace("_", " ")
            mid = big4_vals[len(big4_vals) // 2] if big4_vals else 0.0
            sentences.append(
                f"Big 4 peer {seg_label} values cluster between "
                f"{big4_vals[0] * 100:.2f}% and {big4_vals[-1] * 100:.2f}% "
                f"(median {mid * 100:.2f}%) across {len(big4_vals)} disclosures."
            )
        # Spread / outlier / coverage commentary.
        thin = [s.segment for s in peer_sets if s.validation_flags.n_sources < 2]
        if thin:
            sentences.append(
                "Coverage gap — segment(s) with fewer than two peer sources: "
                + ", ".join(sorted(thin)[:5])
                + (" (and more)" if len(thin) > 5 else "")
                + "."
            )
        else:
            sentences.append("Every segment has at least two peer sources.")
        outliers = [
            (s.segment, s.validation_flags.outlier_sources)
            for s in peer_sets if s.validation_flags.outlier_sources
        ]
        if outliers:
            seg, names = outliers[0]
            sentences.append(
                f"Outlier flags this cycle: {', '.join(names[:3])} on "
                f"{seg} — review for definition drift or data-quality issue."
            )
        return " ".join(sentences)

    def _build_per_source_observations(self, sets: list[ObservationSet]) -> list[dict[str, Any]]:
        """One block per segment, internally grouped by parameter.

        Each parameter group renders as one table per cohort (Big 4,
        Macquarie, non-bank peers, regulators, ...). For every source we
        show its latest value plus the median across its available
        vintages. We also compute a cohort-level median across the
        latest values so the committee can see a single "Big 4 median"
        / "Non-bank median" number per parameter without having to
        eyeball the rows.
        """
        from src.models import cohort_for

        out: list[dict[str, Any]] = []
        for s in sets:
            if not s.observations:
                continue

            # observation -> (parameter, cohort_value) bucket, then collapse
            # multi-vintage rows down to a single source-level row.
            by_param: dict[str, dict[str, list[Any]]] = {}
            for o in s.observations:
                bucket = by_param.setdefault(o.parameter, {})
                bucket.setdefault(o.source_id, []).append(o)

            param_blocks: list[dict[str, Any]] = []
            for parameter, sources in by_param.items():
                rows: list[dict[str, Any]] = []
                for source_id, obs_list in sources.items():
                    obs_sorted = sorted(obs_list, key=lambda o: o.as_of_date, reverse=True)
                    latest = obs_sorted[0]
                    numeric = [o.value for o in obs_sorted if o.value is not None]
                    median_value = _median(numeric) if numeric else None
                    rows.append({
                        "source_id": source_id,
                        "friendly_name": friendly_name(source_id),
                        "cohort": cohort_for(latest.source_type, source_id).value,
                        "cohort_label": cohort_label(
                            cohort_for(latest.source_type, source_id).value
                        ),
                        "source_type": latest.source_type.value,
                        "latest_value": latest.value,
                        "latest_as_of": latest.as_of_date.isoformat(),
                        "median_value": median_value,
                        "n_vintages": len(obs_list),
                        "reporting_basis": latest.reporting_basis,
                        "methodology_note": latest.methodology_note,
                        "page_or_table_ref": latest.page_or_table_ref,
                        "source_url": latest.source_url,
                    })

                # Cohort-level medians across the *latest* values.
                cohort_medians: list[dict[str, Any]] = []
                grouped: dict[str, list[float]] = {}
                for r in rows:
                    if r["latest_value"] is None:
                        continue
                    grouped.setdefault(r["cohort"], []).append(r["latest_value"])
                for cohort_value, values in grouped.items():
                    if not values:
                        continue
                    cohort_medians.append({
                        "cohort": cohort_value,
                        "cohort_label": cohort_label(cohort_value),
                        "median": _median(values),
                        "n": len(values),
                    })

                # Stable cohort order: peers first, then references.
                _COHORT_ORDER = {
                    "peer_big4": 0,
                    "peer_other_major_bank": 1,
                    "peer_non_bank": 2,
                    "regulator_aggregate": 3,
                    "rating_agency": 4,
                    "regulatory_floor": 5,
                    "industry_body": 6,
                }
                rows.sort(key=lambda r: (
                    _COHORT_ORDER.get(r["cohort"], 99),
                    r["friendly_name"].lower(),
                ))
                cohort_medians.sort(key=lambda c: _COHORT_ORDER.get(c["cohort"], 99))

                param_blocks.append({
                    "parameter": parameter,
                    "label": parameter_label(parameter),
                    "rows": rows,
                    "cohort_medians": cohort_medians,
                })

            # Stable parameter order so reports are deterministic.
            _PARAM_ORDER = {
                "pd": 0, "lgd": 1, "arrears": 2, "impaired": 3,
                "npl": 4, "loss_rate": 5, "commentary": 6,
            }
            param_blocks.sort(key=lambda b: _PARAM_ORDER.get(b["parameter"], 99))

            out.append({
                "segment": s.segment,
                "segment_label": segment_label(s.segment),
                "by_parameter": param_blocks,
            })
        return out

    def _build_validation_summary(self, sets: list[ObservationSet]) -> list[dict[str, Any]]:
        return [
            {
                "segment": s.segment,
                "n_sources": s.validation_flags.n_sources,
                "spread_pct": s.validation_flags.spread_pct,
                "big4_spread_pct": s.validation_flags.big4_spread_pct,
                "bank_vs_nonbank_ratio": s.validation_flags.bank_vs_nonbank_ratio,
                "peer_big4_vs_non_bank_ratio": (
                    s.validation_flags.peer_big4_vs_non_bank_ratio
                ),
                "outlier_sources": list(s.validation_flags.outlier_sources),
                "stale_sources": list(s.validation_flags.stale_sources),
                "frozen_dataset_banner": s.validation_flags.frozen_dataset_banner,
            }
            for s in sets if s.observations
        ]

    def _build_big4_vs_nonbank_spread(self, sets: list[ObservationSet]) -> list[dict[str, Any]]:
        from src.models import Cohort, cohort_for

        out: list[dict[str, Any]] = []
        for s in sets:
            big4_vals = [
                o.value for o in s.observations
                if o.value is not None
                and cohort_for(o.source_type, o.source_id) is Cohort.PEER_BIG4
            ]
            non_vals = [
                o.value for o in s.observations
                if o.value is not None
                and cohort_for(o.source_type, o.source_id) is Cohort.PEER_NON_BANK
            ]
            if not big4_vals and not non_vals:
                continue
            big4_med = _median(big4_vals) if big4_vals else None
            non_med = _median(non_vals) if non_vals else None
            ratio = (
                non_med / big4_med
                if (big4_med is not None and non_med is not None and big4_med > 0)
                else None
            )
            out.append({
                "segment": s.segment,
                "big4_median": big4_med,
                "nonbank_median": non_med,
                "ratio": ratio,
                "big4_count": len(big4_vals),
                "nonbank_count": len(non_vals),
            })
        return out

    def _build_pd_overview(
        self, sets: list[ObservationSet],
    ) -> list[dict[str, Any]]:
        """One-row-per-segment summary that fronts Section 2.

        For every segment in the report we list:
        - Big 4 median PD (or None if no Big 4 published a PD).
        - Non-bank peer median PD (or None).
        - The other metrics published for this segment, so the reader
          can see at a glance "this segment has no peer PD; here's
          what is published instead".

        The table answers the most common board question — "where are
        the PDs?" — and makes "no published PD for this segment"
        explicit instead of leaving the reader guessing.
        """
        from src.models import Cohort, cohort_for

        out: list[dict[str, Any]] = []
        for s in sets:
            big4_pds = [
                o.value for o in s.observations
                if o.value is not None and o.parameter == "pd"
                and cohort_for(o.source_type, o.source_id) is Cohort.PEER_BIG4
            ]
            nonbank_pds = [
                o.value for o in s.observations
                if o.value is not None and o.parameter == "pd"
                and cohort_for(o.source_type, o.source_id) is Cohort.PEER_NON_BANK
            ]
            other_params = sorted({
                o.parameter for o in s.observations if o.parameter != "pd"
            })
            out.append({
                "segment": s.segment,
                "segment_label": segment_label(s.segment),
                "big4_median_pd": _median(big4_pds) if big4_pds else None,
                "big4_count": len(big4_pds),
                "nonbank_median_pd": _median(nonbank_pds) if nonbank_pds else None,
                "nonbank_count": len(nonbank_pds),
                "other_metrics": [parameter_label(p) for p in other_params],
            })
        out.sort(key=lambda r: r["segment_label"].lower())
        return out

    def _build_provenance(self, sets: list[ObservationSet]) -> list[dict[str, Any]]:
        seen: dict[str, dict[str, Any]] = {}
        for s in sets:
            for o in s.observations:
                if o.source_id in seen:
                    continue
                seen[o.source_id] = {
                    "source_id": o.source_id,
                    "source_type": o.source_type.value,
                    "reporting_basis": o.reporting_basis,
                    "source_url": o.source_url,
                }
        return sorted(seen.values(), key=lambda x: x["source_id"])

    def _build_segment_trend(self) -> list[dict[str, Any]]:
        return [
            {
                "segment": row.segment,
                "parameter": row.parameter,
                "source_id": row.source_id,
                "current_value": row.current_value,
                "current_as_of": row.current_as_of.isoformat(),
                "prior_value": row.prior_value,
                "prior_as_of": row.prior_as_of.isoformat(),
                "delta": row.delta,
                "pct_change": row.pct_change,
            }
            for row in build_segment_trends(self._registry)
        ]

    def _build_source_documentation(self) -> dict[str, Any]:
        """Read RBA publication metadata sidecars from the raw-data cache."""
        if self._raw_data_dir is None:
            return {
                "lead_text": "",
                "documents": [],
                "recent_regulator_commentary": _empty_regulator_commentary(),
            }

        rba_dir = self._raw_data_dir / "rba"
        docs: list[dict[str, Any]] = []
        if rba_dir.exists():
            for meta_path in sorted(rba_dir.glob("*.metadata.json")):
                try:
                    payload = json.loads(meta_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    continue
                if payload.get("source_key") not in {
                    "rba_fsr", "rba_smp", "rba_chart_pack",
                }:
                    continue
                docs.append({
                    "source_key": payload.get("source_key", ""),
                    "source": payload.get("source", ""),
                    "publisher": payload.get("publisher", "Reserve Bank of Australia"),
                    "url": payload.get("url", ""),
                    "local_cached_file": payload.get("local_cached_file", ""),
                    "period": payload.get("period", ""),
                    "retrieval_date": payload.get("retrieval_date", ""),
                })

        order = {"rba_fsr": 0, "rba_smp": 1, "rba_chart_pack": 2}
        docs.sort(key=lambda row: order.get(str(row.get("source_key")), 99))
        periods = {d["source"]: d["period"] for d in docs}
        lead = ""
        if docs:
            fsr = periods.get("RBA Financial Stability Review", "latest")
            smp = periods.get("RBA Statement on Monetary Policy", "latest")
            chart = periods.get("RBA Chart Pack", "latest")
            lead = (
                "Forward-looking commentary draws from RBA Financial Stability "
                f"Review ({fsr}), Statement on Monetary Policy ({smp}), and "
                f"Chart Pack ({chart}). These are not benchmark sources; they "
                "inform governance overlays only."
            )
        commentary = self._build_recent_regulator_commentary()
        return {
            "lead_text": lead,
            "documents": docs,
            "recent_regulator_commentary": commentary,
        }

    def _build_recent_regulator_commentary(self) -> dict[str, Any]:
        """Top-3 newest items from APRA Insight + CFR per-source manifests.

        Reads ``apra/insight/_manifest.json`` and ``cfr/_manifest.json`` under
        the configured raw-data directory. When both manifests are missing or
        empty, the returned ``empty_message`` is the fallback string the
        renderer emits in place of a table — keeps the report stable on first
        run / before any governance scrape has succeeded.
        """
        assert self._raw_data_dir is not None
        apra = _read_manifest_top(
            self._raw_data_dir / "apra" / "insight" / "_manifest.json", limit=3,
        )
        cfr = _read_manifest_top(
            self._raw_data_dir / "cfr" / "_manifest.json", limit=3,
        )
        any_items = bool(apra) or bool(cfr)
        return {
            "apra_insight": apra,
            "cfr_publications": cfr,
            "empty_message": (
                "" if any_items else "No recent regulator commentary captured."
            ),
        }

    def _build_raw_data_inventory(self) -> dict[str, Any]:
        """Walk the configured raw-data directory and group files by family.

        Lazy-imports `src.csv_exporter` walker helpers so the report module
        keeps no new top-level dependency. Returns:

            {
              "root":      "data/raw" (the relative root walked),
              "n_files":   total count,
              "by_family": [
                  {"family": "pillar3", "files": [{filename, subfamily,
                                                   kind, size_bytes,
                                                   modified_utc}, ...]},
                  ...
              ],
            }
        """
        from datetime import datetime, timezone
        from src.csv_exporter import _classify_file, _walk_raw_files

        assert self._raw_data_dir is not None
        root = self._raw_data_dir
        files: list[dict[str, Any]] = []
        if root.exists():
            for fp in sorted(_walk_raw_files(root)):
                rel = fp.relative_to(root)
                parts = rel.parts
                family = parts[0] if parts else ""
                # Subfamily exists only when the path has >= 3 components.
                subfamily = parts[1] if len(parts) >= 3 else ""
                stat = fp.stat()
                files.append({
                    "family": family,
                    "subfamily": subfamily,
                    "filename": fp.name,
                    "relative_path": str(rel).replace("\\", "/"),
                    "size_bytes": stat.st_size,
                    "modified_utc": datetime.fromtimestamp(
                        stat.st_mtime, tz=timezone.utc,
                    ).isoformat(timespec="seconds"),
                    "kind": _classify_file(fp),
                })

        # Group by family, preserve sort order from the walker.
        by_family: list[dict[str, Any]] = []
        seen: dict[str, list[dict[str, Any]]] = {}
        for f in files:
            seen.setdefault(f["family"], []).append(f)
        for family in sorted(seen):
            by_family.append({"family": family, "files": seen[family]})

        return {
            "root": str(root).replace("\\", "/"),
            "n_files": len(files),
            "by_family": by_family,
        }

    @staticmethod
    def _default_period_label() -> str:
        today = date.today()
        q = (today.month - 1) // 3 + 1
        return f"Q{q} {today.year}"


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _fmt_observation_value(v: float | None) -> str:
    """Format a per-source value cell. Commentary rows (None) render blank."""
    if v is None:
        return "_(qualitative)_"
    return f"{v:.4%}"


def _empty_regulator_commentary() -> dict[str, Any]:
    return {
        "apra_insight": [],
        "cfr_publications": [],
        "empty_message": "No recent regulator commentary captured.",
    }


def _read_manifest_top(path: Path, *, limit: int) -> list[dict[str, Any]]:
    """Return the ``limit`` newest items in a governance manifest, newest first.

    Sort key is ``published_date`` descending (ISO strings sort lexically when
    they're well-formed); falls back to insertion order when the date is
    missing. Missing or unreadable manifests yield an empty list — the
    Board-report renderer handles the empty case via ``empty_message``.
    """
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    items = list(payload.get("items", []) or [])
    items.sort(
        key=lambda row: (row.get("published_date") or "", row.get("title") or ""),
        reverse=True,
    )
    out: list[dict[str, Any]] = []
    for row in items[:limit]:
        out.append({
            "title": row.get("title", ""),
            "url": row.get("url", ""),
            "published_date": row.get("published_date", ""),
            "fetched_at": row.get("fetched_at", ""),
            "local_path": row.get("local_path", ""),
        })
    return out


def _fmt_pct(v: float | None) -> str:
    if v is None:
        return "-"
    return f"{v * 100:.1f}%"


def _fmt_pct_value(v: float | None) -> str:
    """Format a PD value (e.g. 0.025) as a percentage."""
    if v is None:
        return "-"
    return f"{v:.4%}"


def _fmt_signed_pct_value(v: float | None) -> str:
    if v is None:
        return "-"
    sign = "+" if v > 0 else ""
    return f"{sign}{v:.4%}"


def _fmt_ratio(r: float | None) -> str:
    if r is None:
        return "-"
    return f"{r:.2f}x"


def _fmt_data_as_of(min_date: str, max_date: str) -> str:
    if not min_date and not max_date:
        return "-"
    if min_date == max_date:
        return min_date
    return f"{min_date}–{max_date}"


def _median(values: list[float]) -> float:
    s = sorted(values)
    n = len(s)
    if n == 0:
        return 0.0
    mid = n // 2
    if n % 2 == 1:
        return s[mid]
    return (s[mid - 1] + s[mid]) / 2.0


def _markdown_to_basic_html(md: str) -> str:
    """Minimal Markdown -> HTML converter for the report shape only.

    Handles: # / ## / ### headings, > blockquote (banner), - bullets, and
    pipe-tables. Does NOT handle inline emphasis, links, or code fences —
    the report doesn't use them. Anything else is escaped and emitted as
    a paragraph.
    """
    out: list[str] = []
    in_table = False
    in_list = False
    table_rows: list[list[str]] = []

    def flush_list() -> None:
        nonlocal in_list
        if in_list:
            out.append("</ul>")
            in_list = False

    def flush_table() -> None:
        nonlocal in_table, table_rows
        if not in_table:
            return
        if table_rows:
            header = table_rows[0]
            body = table_rows[2:] if len(table_rows) >= 2 else []
            out.append("<table><thead><tr>"
                       + "".join(f"<th>{html.escape(c.strip())}</th>" for c in header)
                       + "</tr></thead><tbody>")
            for r in body:
                out.append("<tr>"
                           + "".join(f"<td>{html.escape(c.strip())}</td>" for c in r)
                           + "</tr>")
            out.append("</tbody></table>")
        in_table = False
        table_rows = []

    for raw_line in md.splitlines():
        line = raw_line.rstrip()
        if line.startswith("|"):
            flush_list()
            in_table = True
            cells = [c for c in line.strip().strip("|").split("|")]
            table_rows.append(cells)
            continue
        else:
            flush_table()

        if line.startswith("### "):
            flush_list()
            out.append(f"<h3>{html.escape(line[4:])}</h3>")
        elif line.startswith("## "):
            flush_list()
            out.append(f"<h2>{html.escape(line[3:])}</h2>")
        elif line.startswith("# "):
            flush_list()
            out.append(f"<h1>{html.escape(line[2:])}</h1>")
        elif line.startswith("> "):
            flush_list()
            out.append(f"<div class='banner'>{html.escape(line[2:])}</div>")
        elif line.startswith("- "):
            if not in_list:
                out.append("<ul>")
                in_list = True
            out.append(f"<li>{html.escape(line[2:])}</li>")
        elif line.strip() == "":
            flush_list()
        else:
            flush_list()
            out.append(f"<p>{html.escape(line)}</p>")

    flush_list()
    flush_table()
    return "\n".join(out)
