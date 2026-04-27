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
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

from src.observations import ObservationSet, PeerObservations
from src.registry import BenchmarkRegistry
from src.validation import BIG4_SOURCE_IDS, ValidationFlags


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
    the registry and the PeerObservations facade, nothing else. The old
    CalibrationFeed / AdjustmentEngine / Triangulator dependencies are
    gone (replaced by deprecation stubs in src/).
    """

    def __init__(
        self,
        registry: BenchmarkRegistry,
        peer_observations: Optional[PeerObservations] = None,
        *,
        period_label: Optional[str] = None,
    ) -> None:
        self._registry = registry
        self._peer = peer_observations or PeerObservations(registry)
        self._period = period_label or self._default_period_label()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(self) -> dict[str, Any]:
        """Return a structured dict with all sections — used by tests + renderers."""
        observation_sets = self._collect_observation_sets()
        return {
            "meta":                    self._build_meta(observation_sets),
            "banner":                  RAW_ONLY_BANNER,
            "executive_summary":       self._build_executive_summary(observation_sets),
            "per_source_observations": self._build_per_source_observations(observation_sets),
            "validation_summary":      self._build_validation_summary(observation_sets),
            "big4_vs_nonbank_spread":  self._build_big4_vs_nonbank_spread(observation_sets),
            "provenance":              self._build_provenance(observation_sets),
        }

    def to_markdown(self) -> str:
        """Render committee-friendly Markdown."""
        data = self.generate()
        lines: list[str] = []
        meta = data["meta"]

        lines.append(f"# External Benchmark Report — {self._period}")
        lines.append(f"_Generated: {meta['generated_at']}_")
        lines.append("")
        lines.append(f"> {data['banner']}")
        lines.append("")

        # 1
        lines.append("## 1. Executive Summary")
        for bullet in data["executive_summary"]["lines"]:
            lines.append(f"- {bullet}")
        lines.append("")

        # 2
        lines.append("## 2. Per-source raw observations by segment")
        if not data["per_source_observations"]:
            lines.append("_No observations recorded._")
        for seg_block in data["per_source_observations"]:
            lines.append(f"### {seg_block['segment']}")
            lines.append("")
            lines.append("| Source | Source type | Param | Value | As-of | Vintage | Methodology | Page/Table |")
            lines.append("| --- | --- | --- | ---:| --- | --- | --- | --- |")
            for obs in seg_block["observations"]:
                lines.append(
                    f"| {obs['source_id']} "
                    f"| {obs['source_type']} "
                    f"| {obs['parameter']} "
                    f"| {obs['value']:.4%} "
                    f"| {obs['as_of_date']} "
                    f"| {obs['reporting_basis']} "
                    f"| {obs['methodology_note']} "
                    f"| {obs['page_or_table_ref'] or '-'} |"
                )
            lines.append("")
        lines.append("")

        # 3
        lines.append("## 3. Cross-source validation summary")
        lines.append("")
        lines.append("| Segment | N | Spread % | Big 4 spread % | Non-bank/Big 4 ratio | Outliers | Stale sources |")
        lines.append("| --- | ---:| ---:| ---:| ---:| --- | --- |")
        for vrow in data["validation_summary"]:
            lines.append(
                f"| {vrow['segment']} "
                f"| {vrow['n_sources']} "
                f"| {_fmt_pct(vrow['spread_pct'])} "
                f"| {_fmt_pct(vrow['big4_spread_pct'])} "
                f"| {_fmt_ratio(vrow['bank_vs_nonbank_ratio'])} "
                f"| {', '.join(vrow['outlier_sources']) or '-'} "
                f"| {', '.join(vrow['stale_sources']) or '-'} |"
            )
        lines.append("")

        # 4
        lines.append("## 4. Big 4 vs non-bank disclosure spread (informational only)")
        lines.append("")
        lines.append(
            "_The values below are raw published figures from each cohort. The "
            "engine does NOT recommend any uplift or adjustment from this spread. "
            "Consuming projects decide how (or whether) to use it._"
        )
        lines.append("")
        lines.append("| Segment | Big 4 median | Non-bank median | Ratio | Big 4 N | Non-bank N |")
        lines.append("| --- | ---:| ---:| ---:| ---:| ---:|")
        for srow in data["big4_vs_nonbank_spread"]:
            lines.append(
                f"| {srow['segment']} "
                f"| {_fmt_pct_value(srow['big4_median'])} "
                f"| {_fmt_pct_value(srow['nonbank_median'])} "
                f"| {_fmt_ratio(srow['ratio'])} "
                f"| {srow['big4_count']} "
                f"| {srow['nonbank_count']} |"
            )
        lines.append("")

        # 5
        lines.append("## 5. Provenance & methodology footnotes")
        for prov in data["provenance"]:
            lines.append(f"- **{prov['source_id']}** ({prov['source_type']}): "
                         f"{prov['reporting_basis']} — {prov['source_url'] or 'n/a'}")
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
            from reports.docx_helpers import (
                new_document, add_heading, add_paragraph, add_table, add_bullet,
            )
        except ImportError as exc:
            raise ImportError(
                "to_docx requires the 'reports' extras (python-docx). "
                "Install with: pip install -e .[reports]"
            ) from exc

        data = self.generate()
        title = f"External Benchmark Report — {self._period}"
        subtitle = f"Generated {data['meta']['generated_at']}"
        doc = new_document(title, subtitle=subtitle)

        add_paragraph(doc, data["banner"], italic=True)

        add_heading(doc, "1. Executive Summary", level=2)
        for line in data["executive_summary"]["lines"]:
            add_bullet(doc, line)

        add_heading(doc, "2. Per-source raw observations by segment", level=2)
        for seg_block in data["per_source_observations"]:
            add_heading(doc, seg_block["segment"], level=3)
            add_table(
                doc,
                headers=["source_id", "source_type", "param", "value", "as_of",
                         "vintage", "methodology", "page_or_table_ref"],
                rows=[
                    [o["source_id"], o["source_type"], o["parameter"],
                     f"{o['value']:.4%}", str(o["as_of_date"]),
                     o["reporting_basis"], o["methodology_note"],
                     o["page_or_table_ref"] or "-"]
                    for o in seg_block["observations"]
                ],
            )

        add_heading(doc, "3. Cross-source validation summary", level=2)
        add_table(
            doc,
            headers=["segment", "n_sources", "spread_pct", "big4_spread_pct",
                     "nonbank/big4 ratio", "outliers", "stale"],
            rows=[
                [v["segment"], str(v["n_sources"]), _fmt_pct(v["spread_pct"]),
                 _fmt_pct(v["big4_spread_pct"]),
                 _fmt_ratio(v["bank_vs_nonbank_ratio"]),
                 ", ".join(v["outlier_sources"]) or "-",
                 ", ".join(v["stale_sources"]) or "-"]
                for v in data["validation_summary"]
            ],
        )

        add_heading(doc, "4. Big 4 vs non-bank disclosure spread (informational only)", level=2)
        add_paragraph(
            doc,
            "Values are raw published figures from each cohort. The engine "
            "does NOT recommend any uplift or adjustment from this spread.",
            italic=True,
        )
        add_table(
            doc,
            headers=["segment", "big4_median", "nonbank_median", "ratio",
                     "big4_count", "nonbank_count"],
            rows=[
                [s["segment"],
                 _fmt_pct_value(s["big4_median"]),
                 _fmt_pct_value(s["nonbank_median"]),
                 _fmt_ratio(s["ratio"]),
                 str(s["big4_count"]), str(s["nonbank_count"])]
                for s in data["big4_vs_nonbank_spread"]
            ],
        )

        add_heading(doc, "5. Provenance & methodology footnotes", level=2)
        for p in data["provenance"]:
            add_bullet(
                doc,
                f"{p['source_id']} ({p['source_type']}): {p['reporting_basis']} — "
                f"{p['source_url'] or 'n/a'}",
            )

        out = Path(path)
        doc.save(str(out))
        return out

    # ------------------------------------------------------------------
    # Section builders
    # ------------------------------------------------------------------

    def _collect_observation_sets(self) -> list[ObservationSet]:
        """One ObservationSet per canonical segment that has observations."""
        return [self._peer.for_segment(seg) for seg in self._peer.all_segments()]

    def _build_meta(self, sets: list[ObservationSet]) -> dict[str, Any]:
        total_obs = sum(len(s.observations) for s in sets)
        sources = {o.source_id for s in sets for o in s.observations}
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "period_label": self._period,
            "n_segments": len(sets),
            "n_observations": total_obs,
            "n_sources": len(sources),
        }

    def _build_executive_summary(self, sets: list[ObservationSet]) -> dict[str, Any]:
        total_obs = sum(len(s.observations) for s in sets)
        sources = {o.source_id for s in sets for o in s.observations}
        nonbank = {sid for sid in sources if sid.lower() not in BIG4_SOURCE_IDS}
        big4 = {sid for sid in sources if sid.lower() in BIG4_SOURCE_IDS}
        lines = [
            f"{total_obs} raw observations across {len(sets)} canonical segments.",
            f"{len(sources)} distinct sources contributing: "
            f"{len(big4)} Big 4 + {len(nonbank)} non-bank / aggregate.",
            "Every value in this report is the source-published raw figure. "
            "No multipliers, no triangulation, no adjustment.",
        ]
        return {"lines": lines}

    def _build_per_source_observations(self, sets: list[ObservationSet]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for s in sets:
            if not s.observations:
                continue
            obs_rows = [
                {
                    "source_id": o.source_id,
                    "source_type": o.source_type.value,
                    "parameter": o.parameter,
                    "value": o.value,
                    "as_of_date": o.as_of_date.isoformat(),
                    "reporting_basis": o.reporting_basis,
                    "methodology_note": o.methodology_note,
                    "page_or_table_ref": o.page_or_table_ref,
                    "source_url": o.source_url,
                }
                for o in s.observations
            ]
            out.append({"segment": s.segment, "observations": obs_rows})
        return out

    def _build_validation_summary(self, sets: list[ObservationSet]) -> list[dict[str, Any]]:
        return [
            {
                "segment": s.segment,
                "n_sources": s.validation_flags.n_sources,
                "spread_pct": s.validation_flags.spread_pct,
                "big4_spread_pct": s.validation_flags.big4_spread_pct,
                "bank_vs_nonbank_ratio": s.validation_flags.bank_vs_nonbank_ratio,
                "outlier_sources": list(s.validation_flags.outlier_sources),
                "stale_sources": list(s.validation_flags.stale_sources),
            }
            for s in sets if s.observations
        ]

    def _build_big4_vs_nonbank_spread(self, sets: list[ObservationSet]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for s in sets:
            big4_vals = [o.value for o in s.by_source_type(big4_only=True)]
            non_vals = [o.value for o in s.by_source_type(nonbank_only=True)]
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

    @staticmethod
    def _default_period_label() -> str:
        today = date.today()
        q = (today.month - 1) // 3 + 1
        return f"Q{q} {today.year}"


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _fmt_pct(v: float | None) -> str:
    if v is None:
        return "-"
    return f"{v * 100:.1f}%"


def _fmt_pct_value(v: float | None) -> str:
    """Format a PD value (e.g. 0.025) as a percentage."""
    if v is None:
        return "-"
    return f"{v:.4%}"


def _fmt_ratio(r: float | None) -> str:
    if r is None:
        return "-"
    return f"{r:.2f}x"


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
