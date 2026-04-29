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
from src.validation import BIG4_SOURCE_IDS, ValidationFlags, is_big4_source_id


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
    ) -> None:
        self._registry = registry
        self._peer = peer_observations or PeerObservations(registry)
        self._period = period_label or self._default_period_label()
        # Section 6 (raw-data inventory) walks this directory. ``None``
        # disables the section entirely (used by unit-test fixtures that
        # don't stage any raw files).
        self._raw_data_dir: Optional[Path] = (
            Path(raw_data_dir) if raw_data_dir is not None else None
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(self) -> dict[str, Any]:
        """Return a structured dict with all sections — used by tests + renderers."""
        observation_sets = self._collect_observation_sets()
        out: dict[str, Any] = {
            "meta":                    self._build_meta(observation_sets),
            "banner":                  RAW_ONLY_BANNER,
            "executive_summary":       self._build_executive_summary(observation_sets),
            "per_source_observations": self._build_per_source_observations(observation_sets),
            "validation_summary":      self._build_validation_summary(observation_sets),
            "big4_vs_nonbank_spread":  self._build_big4_vs_nonbank_spread(observation_sets),
            "provenance":              self._build_provenance(observation_sets),
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
        big4 = {sid for sid in sources if is_big4_source_id(sid)}
        nonbank = {sid for sid in sources if not is_big4_source_id(sid)}
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
