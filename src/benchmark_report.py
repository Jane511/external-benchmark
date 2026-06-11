"""Private-credit model input report.

The rendered report is intentionally lean: PD, LGD, expected-loss rate,
stress-testing inputs, portfolio-monitor metrics, and per-bank industry
monitor rows. Audit-heavy content such as methodology prose, source
inventories, raw-data manifests, caveat logs, and provenance appendices
belongs in engineering/governance outputs, not in the model input report.
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
from src.model_inputs import (
    build_report_summary,
    PD_STRESS_MULTIPLIER,
    LGD_STRESS_MULTIPLIER,
)
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
    "Direct model inputs only: PD, LGD, expected-loss rate, stress-testing "
    "rates, portfolio-monitor metrics, and per-bank industry monitor rows."
)


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class BenchmarkCalibrationReport:
    """Compose direct model-input sections from a registry.

    Constructor arguments are retained for backwards-compatible call sites;
    the current renderer uses the registry-backed model-input bundle.
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
        # Section 6 reads Big 4 Pillar 3 industry tables from this raw-data
        # directory. ``None`` leaves that report table empty.
        self._raw_data_dir: Optional[Path] = (
            Path(raw_data_dir) if raw_data_dir is not None else None
        )
        self._refresh_schedules = refresh_schedules
        self._refresh_pipeline_quiet = refresh_pipeline_quiet

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(self) -> dict[str, Any]:
        """Return the lean model-input report payload."""
        return build_report_summary(self._registry, raw_data_dir=self._raw_data_dir)

    def _executive_summary_blocks(
        self, data: dict[str, Any],
    ) -> list[tuple[str, str]]:
        """Plain-English summary block list shared by the markdown and DOCX renders.

        Each tuple is ``(kind, text)`` where kind is ``h2`` / ``h3`` / ``p`` /
        ``bullet``. Headline figures are pulled from ``data`` so the summary
        stays accurate as the underlying disclosures change. Markdown bold
        (``**term**``) is stripped for the DOCX bullets.
        """
        meta = data["meta"]
        n_obs = meta.get("n_observations", 0)
        n_seg = meta.get("n_segments", 0)
        banks = sorted({
            str(row["bank"]) for row in data.get("bank_industry_inputs", [])
            if row.get("bank")
        })
        window = _fmt_data_as_of(
            meta.get("data_as_of_min", ""), meta.get("data_as_of_max", ""),
        )
        return [
            ("h2", "Executive summary"),
            ("p",
             "This report consolidates externally-disclosed credit-risk "
             "parameters for Australian bank and non-bank lenders into a "
             "single set of model-ready benchmarks. It is built from public "
             "Basel Pillar 3 disclosures, APRA and RBA statistics, and "
             "non-bank lender reports, and is aligned to the APRA APS 113 / "
             "Basel IRB framework."),
            ("p",
             "Every figure is a source-published value — no adjustment, "
             "triangulation, or modelling overlay — so each number traces back "
             "to a named disclosure and reporting date."),
            ("h3", "What this report covers"),
            ("bullet",
             "**Probability of default (PD)** — likelihood a borrower defaults "
             "within 12 months, by credit segment (Section 1)."),
            ("bullet",
             "**Loss given default (LGD)** — share of exposure not recovered "
             "after default (Section 2)."),
            ("bullet",
             "**Expected loss (EL = PD × LGD)** — the headline credit-loss "
             "rate per segment (Section 3)."),
            ("bullet",
             "**Stress testing** — PD and LGD under a downturn, using stress "
             "multipliers floored at APS 113 regulatory bands (Section 4)."),
            ("bullet",
             "**Portfolio monitoring** — arrears, non-performing, impaired and "
             "loss-rate metrics for early-warning tracking (Section 5)."),
            ("bullet",
             "**Per-bank industry exposures** — Big 4 exposure, "
             "non-performing, provision and write-off by industry sector "
             "(Section 6)."),
            ("h3", "Coverage at a glance"),
            ("bullet", f"{n_obs} source observations across {n_seg} credit segments."),
            ("bullet",
             f"{len(banks)} banks in the industry-exposure view"
             + (f" ({', '.join(banks)})" if banks else "")
             + ", plus non-bank lenders and regulatory references."),
            ("bullet", f"Data as-of window: {window}."),
            ("h3", "How to read the numbers"),
            ("bullet",
             "Rates are decimals in [0, 1]; for example, 0.03 represents "
             "three percent."),
            ("bullet",
             "Expected-loss rate = PD × LGD, shown in basis points (bps); "
             "1 bp = 0.01%, so 14 bps = 0.14%."),
            ("bullet",
             f"Stressed PD/LGD apply {PD_STRESS_MULTIPLIER}× / "
             f"{LGD_STRESS_MULTIPLIER}× multipliers, floored at APS 113 bands."),
            ("bullet", '"As-of" is the disclosure date of the most recent source.'),
        ]

    def to_markdown(self) -> str:
        """Render a concise private-credit model-input report."""
        data = self.generate()
        lines: list[str] = []
        meta = data["meta"]

        lines.append(f"# Australian Credit Risk Benchmarks - {self._period}")
        lines.append(
            f"_Generated: {meta['generated_at']} | Data as-of: "
            f"{_fmt_data_as_of(meta['data_as_of_min'], meta['data_as_of_max'])}_"
        )
        lines.append("")

        prev_bullet = False
        for kind, text in self._executive_summary_blocks(data):
            if kind == "bullet":
                lines.append(f"- {text}")
                prev_bullet = True
                continue
            if prev_bullet:
                lines.append("")
                prev_bullet = False
            if kind == "h2":
                lines.append(f"## {text}")
            elif kind == "h3":
                lines.append(f"### {text}")
            else:  # paragraph
                lines.append(text)
            lines.append("")
        if prev_bullet:
            lines.append("")

        def append_table(
            title: str,
            headers: list[str],
            rows: list[list[object]],
        ) -> None:
            lines.append(f"## {title}")
            lines.append("")
            if not rows:
                lines.append("_No numeric inputs available._")
                lines.append("")
                return
            lines.append("| " + " | ".join(headers) + " |")
            lines.append("| " + " | ".join("---" for _ in headers) + " |")
            for row in rows:
                lines.append("| " + " | ".join(str(cell) for cell in row) + " |")
            lines.append("")

        append_table(
            "1. PD Inputs",
            ["Segment", "Product", "PD decimal", "Source", "As-of"],
            [
                [
                    row["segment_label"],
                    row["product"],
                    _fmt_decimal(row["pd_decimal"]),
                    row["source_id"],
                    row["as_of_date"],
                ]
                for row in data["pd_inputs"]
            ],
        )
        append_table(
            "2. LGD Inputs",
            ["Segment", "Product", "LGD decimal", "Source", "As-of"],
            [
                [
                    row["segment_label"],
                    row["product"],
                    _fmt_decimal(row["lgd_decimal"]),
                    row["source_id"],
                    row["as_of_date"],
                ]
                for row in data["lgd_inputs"]
            ],
        )
        append_table(
            "3. Expected Loss Inputs",
            [
                "Segment", "Product", "PD decimal", "LGD decimal",
                "EL rate (bps)", "PD N", "LGD N", "As-of",
            ],
            [
                [
                    row["segment_label"],
                    row["product"],
                    _fmt_decimal(row["pd_decimal"]),
                    _fmt_decimal(row["lgd_decimal"]),
                    _fmt_bps(row["expected_loss_rate_decimal"]),
                    row["pd_source_count"],
                    row["lgd_source_count"],
                    row["as_of_date"],
                ]
                for row in data["expected_loss_inputs"]
            ],
        )
        append_table(
            "4. Stress Testing Inputs",
            [
                "Segment",
                "Product",
                "Base EL (bps)",
                "Stressed PD decimal",
                "Stressed LGD decimal",
                "Stressed EL (bps)",
                "As-of",
            ],
            [
                [
                    row["segment_label"],
                    row["product"],
                    _fmt_bps(row["base_expected_loss_rate_decimal"]),
                    _fmt_decimal(row["stressed_pd_decimal"]),
                    _fmt_decimal(row["stressed_lgd_decimal"]),
                    _fmt_bps(row["stressed_expected_loss_rate_decimal"]),
                    row["as_of_date"],
                ]
                for row in data["stress_testing_inputs"]
            ],
        )
        append_table(
            "5. Portfolio Monitor Inputs",
            [
                "Segment",
                "Product",
                "Arrears decimal",
                "NPL decimal",
                "Impaired decimal",
                "Loss rate decimal",
                "Sources",
                "As-of",
            ],
            [
                [
                    row["segment_label"],
                    row["product"],
                    _fmt_decimal_or_blank(row["arrears_decimal"]),
                    _fmt_decimal_or_blank(row["npl_decimal"]),
                    _fmt_decimal_or_blank(row["impaired_decimal"]),
                    _fmt_decimal_or_blank(row["loss_rate_decimal"]),
                    row["monitor_source_count"],
                    row["as_of_date"],
                ]
                for row in data["portfolio_monitor_inputs"]
            ],
        )
        append_table(
            "6. Per-Bank Industry Inputs",
            [
                "Bank",
                "Industry",
                "Exposure AUDm",
                "NPE AUDm",
                "NPE decimal",
                "Provision AUDm",
                "Write-offs AUDm",
                "Write-off decimal",
                "As-of",
            ],
            [
                [
                    row["bank"],
                    row["industry"],
                    _fmt_decimal_or_blank(row["exposure_aud_m"]),
                    _fmt_decimal_or_blank(row["npe_aud_m"]),
                    _fmt_decimal_or_blank(row["npe_rate_decimal"]),
                    _fmt_decimal_or_blank(row["provision_aud_m"]),
                    _fmt_decimal_or_blank(row["write_offs_aud_m"]),
                    _fmt_decimal_or_blank(row["write_off_rate_decimal"]),
                    row["as_of_date"],
                ]
                for row in data["bank_industry_inputs"]
            ],
        )

        return "\n".join(lines)

    def to_html(self) -> str:
        """Single self-contained HTML file, inline CSS, no JS."""
        md = self.to_markdown()
        body = _markdown_to_basic_html(md)
        return (
            "<!doctype html><html><head><meta charset='utf-8'>"
            f"<title>Australian Credit Risk Benchmarks - {html.escape(self._period)}</title>"
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
        title = f"Australian Credit Risk Benchmarks - {self._period}"
        subtitle = (
            f"Generated {data['meta']['generated_at']} | Data as-of "
            f"{_fmt_data_as_of(data['meta']['data_as_of_min'], data['meta']['data_as_of_max'])}"
        )
        doc = new_document(title, subtitle=subtitle)

        for kind, text in self._executive_summary_blocks(data):
            clean = text.replace("**", "")
            if kind == "h2":
                add_heading(doc, clean, level=2)
            elif kind == "h3":
                add_heading(doc, clean, level=3)
            elif kind == "bullet":
                add_bullet(doc, clean)
            else:  # paragraph
                add_paragraph(doc, clean)

        def add_input_table(
            title: str,
            headers: list[str],
            rows: list[list[object]],
        ) -> None:
            add_heading(doc, title, level=2)
            if not rows:
                add_paragraph(doc, "No numeric inputs available.", italic=True)
                return
            add_table(doc, headers=headers, rows=rows)

        add_input_table(
            "1. PD Inputs",
            ["segment", "product", "pd_decimal", "source", "as_of"],
            [
                [
                    row["segment_label"],
                    row["product"],
                    _fmt_decimal(row["pd_decimal"]),
                    row["source_id"],
                    row["as_of_date"],
                ]
                for row in data["pd_inputs"]
            ],
        )
        add_input_table(
            "2. LGD Inputs",
            ["segment", "product", "lgd_decimal", "source", "as_of"],
            [
                [
                    row["segment_label"],
                    row["product"],
                    _fmt_decimal(row["lgd_decimal"]),
                    row["source_id"],
                    row["as_of_date"],
                ]
                for row in data["lgd_inputs"]
            ],
        )
        add_input_table(
            "3. Expected Loss Inputs",
            [
                "segment", "product", "pd_decimal", "lgd_decimal",
                "el_rate_bps", "pd_n", "lgd_n", "as_of",
            ],
            [
                [
                    row["segment_label"],
                    row["product"],
                    _fmt_decimal(row["pd_decimal"]),
                    _fmt_decimal(row["lgd_decimal"]),
                    _fmt_bps(row["expected_loss_rate_decimal"]),
                    str(row["pd_source_count"]),
                    str(row["lgd_source_count"]),
                    row["as_of_date"],
                ]
                for row in data["expected_loss_inputs"]
            ],
        )
        add_input_table(
            "4. Stress Testing Inputs",
            ["segment", "product", "base_el_bps", "stressed_pd_decimal",
             "stressed_lgd_decimal", "stressed_el_bps", "as_of"],
            [
                [
                    row["segment_label"],
                    row["product"],
                    _fmt_bps(row["base_expected_loss_rate_decimal"]),
                    _fmt_decimal(row["stressed_pd_decimal"]),
                    _fmt_decimal(row["stressed_lgd_decimal"]),
                    _fmt_bps(row["stressed_expected_loss_rate_decimal"]),
                    row["as_of_date"],
                ]
                for row in data["stress_testing_inputs"]
            ],
        )
        add_input_table(
            "5. Portfolio Monitor Inputs",
            ["segment", "product", "arrears_decimal", "npl_decimal",
             "impaired_decimal", "loss_rate_decimal", "sources", "as_of"],
            [
                [
                    row["segment_label"],
                    row["product"],
                    _fmt_decimal_or_blank(row["arrears_decimal"]),
                    _fmt_decimal_or_blank(row["npl_decimal"]),
                    _fmt_decimal_or_blank(row["impaired_decimal"]),
                    _fmt_decimal_or_blank(row["loss_rate_decimal"]),
                    str(row["monitor_source_count"]),
                    row["as_of_date"],
                ]
                for row in data["portfolio_monitor_inputs"]
            ],
        )
        add_input_table(
            "6. Per-Bank Industry Inputs",
            ["bank", "industry", "exposure_aud_m", "npe_aud_m",
             "npe_decimal", "provision_aud_m", "write_offs_aud_m",
             "write_off_decimal", "as_of"],
            [
                [
                    row["bank"],
                    row["industry"],
                    _fmt_decimal_or_blank(row["exposure_aud_m"]),
                    _fmt_decimal_or_blank(row["npe_aud_m"]),
                    _fmt_decimal_or_blank(row["npe_rate_decimal"]),
                    _fmt_decimal_or_blank(row["provision_aud_m"]),
                    _fmt_decimal_or_blank(row["write_offs_aud_m"]),
                    _fmt_decimal_or_blank(row["write_off_rate_decimal"]),
                    row["as_of_date"],
                ]
                for row in data["bank_industry_inputs"]
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
        """Section 2 structure — metric first, then segment, then cohort.

        A reader looking for "PD for bridging" goes to the PD section
        and scans for the bridging sub-table. If a metric isn't
        published for a given segment, that segment is listed under
        "No published <metric> for ..." at the top of the metric
        section, so the absence is explicit.

        Output shape::

            [
              {
                "parameter": "pd",
                "label": "Probability of default (PD)",
                "segments_with_data":    ["Commercial Property", ...],
                "segments_without_data": ["Bridging Residential", ...],
                "by_segment": [
                  {
                    "segment": "commercial_property",
                    "segment_label": "Commercial Property",
                    "rows": [{cohort, source, latest, ...}, ...],
                    "cohort_medians": [...],
                  },
                  ...
                ],
              },
              ...
            ]
        """
        from src.models import cohort_for

        # Inventory every segment that has any observation, so we can
        # tell readers which segments are missing this metric.
        all_segments_with_obs = {s.segment for s in sets if s.observations}
        all_segment_labels = {
            seg: segment_label(seg) for seg in all_segments_with_obs
        }

        # Bucket observations by (parameter, segment, source_id).
        by_param_segment: dict[str, dict[str, dict[str, list[Any]]]] = {}
        for s in sets:
            if not s.observations:
                continue
            for o in s.observations:
                (
                    by_param_segment
                    .setdefault(o.parameter, {})
                    .setdefault(s.segment, {})
                    .setdefault(o.source_id, [])
                    .append(o)
                )

        _COHORT_ORDER = {
            "peer_big4": 0,
            "peer_other_major_bank": 1,
            "peer_non_bank": 2,
            "regulator_aggregate": 3,
            "rating_agency": 4,
            "regulatory_floor": 5,
            "industry_body": 6,
        }
        _PARAM_ORDER = {
            "pd": 0, "lgd": 1, "arrears": 2, "impaired": 3,
            "npl": 4, "loss_rate": 5, "commentary": 6,
        }

        out: list[dict[str, Any]] = []
        for parameter in sorted(
            by_param_segment, key=lambda p: _PARAM_ORDER.get(p, 99),
        ):
            segments_dict = by_param_segment[parameter]
            by_segment_blocks: list[dict[str, Any]] = []
            segments_with_data: list[str] = []

            for segment in sorted(segments_dict, key=lambda s: segment_label(s).lower()):
                sources_dict = segments_dict[segment]
                rows: list[dict[str, Any]] = []
                for source_id, obs_list in sources_dict.items():
                    obs_sorted = sorted(obs_list, key=lambda o: o.as_of_date, reverse=True)
                    latest = obs_sorted[0]
                    numeric = [o.value for o in obs_sorted if o.value is not None]
                    median_value = _median(numeric) if numeric else None
                    coh = cohort_for(latest.source_type, source_id).value
                    rows.append({
                        "source_id": source_id,
                        "friendly_name": friendly_name(source_id),
                        "cohort": coh,
                        "cohort_label": cohort_label(coh),
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

                # Cohort-level median across latest values (per metric × segment).
                grouped: dict[str, list[float]] = {}
                for r in rows:
                    if r["latest_value"] is None:
                        continue
                    grouped.setdefault(r["cohort"], []).append(r["latest_value"])
                cohort_medians = [
                    {
                        "cohort": cohort_value,
                        "cohort_label": cohort_label(cohort_value),
                        "median": _median(values),
                        "n": len(values),
                    }
                    for cohort_value, values in grouped.items() if values
                ]

                rows.sort(key=lambda r: (
                    _COHORT_ORDER.get(r["cohort"], 99),
                    r["friendly_name"].lower(),
                ))
                cohort_medians.sort(key=lambda c: _COHORT_ORDER.get(c["cohort"], 99))

                by_segment_blocks.append({
                    "segment": segment,
                    "segment_label": segment_label(segment),
                    "rows": rows,
                    "cohort_medians": cohort_medians,
                })
                segments_with_data.append(segment_label(segment))

            segments_without_data = sorted(
                label for seg, label in all_segment_labels.items()
                if seg not in segments_dict
            )

            out.append({
                "parameter": parameter,
                "label": parameter_label(parameter),
                "segments_with_data": segments_with_data,
                "segments_without_data": segments_without_data,
                "by_segment": by_segment_blocks,
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


def _fmt_pct_or_blank(v: object) -> str:
    if v in ("", None):
        return "-"
    return _fmt_pct_value(float(v))


def _fmt_decimal(v: object) -> str:
    return f"{float(v):.2f}"


def _fmt_bps(v: object) -> str:
    """Format a small loss-rate decimal as basis points (0.0014 -> '14 bps').

    Loss rates are tiny as decimals, so rounding to 2dp reads as "0.00" and
    looks like zero loss. Basis points keep them legible (1 bp = 0.01%).
    """
    return f"{round(float(v) * 10000)} bps"


def _fmt_decimal_or_blank(v: object) -> str:
    if v in ("", None):
        return "-"
    return _fmt_decimal(v)


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
