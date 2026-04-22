"""Governance reporting — READ-ONLY observer over the registry.

Hard constraint: `governance.py` must NEVER invoke the adjustment layer or
write to the database. All six reports are pure reads over the registry
(plus the provided own-estimates dict for peer comparison). This preserves
the audit trail — governance queries don't generate adjustment rows, and
read-only observers don't change the thing they're observing.

Six report types (plan §8):
    1. stale_benchmark_report()          per-source-type cadence from YAML
    2. quality_assessment_report()       5-dim matrix (Bank Framework §11)
    3. peer_comparison_report()          flag divergence > 30% vs peer median
    4. coverage_report()                 flag < 2 external sources per segment
    5. annual_review_package()           MRC (bank) / Credit Committee (PC)
    6. version_drift_report()            leverages registry.get_version_history

Each report returns a `GovernanceReport` Pydantic model (from models.py).

DOCX export is deferred: `export_to_docx()` raises ImportError with a clear
install hint when python-docx isn't available. When the dependency lands,
plumb the real DOCX generation into the same function signature.
"""
from __future__ import annotations

import statistics
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

import yaml

from src.models import (
    DataType,
    GovernanceReport,
    InstitutionType,
    QualityScore,
    SourceType,
)
from src.registry import BenchmarkRegistry


DEFAULT_REFRESH_PATH = (
    Path(__file__).parent.parent / "config" / "refresh_schedules.yaml"
)

# Per-source-type 5-dimension baseline (Bank Framework §11 interpretation).
# Each entry covers: depth, relevance, transparency, frequency, regulatory_standing.
_QUALITY_MATRIX: dict[SourceType, dict[str, QualityScore]] = {
    SourceType.PILLAR3: {
        "depth": QualityScore.HIGH, "relevance": QualityScore.HIGH,
        "transparency": QualityScore.HIGH, "frequency": QualityScore.HIGH,
        "regulatory_standing": QualityScore.HIGH,
    },
    SourceType.APRA_ADI: {
        "depth": QualityScore.HIGH, "relevance": QualityScore.HIGH,
        "transparency": QualityScore.HIGH, "frequency": QualityScore.HIGH,
        "regulatory_standing": QualityScore.HIGH,
    },
    SourceType.RATING_AGENCY: {
        "depth": QualityScore.HIGH, "relevance": QualityScore.MEDIUM,
        "transparency": QualityScore.MEDIUM, "frequency": QualityScore.MEDIUM,
        "regulatory_standing": QualityScore.MEDIUM,
    },
    SourceType.ICC_TRADE: {
        "depth": QualityScore.HIGH, "relevance": QualityScore.HIGH,
        "transparency": QualityScore.HIGH, "frequency": QualityScore.MEDIUM,
        "regulatory_standing": QualityScore.MEDIUM,
    },
    SourceType.INDUSTRY_BODY: {
        "depth": QualityScore.MEDIUM, "relevance": QualityScore.MEDIUM,
        "transparency": QualityScore.MEDIUM, "frequency": QualityScore.MEDIUM,
        "regulatory_standing": QualityScore.LOW,
    },
    SourceType.LISTED_PEER: {
        "depth": QualityScore.MEDIUM, "relevance": QualityScore.MEDIUM,
        "transparency": QualityScore.MEDIUM, "frequency": QualityScore.MEDIUM,
        "regulatory_standing": QualityScore.LOW,
    },
    SourceType.REGULATORY: {
        "depth": QualityScore.HIGH, "relevance": QualityScore.HIGH,
        "transparency": QualityScore.HIGH, "frequency": QualityScore.LOW,
        "regulatory_standing": QualityScore.HIGH,
    },
    SourceType.RBA: {
        "depth": QualityScore.HIGH, "relevance": QualityScore.HIGH,
        "transparency": QualityScore.HIGH, "frequency": QualityScore.MEDIUM,
        "regulatory_standing": QualityScore.HIGH,
    },
    SourceType.BUREAU: {
        "depth": QualityScore.MEDIUM, "relevance": QualityScore.MEDIUM,
        "transparency": QualityScore.MEDIUM, "frequency": QualityScore.HIGH,
        "regulatory_standing": QualityScore.MEDIUM,
    },
    SourceType.INSOLVENCY: {
        "depth": QualityScore.MEDIUM, "relevance": QualityScore.MEDIUM,
        "transparency": QualityScore.MEDIUM, "frequency": QualityScore.LOW,
        "regulatory_standing": QualityScore.HIGH,
    },
}


def load_refresh_schedules(path: Path | str | None = None) -> dict[str, int]:
    """Return {source_type_string: days_threshold}."""
    resolved = Path(path) if path else DEFAULT_REFRESH_PATH
    with open(resolved, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return dict(raw["refresh_schedules"])


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class GovernanceReporter:
    """Read-only observer that produces the six report variants from plan §8."""

    def __init__(
        self,
        registry: BenchmarkRegistry,
        institution_type: InstitutionType,
        refresh_schedules: Optional[dict[str, int]] = None,
    ) -> None:
        self._registry = registry
        self._inst = institution_type
        self._schedules = (
            refresh_schedules
            if refresh_schedules is not None
            else load_refresh_schedules()
        )

    # ------------------------------------------------------------------
    # 1. Stale benchmark report
    # ------------------------------------------------------------------

    def stale_benchmark_report(
        self, *, as_of: Optional[date] = None,
    ) -> GovernanceReport:
        """Flag any latest-version entry older than its source-type threshold."""
        today = as_of or date.today()
        entries = self._registry.list(latest_only=True)

        findings: list[dict[str, Any]] = []
        flags: list[str] = []
        for e in entries:
            threshold = self._schedules.get(e.source_type.value, 365)
            days_old = (today - e.retrieval_date).days
            is_stale = days_old > threshold
            findings.append({
                "source_id": e.source_id,
                "source_type": e.source_type.value,
                "retrieval_date": e.retrieval_date.isoformat(),
                "days_old": days_old,
                "threshold_days": threshold,
                "stale": is_stale,
            })
            if is_stale:
                flags.append(f"stale:{e.source_id}")

        return GovernanceReport(
            report_type="stale_benchmarks",
            generated_at=_utcnow(),
            institution_type=self._inst,
            findings=findings,
            flags=flags,
        )

    # ------------------------------------------------------------------
    # 2. Quality assessment matrix
    # ------------------------------------------------------------------

    def quality_assessment_report(self) -> GovernanceReport:
        """Score each latest-version entry on 5 dimensions."""
        entries = self._registry.list(latest_only=True)
        findings: list[dict[str, Any]] = []
        flags: list[str] = []

        for e in entries:
            matrix = _QUALITY_MATRIX.get(e.source_type, {})
            dims = {dim: score.value for dim, score in matrix.items()}
            low_dims = [d for d, s in matrix.items() if s == QualityScore.LOW]
            findings.append({
                "source_id": e.source_id,
                "source_type": e.source_type.value,
                "analyst_quality": e.quality_score.value,
                "dimensions": dims,
                "low_dimensions": low_dims,
            })
            if low_dims:
                flags.append(f"low_quality:{e.source_id}:{','.join(low_dims)}")

        return GovernanceReport(
            report_type="quality_assessment",
            generated_at=_utcnow(),
            institution_type=self._inst,
            findings=findings,
            flags=flags,
        )

    # ------------------------------------------------------------------
    # 3. Peer comparison (>30% divergence flagged)
    # ------------------------------------------------------------------

    def peer_comparison_report(
        self,
        own_estimates: dict[str, float],
        segments: list[str],
        *,
        data_type: DataType = DataType.PD,
        divergence_threshold: float = 0.30,
    ) -> GovernanceReport:
        """Compare own PD/LGD per segment against peer median; flag >30% divergence."""
        findings: list[dict[str, Any]] = []
        flags: list[str] = []

        for seg in segments:
            peer_entries = self._registry.get_by_segment(
                asset_class=seg, data_type=data_type,
            )
            if not peer_entries:
                findings.append({
                    "segment": seg,
                    "status": "no_peer_data",
                })
                continue
            if seg not in own_estimates:
                findings.append({
                    "segment": seg,
                    "status": "no_own_estimate",
                    "peer_count": len(peer_entries),
                })
                continue

            peer_values = [e.value for e in peer_entries]
            peer_median = statistics.median(peer_values)
            own = own_estimates[seg]
            if peer_median == 0:
                divergence = 0.0
            else:
                divergence = abs(own - peer_median) / peer_median

            finding = {
                "segment": seg,
                "own": own,
                "peer_median": peer_median,
                "peer_count": len(peer_entries),
                "divergence": divergence,
                "breached": divergence > divergence_threshold,
            }
            findings.append(finding)
            if divergence > divergence_threshold:
                flags.append(f"divergence:{seg}:{divergence:.2%}")

        return GovernanceReport(
            report_type="peer_comparison",
            generated_at=_utcnow(),
            institution_type=self._inst,
            findings=findings,
            flags=flags,
        )

    # ------------------------------------------------------------------
    # 4. Coverage report
    # ------------------------------------------------------------------

    def coverage_report(
        self,
        segments: list[str],
        *,
        data_type: DataType = DataType.PD,
        min_sources: int = 2,
    ) -> GovernanceReport:
        """Flag segments with fewer than `min_sources` distinct external sources."""
        findings: list[dict[str, Any]] = []
        flags: list[str] = []

        for seg in segments:
            entries = self._registry.get_by_segment(
                asset_class=seg, data_type=data_type,
            )
            distinct_sources = {e.source_id for e in entries}
            count = len(distinct_sources)
            finding = {
                "segment": seg,
                "source_count": count,
                "meets_threshold": count >= min_sources,
                "min_sources": min_sources,
            }
            findings.append(finding)
            if count < min_sources:
                flags.append(f"low_coverage:{seg}:{count}")

        return GovernanceReport(
            report_type="coverage",
            generated_at=_utcnow(),
            institution_type=self._inst,
            findings=findings,
            flags=flags,
        )

    # ------------------------------------------------------------------
    # 5. Annual review package (institution-specific format)
    # ------------------------------------------------------------------

    def annual_review_package(
        self,
        segments: list[str],
        own_estimates: Optional[dict[str, float]] = None,
    ) -> GovernanceReport:
        """Aggregate stale + quality + coverage (+peer when own_estimates given).

        Bank institutions get an MRC (Model Risk Committee) header;
        private credit gets a Credit Committee header — per plan §8.
        """
        stale = self.stale_benchmark_report()
        quality = self.quality_assessment_report()
        coverage = self.coverage_report(segments)

        sections: list[dict[str, Any]] = [
            {"section": "stale", "findings": stale.findings, "flags": stale.flags},
            {"section": "quality", "findings": quality.findings, "flags": quality.flags},
            {"section": "coverage", "findings": coverage.findings, "flags": coverage.flags},
        ]
        combined_flags = list(stale.flags) + list(quality.flags) + list(coverage.flags)

        if own_estimates:
            peer = self.peer_comparison_report(own_estimates, segments)
            sections.append({
                "section": "peer_comparison",
                "findings": peer.findings, "flags": peer.flags,
            })
            combined_flags.extend(peer.flags)

        # Pillar 3 cross-bank divergence — runs regardless of own_estimates.
        # Different question from peer_comparison: answers "is any Big 4 bank
        # an outlier vs its peers?" rather than "does our own PD match the
        # peer median?".
        pillar3 = self.pillar3_peer_divergence_report()
        sections.append({
            "section": "pillar3_peer_divergence",
            "findings": pillar3.findings, "flags": pillar3.flags,
        })
        combined_flags.extend(pillar3.flags)

        committee = "MRC" if self._inst == InstitutionType.BANK else "Credit Committee"
        sections.insert(0, {
            "section": "header",
            "committee": committee,
            "institution_type": self._inst.value,
            "segments_reviewed": segments,
        })

        return GovernanceReport(
            report_type="annual_review_package",
            generated_at=_utcnow(),
            institution_type=self._inst,
            findings=sections,
            flags=combined_flags,
        )

    # ------------------------------------------------------------------
    # 6b. Pillar 3 peer divergence — wires Pillar3BaseScraper.peer_comparison_flag
    # into the governance read path so MRC / Credit Committee see Big-4 outliers.
    # ------------------------------------------------------------------

    def pillar3_peer_divergence_report(
        self,
        *,
        threshold_multiple: float = 3.0,
        min_cohort_size: int = 3,
    ) -> GovernanceReport:
        """Flag Big-4 Pillar 3 PDs/LGDs whose value diverges >3x from the peer median.

        Grouping: (asset_class, data_type, value_date). A group is only
        evaluated when at least `min_cohort_size` banks report a value — a
        2-bank median isn't meaningful.

        Matches `Pillar3BaseScraper.peer_comparison_flag` semantics so the
        governance output agrees with the scrape-time warning logs.
        """
        from statistics import median

        entries = [
            e for e in self._registry.list(latest_only=True)
            if e.source_type == SourceType.PILLAR3
        ]

        # Group entries by (asset_class, data_type, value_date).
        groups: dict[tuple, list] = {}
        for e in entries:
            key = (e.asset_class, e.data_type.value, e.value_date.isoformat())
            groups.setdefault(key, []).append(e)

        findings: list[dict[str, Any]] = []
        flags: list[str] = []

        for (asset_class, dt_value, period), group in sorted(groups.items()):
            if len(group) < min_cohort_size:
                findings.append({
                    "segment": asset_class,
                    "data_type": dt_value,
                    "period": period,
                    "cohort_size": len(group),
                    "status": "skipped_incomplete_cohort",
                    "min_cohort_size": min_cohort_size,
                })
                continue

            values = [e.value for e in group]
            peer_median = median(values)

            for e in group:
                if peer_median <= 0 or e.value < 0:
                    divergence_ratio = None
                    breached = False
                elif e.value == 0:
                    divergence_ratio = float("inf")
                    breached = True
                else:
                    divergence_ratio = max(
                        e.value / peer_median, peer_median / e.value,
                    )
                    breached = divergence_ratio > threshold_multiple

                finding = {
                    "segment": asset_class,
                    "data_type": dt_value,
                    "period": period,
                    "publisher": e.publisher,
                    "source_id": e.source_id,
                    "value": e.value,
                    "peer_median": peer_median,
                    "peer_count": len(group),
                    "divergence_ratio": divergence_ratio,
                    "breached": breached,
                }
                findings.append(finding)
                if breached:
                    flags.append(
                        f"pillar3_divergence:{e.source_id}:"
                        f"{e.value:.4f}_vs_median_{peer_median:.4f}"
                    )

        return GovernanceReport(
            report_type="pillar3_peer_divergence",
            generated_at=_utcnow(),
            institution_type=self._inst,
            findings=findings,
            flags=flags,
        )

    # ------------------------------------------------------------------
    # 7. Version drift report
    # ------------------------------------------------------------------

    def version_drift_report(self, source_id: str) -> GovernanceReport:
        """Show value evolution across all versions of `source_id`."""
        history = self._registry.get_version_history(source_id)
        findings: list[dict[str, Any]] = []
        for entry in history:
            findings.append({
                "version": entry.version,
                "value": entry.value,
                "value_date": entry.value_date.isoformat(),
                "retrieval_date": entry.retrieval_date.isoformat(),
                "superseded_by": entry.superseded_by,
            })

        flags: list[str] = []
        if len(history) >= 2:
            # Flag if cumulative absolute change exceeds 30% of the original value.
            original = history[0].value
            latest = history[-1].value
            if original != 0 and abs(latest - original) / abs(original) > 0.30:
                flags.append(f"drift:{source_id}:{abs(latest-original)/abs(original):.2%}")

        return GovernanceReport(
            report_type="version_drift",
            generated_at=_utcnow(),
            institution_type=self._inst,
            findings=findings,
            flags=flags,
        )


# ---------------------------------------------------------------------------
# DOCX export (lazy import; clear error when dependency missing)
# ---------------------------------------------------------------------------

def export_to_docx(
    report: GovernanceReport,
    output_path: Path | str,
    institution_type: Optional[str] = None,
) -> Path:
    """Write `report` to a Word document formatted for the relevant committee.

    `institution_type` selects the template:
      - "bank"            MRC format with 3 Lines of Defence sign-off tables
      - "private_credit"  Credit Committee format with decision log + next actions
      - None              defaults to `report.institution_type` from the Pydantic model

    The actual python-docx work lives in `reports/docx_helpers.py` — this
    function is a thin shim so callers who only want the core engine don't
    pay the import cost of python-docx unless they request a DOCX export.
    """
    try:
        from reports.docx_helpers import (
            _require_docx,  # surfaces the shared "install python-docx" hint
            new_document,
            add_heading,
            add_paragraph,
            add_table,
            add_bullet,
            set_footer,
            add_3lod_signoff,
            add_decision_log,
            add_next_review_actions,
        )
    except ImportError as exc:
        raise ImportError(
            "DOCX export requires python-docx. Install with: "
            "pip install external_benchmark_engine[reports]"
        ) from exc

    # Trigger the shared import gate so the error message is consistent
    # whether python-docx is missing or only reports.docx_helpers is missing.
    _require_docx()

    resolved_inst = (institution_type or report.institution_type.value).lower()
    output_path = Path(output_path)

    if resolved_inst == "bank":
        title = f"External Benchmark Governance Report — {report.report_type}"
        subtitle = (
            f"Model Risk Committee — "
            f"{report.generated_at.strftime('%Y-%m-%d')}"
        )
    else:
        title = f"External Benchmark Report — {report.report_type}"
        subtitle = (
            f"Credit Committee — "
            f"{report.generated_at.strftime('%Y-%m-%d')}"
        )

    doc = new_document(title, subtitle=subtitle)

    # ---- Report summary ----
    add_heading(doc, "Report Summary", level=2)
    add_paragraph(doc, f"Report type: {report.report_type}")
    add_paragraph(doc, f"Institution type: {report.institution_type.value}")
    add_paragraph(doc, f"Generated at: {report.generated_at.isoformat()}")
    add_paragraph(doc, f"Flags raised: {len(report.flags)}")
    add_paragraph(doc, f"Findings: {len(report.findings)}")

    # ---- Methodology (brief) ----
    add_heading(doc, "Methodology", level=2)
    add_paragraph(
        doc,
        "This report is a read-only observation over the External Benchmark "
        "Engine registry. It does not trigger any adjustments or writes — "
        "all findings are derived from latest-version registry entries. "
        "Refresh cadences per source type are defined in "
        "config/refresh_schedules.yaml.",
    )

    # ---- Flags ----
    if report.flags:
        add_heading(doc, "Flags", level=2)
        for flag in report.flags:
            add_bullet(doc, flag)

    # ---- Findings table ----
    add_heading(doc, "Findings", level=2)
    findings = list(report.findings)
    if findings:
        # Use the union of keys across the first few rows as the column set.
        column_keys: list[str] = []
        for row in findings[:20]:
            if isinstance(row, dict):
                for k in row.keys():
                    if k not in column_keys:
                        column_keys.append(k)
        if column_keys:
            add_table(
                doc,
                headers=column_keys,
                rows=[
                    [_stringify(row.get(k)) for k in column_keys]
                    for row in findings if isinstance(row, dict)
                ],
            )
        else:
            for row in findings:
                add_paragraph(doc, str(row))
    else:
        add_paragraph(doc, "No findings recorded.", italic=True)

    # ---- Committee-specific sign-off sections ----
    if resolved_inst == "bank":
        add_3lod_signoff(doc)
    else:
        add_decision_log(doc)
        add_next_review_actions(doc)

    set_footer(doc, "Generated by External Benchmark Engine")
    doc.save(str(output_path))
    return output_path


def _stringify(v: Any) -> str:
    """Compact, display-friendly representation for DOCX table cells."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return "yes" if v else "no"
    if isinstance(v, float):
        if v != v:   # NaN guard
            return ""
        return f"{v:.4g}"
    if isinstance(v, (list, tuple)):
        return ", ".join(str(x) for x in v)
    return str(v)
