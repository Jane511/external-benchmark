"""Tests for the rewritten raw-only BenchmarkCalibrationReport (Brief 1)."""
from __future__ import annotations

from datetime import date, timedelta

import pytest

from reports.benchmark_report import BenchmarkCalibrationReport, RAW_ONLY_BANNER
from src.db import create_engine_and_schema
from src.models import RawObservation, SourceType
from src.observations import PeerObservations
from src.registry import BenchmarkRegistry


@pytest.fixture()
def populated_registry() -> BenchmarkRegistry:
    engine = create_engine_and_schema(":memory:")
    reg = BenchmarkRegistry(engine, actor="test")
    today = date(2026, 4, 27)
    reg.add_observations([
        RawObservation(
            source_id="cba", source_type=SourceType.BANK_PILLAR3,
            segment="commercial_property", parameter="pd",
            value=0.025, as_of_date=today - timedelta(days=30),
            reporting_basis="Pillar 3 trailing 4-quarter average",
            methodology_note="CR6 EAD-weighted Average PD",
            page_or_table_ref="CR6 row 4",
        ),
        RawObservation(
            source_id="judo", source_type=SourceType.NON_BANK_LISTED,
            segment="commercial_property", parameter="pd",
            value=0.045, as_of_date=today - timedelta(days=90),
            reporting_basis="Half-yearly disclosure",
            methodology_note="Average PD on commercial real estate book",
        ),
    ])
    return reg


def test_generate_returns_raw_only_sections(populated_registry):
    report = BenchmarkCalibrationReport(populated_registry, period_label="Q1 2026")
    data = report.generate()
    expected_keys = {
        "meta", "banner", "executive_summary", "per_source_observations",
        "validation_summary", "big4_vs_nonbank_spread", "provenance",
    }
    assert expected_keys.issubset(data.keys())


def test_generate_does_not_include_adjusted_or_triangulated_sections(populated_registry):
    report = BenchmarkCalibrationReport(populated_registry, period_label="Q1 2026")
    data = report.generate()
    forbidden = {
        "adjustment_audit_trail", "triangulated_values", "calibration_outputs",
        "downturn_lgd", "bank_vs_pc_comparison", "data_governance",
    }
    assert not (forbidden & data.keys()), (
        "Brief 1 requires raw-only output — no adjustment / triangulation sections."
    )


def test_per_source_observations_carry_full_attribution(populated_registry):
    report = BenchmarkCalibrationReport(populated_registry, period_label="Q1 2026")
    data = report.generate()
    blocks = data["per_source_observations"]
    assert any(b["segment"] == "commercial_property" for b in blocks)
    cre = next(b for b in blocks if b["segment"] == "commercial_property")
    cba = next(o for o in cre["observations"] if o["source_id"] == "cba")
    assert cba["as_of_date"]
    assert cba["reporting_basis"]
    assert cba["methodology_note"]
    assert cba["page_or_table_ref"] == "CR6 row 4"


def test_markdown_contains_raw_only_banner(populated_registry):
    report = BenchmarkCalibrationReport(populated_registry, period_label="Q1 2026")
    md = report.to_markdown()
    assert RAW_ONLY_BANNER in md
    assert "## 2. Per-source raw observations by segment" in md
    assert "## 3. Cross-source validation summary" in md
    assert "## 4. Big 4 vs non-bank disclosure spread (informational only)" in md


def test_html_renders_with_banner(populated_registry):
    report = BenchmarkCalibrationReport(populated_registry, period_label="Q1 2026")
    html = report.to_html()
    assert "<title>External Benchmark Report" in html
    assert "raw, source-attributable observations only" in html
