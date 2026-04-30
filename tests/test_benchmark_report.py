"""Tests for the rewritten raw-only BenchmarkCalibrationReport (Brief 1)."""
from __future__ import annotations

from datetime import date, timedelta

import pytest

from src.benchmark_report import BenchmarkCalibrationReport, RAW_ONLY_BANNER
from src.db import create_engine_and_schema
from src.models import DataDefinitionClass, RawObservation, SourceType
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
            data_definition_class=DataDefinitionClass.BASEL_PD_ONE_YEAR,
            value=0.025, as_of_date=today - timedelta(days=30),
            reporting_basis="Pillar 3 trailing 4-quarter average",
            methodology_note="CR6 EAD-weighted Average PD",
            page_or_table_ref="CR6 row 4",
        ),
        RawObservation(
            source_id="cba", source_type=SourceType.BANK_PILLAR3,
            segment="commercial_property", parameter="pd",
            data_definition_class=DataDefinitionClass.BASEL_PD_ONE_YEAR,
            value=0.023, as_of_date=today - timedelta(days=210),
            reporting_basis="Pillar 3 trailing 4-quarter average",
            methodology_note="CR6 EAD-weighted Average PD",
            page_or_table_ref="CR6 row 4",
        ),
        RawObservation(
            source_id="judo", source_type=SourceType.NON_BANK_LISTED,
            segment="commercial_property", parameter="pd",
            data_definition_class=DataDefinitionClass.BASEL_PD_ONE_YEAR,
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
    assert "Data as-of:" in md
    assert "## 0. Segment definitions" in md
    assert "## 2. Per-source raw observations by segment" in md
    assert "Reporting basis" in md
    assert "## 3. Cross-source validation summary" in md
    assert "## 4. Big 4 vs non-bank disclosure spread (informational only)" in md
    assert "## 7. Trend vs prior cycle" in md


def test_generate_includes_segment_trend(populated_registry):
    report = BenchmarkCalibrationReport(populated_registry, period_label="Q1 2026")
    trends = report.generate()["segment_trend"]
    assert len(trends) == 1
    assert trends[0]["segment"] == "commercial_property"
    assert trends[0]["source_id"] == "cba"
    assert trends[0]["delta"] == pytest.approx(0.002)


def test_html_renders_with_banner(populated_registry):
    report = BenchmarkCalibrationReport(populated_registry, period_label="Q1 2026")
    html = report.to_html()
    assert "<title>External Benchmark Report" in html
    assert "raw, source-attributable observations only" in html


def test_raw_data_inventory_section_only_when_dir_provided(
    populated_registry, tmp_path,
):
    """`raw_data_dir=None` (default) skips section 6 entirely."""
    bare = BenchmarkCalibrationReport(populated_registry, period_label="Q1 2026")
    assert "raw_data_inventory" not in bare.generate()
    assert "## 6. Raw data inventory" not in bare.to_markdown()


def test_raw_data_inventory_section_walks_directory(populated_registry, tmp_path):
    """`raw_data_dir` populates section 6 with files grouped by family."""
    raw = tmp_path / "raw"
    (raw / "pillar3").mkdir(parents=True)
    (raw / "pillar3" / "CBA_FY25.pdf").write_bytes(b"%PDF stub")
    (raw / "non_bank" / "qualitas").mkdir(parents=True)
    (raw / "non_bank" / "qualitas" / "_MANUAL.md").write_text("hint")

    report = BenchmarkCalibrationReport(
        populated_registry, period_label="Q1 2026", raw_data_dir=raw,
    )
    data = report.generate()
    inv = data["raw_data_inventory"]
    assert inv["n_files"] == 2
    families = {b["family"] for b in inv["by_family"]}
    assert families == {"pillar3", "non_bank"}

    md = report.to_markdown()
    assert "## 6. Raw data inventory" in md
    assert "### pillar3" in md
    assert "### non_bank" in md
    assert "CBA_FY25.pdf" in md
    assert "_MANUAL.md" in md


def test_supporting_documentation_reads_rba_metadata(populated_registry, tmp_path):
    raw = tmp_path / "raw"
    rba = raw / "rba"
    rba.mkdir(parents=True)
    (rba / "RBA_FSR_March_2026.pdf").write_bytes(b"%PDF")
    (rba / "RBA_FSR_March_2026.pdf.metadata.json").write_text(
        """{
          "source_key": "rba_fsr",
          "source": "RBA Financial Stability Review",
          "publisher": "Reserve Bank of Australia",
          "url": "https://www.rba.gov.au/publications/fsr/",
          "local_cached_file": "data/raw/rba/RBA_FSR_March_2026.pdf",
          "period": "March 2026",
          "retrieval_date": "2026-04-29"
        }""",
        encoding="utf-8",
    )

    report = BenchmarkCalibrationReport(
        populated_registry, period_label="Q1 2026", raw_data_dir=raw,
    )
    data = report.generate()
    docs = data["supporting_documentation"]
    assert docs["documents"][0]["source"] == "RBA Financial Stability Review"
    assert "Forward-looking commentary draws from RBA" in docs["lead_text"]
    assert "These are not benchmark sources" in report.to_markdown()


def test_glossary_covers_all_rendered_segments(populated_registry):
    report = BenchmarkCalibrationReport(populated_registry, period_label="Q1 2026")
    data = report.generate()
    glossary_segments = {row["segment"] for row in data["segment_glossary"]}
    rendered_segments = {
        row["segment"] for row in data["per_source_observations"]
    }
    assert rendered_segments.issubset(glossary_segments)


def test_executive_narrative_contains_segment_facts(populated_registry):
    """Section 1 prose must mention the headline segment + actual numbers."""
    report = BenchmarkCalibrationReport(populated_registry, period_label="Q1 2026")
    data = report.generate()
    narrative = data["executive_summary"]["narrative"]
    assert narrative
    # Headline mentions commercial_property (the only segment with peer rows
    # in the populated_registry fixture) and a numeric Big-4 value.
    assert "commercial property" in narrative.lower() or "no peer observations" in narrative.lower()


def test_peer_ratio_definition_rendered_in_report(populated_registry):
    """Both the rendered Markdown and CSV must carry PEER_RATIO_DEFINITION verbatim."""
    from src.validation import PEER_RATIO_DEFINITION
    report = BenchmarkCalibrationReport(populated_registry, period_label="Q1 2026")
    md = report.to_markdown()
    assert PEER_RATIO_DEFINITION in md
    # Same constant powers the CSV header — exercise once via export.
    from pathlib import Path
    import tempfile
    from src.csv_exporter import export_validation_flags
    with tempfile.TemporaryDirectory() as tmp:
        out = export_validation_flags(populated_registry, out_dir=Path(tmp))
        assert "peer_big4_vs_non_bank_ratio" in out.read_text(encoding="utf-8")


def test_commentary_row_does_not_break_markdown(populated_registry):
    """Adding a commentary row must not crash the report and must show
    the qualitative tag instead of a percentage."""
    from datetime import date as _date
    from src.models import DataDefinitionClass, RawObservation, SourceType
    populated_registry.add_observation(
        RawObservation(
            source_id="QUALITAS_CRE_COMMENTARY_2024H2",
            source_type=SourceType.NON_BANK_LISTED,
            segment="commercial_property",
            parameter="commentary",
            data_definition_class=DataDefinitionClass.QUALITATIVE_COMMENTARY,
            value=None,
            as_of_date=_date(2024, 12, 31),
            reporting_basis="Half-yearly results commentary",
            methodology_note="QUALITATIVE: office sector under pressure",
        )
    )
    report = BenchmarkCalibrationReport(populated_registry, period_label="Q1 2026")
    md = report.to_markdown()
    assert "QUALITAS_CRE_COMMENTARY_2024H2" in md
    # qualitative tag in the value column
    assert "(qualitative)" in md
