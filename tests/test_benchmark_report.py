"""Tests for the lean private-credit model input report."""
from __future__ import annotations

from datetime import date

import pytest

from src.benchmark_report import BenchmarkCalibrationReport
from src.db import create_engine_and_schema
from src.models import DataDefinitionClass, RawObservation, SourceType
from src.registry import BenchmarkRegistry


@pytest.fixture()
def populated_registry() -> BenchmarkRegistry:
    engine = create_engine_and_schema(":memory:")
    reg = BenchmarkRegistry(engine, actor="test")
    reg.add_observations([
        RawObservation(
            source_id="cba",
            source_type=SourceType.BANK_PILLAR3,
            segment="commercial_property",
            parameter="pd",
            data_definition_class=DataDefinitionClass.BASEL_PD_ONE_YEAR,
            value=0.025,
            as_of_date=date(2025, 12, 31),
            reporting_basis="Pillar 3",
            methodology_note="CR6 EAD-weighted Average PD",
        ),
        RawObservation(
            source_id="aps113_cre_lgd",
            source_type=SourceType.REGULATORY,
            segment="commercial_property",
            parameter="lgd",
            data_definition_class=DataDefinitionClass.REGULATORY_FLOOR_LGD,
            value=0.175,
            as_of_date=date(2025, 12, 31),
            reporting_basis="APS 113",
            methodology_note="Commercial-property LGD floor",
        ),
        RawObservation(
            source_id="apra_qpex_cre_npl",
            source_type=SourceType.APRA_QPEX,
            segment="commercial_property",
            parameter="npl",
            data_definition_class=DataDefinitionClass.NPL_RATIO,
            value=0.018,
            as_of_date=date(2025, 12, 31),
            reporting_basis="APRA QPEX",
            methodology_note="Commercial-property NPL ratio",
        ),
    ])
    return reg


def test_generate_returns_only_model_input_sections(populated_registry) -> None:
    report = BenchmarkCalibrationReport(populated_registry, period_label="Q1 2026")
    data = report.generate()
    assert set(data) == {
        "meta",
        "pd_inputs",
        "lgd_inputs",
        "expected_loss_inputs",
        "stress_testing_inputs",
        "portfolio_monitor_inputs",
        "bank_industry_inputs",
    }
    assert data["expected_loss_inputs"][0]["expected_loss_rate_decimal"] == pytest.approx(
        0.004375
    )
    assert data["bank_industry_inputs"] == []


def test_markdown_contains_direct_inputs_and_excludes_audit_sections(
    populated_registry,
) -> None:
    report = BenchmarkCalibrationReport(populated_registry, period_label="Q1 2026")
    md = report.to_markdown()
    assert "# Australian Credit Risk Benchmarks - Q1 2026" in md
    assert "## 1. PD Inputs" in md
    assert "## 2. LGD Inputs" in md
    assert "## 3. Expected Loss Inputs" in md
    assert "## 4. Stress Testing Inputs" in md
    assert "## 5. Portfolio Monitor Inputs" in md
    assert "## 6. Per-Bank Industry Inputs" in md
    assert "PD decimal" in md
    assert "%" not in md
    assert "0.025000" not in md
    assert "0.03" in md
    assert "Methodology" not in md
    assert "Provenance" not in md
    assert "Raw data inventory" not in md
    assert "Glossary" not in md


def test_markdown_includes_bank_industry_rows(
    populated_registry,
    tmp_path,
    monkeypatch,
) -> None:
    from src import model_inputs

    monkeypatch.setattr(
        model_inputs,
        "build_bank_industry_input_rows",
        lambda raw_data_dir: [{
            "bank_code": "cba",
            "bank": "CBA",
            "industry": "Construction",
            "exposure_aud_m": 12345.678,
            "npe_aud_m": 234.567,
            "npe_rate_decimal": 0.018999,
            "provision_aud_m": 45.678,
            "write_offs_aud_m": 12.345,
            "write_off_rate_decimal": 0.000999,
            "as_of_date": "2025-06-30",
        }],
    )

    report = BenchmarkCalibrationReport(
        populated_registry,
        period_label="Q1 2026",
        raw_data_dir=tmp_path,
    )
    md = report.to_markdown()

    assert "| CBA | Construction | 12345.68 | 234.57 | 0.02 | 45.68 | 12.35 | 0.00 | 2025-06-30 |" in md
    assert "12345.678" not in md


def test_stress_inputs_are_model_ready_numbers(populated_registry) -> None:
    report = BenchmarkCalibrationReport(populated_registry, period_label="Q1 2026")
    stress = report.generate()["stress_testing_inputs"][0]
    assert stress["base_expected_loss_rate_decimal"] == pytest.approx(0.004375)
    assert stress["stressed_pd_decimal"] == pytest.approx(0.05)
    assert stress["stressed_lgd_decimal"] == pytest.approx(0.21)
    assert stress["stressed_expected_loss_rate_decimal"] == pytest.approx(0.0105)


def test_html_renders_lean_report(populated_registry) -> None:
    report = BenchmarkCalibrationReport(populated_registry, period_label="Q1 2026")
    html = report.to_html()
    assert "<title>Australian Credit Risk Benchmarks - Q1 2026</title>" in html
    assert "Expected Loss Inputs" in html
    assert "raw, source-attributable observations only" not in html
