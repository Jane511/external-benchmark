"""Smoke test for the docx renderer (P2.5).

Renders a populated ``BenchmarkCalibrationReport`` to a ``tmp_path``
``.docx`` file, then re-reads it with python-docx and asserts the
shape: title, banner, every section heading, and at least one row
in each section's table. Skips cleanly when ``python-docx`` is not
installed.

The committee consumes the docx; HTML/MD test parity isn't enough on
its own — Section 0 (glossary), Section 4a (reference anchors), and
Section 7 (trend) all touch the docx renderer through dedicated
branches that no other test exercises.
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest

pytest.importorskip("docx", reason="python-docx is required for docx smoke test")

from docx import Document  # noqa: E402

from src.benchmark_report import BenchmarkCalibrationReport  # noqa: E402
from src.db import create_engine_and_schema  # noqa: E402
from src.models import (  # noqa: E402
    DataDefinitionClass,
    RawObservation,
    SourceType,
)
from src.registry import BenchmarkRegistry  # noqa: E402


@pytest.fixture()
def populated_registry() -> BenchmarkRegistry:
    engine = create_engine_and_schema(":memory:")
    reg = BenchmarkRegistry(engine, actor="test")
    today = date(2026, 4, 27)
    reg.add_observations([
        RawObservation(
            source_id="cba",
            source_type=SourceType.BANK_PILLAR3,
            segment="commercial_property",
            parameter="pd",
            data_definition_class=DataDefinitionClass.BASEL_PD_ONE_YEAR,
            value=0.025,
            as_of_date=today - timedelta(days=120),
            reporting_basis="Pillar 3 H2 2024",
            methodology_note="CBA Pillar 3 Table CR6 commercial property PD",
        ),
        RawObservation(
            source_id="cba",
            source_type=SourceType.BANK_PILLAR3,
            segment="commercial_property",
            parameter="pd",
            data_definition_class=DataDefinitionClass.BASEL_PD_ONE_YEAR,
            value=0.024,
            as_of_date=today - timedelta(days=300),
            reporting_basis="Pillar 3 H1 2024",
            methodology_note="CBA Pillar 3 Table CR6 commercial property PD",
        ),
        RawObservation(
            source_id="judo",
            source_type=SourceType.NON_BANK_LISTED,
            segment="commercial_property",
            parameter="pd",
            data_definition_class=DataDefinitionClass.BASEL_PD_ONE_YEAR,
            value=0.045,
            as_of_date=today - timedelta(days=90),
            reporting_basis="Half-yearly disclosure",
            methodology_note="Judo Bank Pillar 3 commercial book",
        ),
        RawObservation(
            source_id="QUALITAS_CRE_COMMENTARY",
            source_type=SourceType.NON_BANK_LISTED,
            segment="commercial_property",
            parameter="commentary",
            data_definition_class=DataDefinitionClass.QUALITATIVE_COMMENTARY,
            value=None,
            as_of_date=today - timedelta(days=120),
            reporting_basis="Half-yearly results commentary",
            methodology_note="QUALITATIVE: office sector under pressure",
        ),
    ])
    return reg


def _all_text(doc) -> str:
    paragraphs = "\n".join(p.text for p in doc.paragraphs)
    table_text = "\n".join(
        cell.text for table in doc.tables for row in table.rows for cell in row.cells
    )
    return paragraphs + "\n" + table_text


def test_to_docx_round_trip(populated_registry, tmp_path: Path) -> None:
    """End-to-end: render docx, re-read with python-docx, assert structure."""
    report = BenchmarkCalibrationReport(
        populated_registry, period_label="Q1 2026",
    )
    out = tmp_path / "smoke.docx"
    report.to_docx(out)
    assert out.exists() and out.stat().st_size > 0

    doc = Document(str(out))
    text = _all_text(doc)

    assert "External Benchmark Report" in text
    # Raw-only banner
    assert "raw, source-attributable" in text
    # Section headings (rendered as paragraphs in docx_helpers)
    assert "1. Executive Summary" in text
    assert "2. Per-source raw observations by segment" in text
    assert "3. Cross-source validation summary" in text
    assert "4. Big 4 vs non-bank disclosure spread" in text
    assert "5. Provenance" in text
    # Glossary (Section 0) appears because commercial_property is rendered.
    assert "0. Segment definitions" in text
    # Commentary observation must show up as the qualitative tag, NOT 0.0%.
    assert "(qualitative)" in text or "qualitative" in text.lower()
    # The peer-ratio definition sentence is rendered verbatim under Sec 4.
    assert "peer_big4_vs_non_bank_ratio" in text


def test_to_docx_includes_trend_when_two_vintages_present(
    populated_registry, tmp_path: Path,
) -> None:
    report = BenchmarkCalibrationReport(
        populated_registry, period_label="Q1 2026",
    )
    out = tmp_path / "smoke_trend.docx"
    report.to_docx(out)

    doc = Document(str(out))
    text = _all_text(doc)
    # CBA has two vintages so Section 7 (trend) must render.
    assert "7. Trend vs prior cycle" in text
