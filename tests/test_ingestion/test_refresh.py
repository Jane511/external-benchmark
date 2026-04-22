"""Tests for ingestion/refresh.py — orchestrator + conflict resolution."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from ingestion.refresh import RefreshOrchestrator
from src.db import create_engine_and_schema
from src.registry import BenchmarkRegistry


@pytest.fixture()
def orchestrator_with_fixture(apra_xlsx_fixture: Path, sources_config: dict):
    engine = create_engine_and_schema(":memory:")
    registry = BenchmarkRegistry(engine, actor="test")
    orch = RefreshOrchestrator(
        registry=registry,
        sources_config=sources_config,
        local_overrides={
            "apra_adi_performance": apra_xlsx_fixture,
            "apra_qpex": apra_xlsx_fixture,   # same file, different sheet
        },
    )
    return orch, registry


def test_refresh_source_adds_new_entries(orchestrator_with_fixture) -> None:
    orch, registry = orchestrator_with_fixture
    report = orch.refresh_source("apra_adi_performance")

    assert report.errors == []
    assert report.counts.get("add", 0) == 6    # 3 cats x 2 metrics
    assert report.counts.get("supersede", 0) == 0

    entries = registry.list()
    assert len(entries) == 6
    source_ids = {e.source_id for e in entries}
    assert any(sid.startswith("APRA_RESIDENTIAL_MORTGAGE_") for sid in source_ids)


def test_refresh_source_dry_run_does_not_write(orchestrator_with_fixture) -> None:
    orch, registry = orchestrator_with_fixture
    report = orch.refresh_source("apra_adi_performance", dry_run=True)
    assert report.dry_run is True
    assert report.counts.get("add", 0) == 6    # reports what WOULD happen
    assert len(registry.list()) == 0            # but nothing written


def test_refresh_twice_skips_unchanged(orchestrator_with_fixture) -> None:
    """Running the same scraper twice with identical data -> no duplicates."""
    orch, registry = orchestrator_with_fixture
    orch.refresh_source("apra_adi_performance")
    report2 = orch.refresh_source("apra_adi_performance")

    assert report2.counts.get("skip_unchanged", 0) == 6
    assert report2.counts.get("add", 0) == 0
    assert len(registry.list()) == 6


def test_refresh_supersedes_when_value_changes(
    tmp_path: Path, sources_config: dict, apra_xlsx_fixture: Path,
) -> None:
    """Same value_date but different value -> supersede (correction path)."""
    import openpyxl

    engine = create_engine_and_schema(":memory:")
    registry = BenchmarkRegistry(engine, actor="test")

    # First pass: use the canonical fixture
    orch1 = RefreshOrchestrator(
        registry=registry, sources_config=sources_config,
        local_overrides={"apra_adi_performance": apra_xlsx_fixture},
    )
    orch1.refresh_source("apra_adi_performance")

    # Second pass: build a corrected fixture with different Residential 90DPD value
    corrected = tmp_path / "apra_corrected.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Asset Quality"
    ws.append(["Period", "Category", "90DPD_Rate", "NPL_Rate"])
    ws.append([date(2025, 9, 30), "Residential", 0.015, 0.008])   # was 0.012
    ws.append([date(2025, 9, 30), "Commercial", 0.018, 0.012])
    ws.append([date(2025, 9, 30), "Corporate", 0.025, 0.015])
    wb.save(corrected)

    orch2 = RefreshOrchestrator(
        registry=registry, sources_config=sources_config,
        local_overrides={"apra_adi_performance": corrected},
    )
    report2 = orch2.refresh_source("apra_adi_performance")

    assert report2.counts.get("supersede", 0) == 1
    assert report2.counts.get("skip_unchanged", 0) == 5

    # Verify the supersede target has both versions
    res_90dpd_id = "APRA_RESIDENTIAL_MORTGAGE_90DPD_RATE_IMPAIRED_RATIO_2025Q3"
    history = registry.get_version_history(res_90dpd_id)
    assert len(history) == 2
    assert history[0].value == 0.012
    assert history[1].value == 0.015


def test_refresh_skips_when_registry_has_newer_date(
    sources_config: dict, apra_xlsx_fixture: Path,
) -> None:
    """Entry with older value_date than the registry's latest -> skip_newer_in_registry."""
    from src.models import BenchmarkEntry, DataType, QualityScore, SourceType

    engine = create_engine_and_schema(":memory:")
    registry = BenchmarkRegistry(engine, actor="test")

    # Seed registry directly with a newer entry.
    source_id = "APRA_RESIDENTIAL_MORTGAGE_90DPD_RATE_IMPAIRED_RATIO_2025Q4"
    registry.add(BenchmarkEntry(
        source_id=source_id, publisher="APRA",
        source_type=SourceType.APRA_ADI, data_type=DataType.IMPAIRED_RATIO,
        asset_class="residential_mortgage",
        value=0.013, value_date=date(2025, 12, 31),
        period_years=1, geography="AU",
        url="https://x", retrieval_date=date(2026, 2, 15),
        quality_score=QualityScore.HIGH,
    ))

    # Build an entry with the same source_id but an older value_date.
    older_entry = BenchmarkEntry(
        source_id=source_id, publisher="APRA",
        source_type=SourceType.APRA_ADI, data_type=DataType.IMPAIRED_RATIO,
        asset_class="residential_mortgage",
        value=0.011, value_date=date(2025, 9, 30),    # older than registry's Q4 entry
        period_years=1, geography="AU",
        url="https://x", retrieval_date=date(2026, 2, 16),
        quality_score=QualityScore.HIGH,
    )
    orch = RefreshOrchestrator(
        registry=registry, sources_config=sources_config,
        local_overrides={"apra_adi_performance": apra_xlsx_fixture},
    )
    action = orch._apply_entry(older_entry, dry_run=False)
    assert action.action == "skip_newer_in_registry"


def test_refresh_unknown_source_returns_error(orchestrator_with_fixture) -> None:
    orch, _ = orchestrator_with_fixture
    report = orch.refresh_source("not_a_real_source")
    assert report.errors
    assert "Unknown source" in report.errors[0]


def test_refresh_report_summary_is_non_empty(orchestrator_with_fixture) -> None:
    orch, _ = orchestrator_with_fixture
    report = orch.refresh_source("apra_adi_performance")
    assert "add=6" in report.summary()
