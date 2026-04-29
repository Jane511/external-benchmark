"""Macquarie Pillar 3 scraper integration tests."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from ingestion.refresh import RefreshOrchestrator
from src.db import create_engine_and_schema
from src.models import SourceType
from src.registry import BenchmarkRegistry


def test_mqg_orchestrator_writes_macquarie_pillar3_entries(
    sources_config: dict, tmp_path: Path,
) -> None:
    pdf_path = tmp_path / "mqg.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n%%EOF\n")

    canned = pd.DataFrame([
        {
            "asset_class": "residential_mortgage", "metric_name": "pd",
            "value": 0.001, "as_of_date": date(2025, 9, 30),
            "period_code": "H1FY2026", "value_basis": "exposure_weighted",
            "source_table": "CR6", "source_page": 25, "pd_band": "0.00 to <0.15",
        },
        {
            "asset_class": "residential_mortgage", "metric_name": "lgd",
            "value": 0.116, "as_of_date": date(2025, 9, 30),
            "period_code": "H1FY2026", "value_basis": "exposure_weighted",
            "source_table": "CR6", "source_page": 25, "pd_band": "0.00 to <0.15",
        },
        {
            "asset_class": "development_satisfactory", "metric_name": "risk_weight",
            "value": 1.15, "as_of_date": date(2025, 9, 30),
            "period_code": "H1FY2026", "value_basis": "supervisory_prescribed",
            "source_table": "CR10", "source_page": 30, "pd_band": "all",
        },
    ])

    engine = create_engine_and_schema(":memory:")
    registry = BenchmarkRegistry(engine, actor="test")
    orch = RefreshOrchestrator(
        registry=registry,
        sources_config=sources_config,
        local_overrides={"mqg_pillar3": pdf_path},
        scraper_extras={"mqg_pillar3": {"reporting_date": date(2025, 9, 30)}},
    )

    with patch(
        "ingestion.adapters.mqg_pillar3_pdf_adapter.MqgPillar3PdfAdapter.normalise",
        return_value=canned,
    ):
        report = orch.refresh_source("mqg_pillar3")

    assert report.errors == []
    assert report.counts.get("add") == 3
    entries = registry.list()
    assert {e.publisher for e in entries} == {"Macquarie Bank"}
    assert all(e.source_type == SourceType.PILLAR3 for e in entries)
    assert any(e.source_id.startswith("MACQUARIE_BANK_") for e in entries)

