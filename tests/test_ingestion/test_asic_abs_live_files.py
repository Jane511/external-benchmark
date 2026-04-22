"""Integration tests: ASICABSFailureRateImporter dual-path dispatch.

Existing CSV fixture tests in ``test_asic_abs_import.py`` cover the
fixture direct-read path. This file verifies:

1. XLSX inputs route through the adapters.
2. The adapter-derived DataFrame merges cleanly into a failure_rate.
3. MRC audit trail (Series-1 filter, sheet names) reaches
   ``ScrapedDataPoint.quality_indicators`` and therefore the final
   entry's ``notes`` string.
"""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import openpyxl
import pytest

from ingestion.asic_abs_import import ASICABSFailureRateImporter
from tests.test_ingestion.adapters.test_abs_business_counts_adapter import (
    _build_abs_workbook,
)
from tests.test_ingestion.adapters.test_asic_insolvency_adapter import (
    _HEADERS,
    _append_data_row,
)


def _build_matching_asic_workbook(path: Path) -> None:
    """Mirror the ABS synthetic fixture: Construction + Retail Trade, FY2024."""
    wb = openpyxl.Workbook()
    wb.active.title = "Contents"
    ws = wb.create_sheet("Data set")
    for _ in range(6):
        ws.append([None] * len(_HEADERS))
    ws.append(_HEADERS)

    # Construction: 4 Series-1 → failure_rate = 4 / 440000 ≈ 0.0000091
    for _ in range(4):
        _append_data_row(ws, appointment="Voluntary administration",
                         industry="Construction", fy="2023-2024", series1="Yes")
    # Retail Trade: 2 Series-1
    for _ in range(2):
        _append_data_row(ws, appointment="Creditors' voluntary liquidation",
                         industry="Retail Trade", fy="2023-2024", series1="Yes")
    # One subsequent appointment that must be filtered out
    _append_data_row(ws, appointment="Receiver appointed",
                     industry="Construction", fy="2023-2024", series1="No")
    wb.save(path)


@pytest.fixture()
def live_data_dirs(tmp_path: Path) -> tuple[Path, Path]:
    abs_dir = tmp_path / "abs"
    asic_dir = tmp_path / "asic"
    abs_dir.mkdir(); asic_dir.mkdir()
    _build_abs_workbook(abs_dir / "8165DC01.xlsx")
    _build_matching_asic_workbook(
        asic_dir / "asic-insolvency-statistics-series-1-and-series-2.xlsx",
    )
    return asic_dir, abs_dir


def test_importer_routes_xlsx_through_adapters(
    live_data_dirs: tuple[Path, Path],
) -> None:
    asic_dir, abs_dir = live_data_dirs
    importer = ASICABSFailureRateImporter(
        asic_dir=asic_dir, abs_dir=abs_dir,
        retrieval_date=date(2026, 4, 21),
    )
    points = importer.scrape()
    assert points, "importer produced zero points from live-shaped inputs"

    # Both expected (industry × FY) combinations present. The importer
    # normalises ABS labels through its ANZSIC map, so "Construction" →
    # "industry_construction" and so on.
    by_asset = {(p.asset_class_raw, p.value_date) for p in points}
    assert ("industry_construction", date(2024, 6, 30)) in by_asset
    assert ("industry_retail_trade", date(2024, 6, 30)) in by_asset


def test_importer_preserves_arithmetic_and_audit_trail(
    live_data_dirs: tuple[Path, Path],
) -> None:
    asic_dir, abs_dir = live_data_dirs
    importer = ASICABSFailureRateImporter(
        asic_dir=asic_dir, abs_dir=abs_dir,
        retrieval_date=date(2026, 4, 21),
    )
    points = importer.scrape()
    construction = next(
        p for p in points
        if p.asset_class_raw == "industry_construction"
        and p.value_date == date(2024, 6, 30)
    )

    qi = construction.quality_indicators
    # numerator/denominator preserved from fixture & adapter
    assert qi["insolvency_count"] == 4
    assert qi["business_count"] == 440_000
    # arithmetic formula + ASIC filter documented
    assert qi["arithmetic"].startswith("failure_rate = insolvency_count")
    assert qi["asic_filter"] == "Series 1 (companies entering)"
    # failure_rate matches 4 / 440000
    assert construction.raw_value == pytest.approx(4 / 440_000, rel=1e-6)


def test_importer_uses_csv_when_both_present(tmp_path: Path) -> None:
    """Fixture CSV takes precedence over live XLSX if both sit in the same dir."""
    abs_dir = tmp_path / "abs"; abs_dir.mkdir()
    asic_dir = tmp_path / "asic"; asic_dir.mkdir()

    # Canonical CSV fixtures.
    (abs_dir / "abs_business_counts.csv").write_text(
        "as_of_date,anzsic_division_code,industry,business_count,source_note\n"
        "2024-06-30,E,Construction,400000,fixture\n"
    )
    (asic_dir / "asic_insolvency_extract.csv").write_text(
        "as_of_date,industry,insolvency_count,source_note\n"
        "2024-06-30,Construction,3000,fixture\n"
    )
    # Also drop a live XLSX alongside — the CSV pattern is listed first,
    # so the importer should prefer the CSV and skip the XLSX entirely.
    _build_abs_workbook(abs_dir / "8165DC01.xlsx")
    _build_matching_asic_workbook(
        asic_dir / "asic-insolvency-statistics-series-1-and-series-2.xlsx",
    )

    importer = ASICABSFailureRateImporter(
        asic_dir=asic_dir, abs_dir=abs_dir,
        retrieval_date=date(2024, 12, 31),
    )
    points = importer.scrape()
    assert len(points) == 1
    qi = points[0].quality_indicators
    # CSV inputs were used — counts match the CSV fixture, not the XLSX.
    assert qi["insolvency_count"] == 3000
    assert qi["business_count"] == 400_000
    # CSV path does not carry an adapter filter tag.
    assert "asic_filter" not in qi
