"""Tests for the Macquarie Bank Pillar 3 PDF adapter."""

from __future__ import annotations

from datetime import date

import pytest

from ingestion.adapters.mqg_pillar3_pdf_adapter import (
    MqgPillar3PdfAdapter,
    _derive_mqg_period_code,
)


_MQG_CR6_PAGE = (
    "MBL Basel III Pillar 3 Disclosures\n"
    "Table 7: CR6 - IRB - Credit risk exposures by portfolio and probability of default (PD) range\n"
    "30 September 2025\n"
    "Wholesale Portfolio\n"
    "PD Scale $m $m % $m % obligors % years $m % $m $m\n"
    "Corporate\n"
    "0.00 to <0.15 4,518 527 60.6 % 5,051 0.1 % 137 48.6 % 2.9 1,801 35.7 % 2\n"
    "100.00 (Non-\n"
    "Performing) 183 30 90.2 % 210 100.0 % 32 44.7 % 1.6 - - 94\n"
    "SME Corporate\n"
    "0.25 to <0.50 376 146 100.0 % 522 0.5 % 485 42.7 % 3.1 341 65.4 % 1\n"
    "Specialised lending - IPRE1\n"
    "0.75 to <2.50 2,842 343 100.0 % 3,185 1.2 % 496 21.5 % 2.5 1,942 61.0 % 10\n"
    "Retail Portfolio\n"
    "Residential Mortgage\n"
    "0.15 to <0.25 28,002 1,449 100.0 % 29,451 0.2 % 42,119 13.3 % 2,640 9.0 % 8\n"
)


def test_mqg_cr6_extracts_split_percent_tokens_and_non_performing_band() -> None:
    adapter = MqgPillar3PdfAdapter()
    rows = adapter._extract_cr6_page(
        _MQG_CR6_PAGE,
        page_num=23,
        reporting=date(2025, 9, 30),
        period_code="H1FY2026",
    )

    assert rows
    by_key = {
        (r["asset_class"], r["metric_name"], r["pd_band"]): r["value"]
        for r in rows
    }
    assert by_key[("corporate_general", "pd", "0.00 to <0.15")] == pytest.approx(0.001)
    assert by_key[("corporate_general", "lgd", "0.00 to <0.15")] == pytest.approx(0.486)
    assert by_key[("corporate_general", "pd", "100.00 (Non-Performing)")] == pytest.approx(1.0)


def test_mqg_cr6_maps_macquarie_portfolio_labels() -> None:
    adapter = MqgPillar3PdfAdapter()
    rows = adapter._extract_cr6_page(
        _MQG_CR6_PAGE,
        page_num=25,
        reporting=date(2025, 9, 30),
        period_code="H1FY2026",
    )
    classes = {r["asset_class"] for r in rows}
    assert "corporate_sme" in classes
    assert "commercial_property_investment" in classes
    assert "residential_mortgage" in classes


def test_mqg_cr10_emits_supervisory_risk_weights() -> None:
    adapter = MqgPillar3PdfAdapter()
    rows = adapter._extract_cr10_page(
        "Table 9: CR10 - IRB (specialised lending under the slotting approach)\n"
        "30 September 2025\n"
        "Strong 6 - 70 % 6 - - 6 4 -\n"
        "Good 1,280 414 90 % 607 1,093 - 1,700 1,531 14\n"
        "Satisfactory 1,878 917 115 % 1,870 911 - 2,781 3,199 78\n"
        "Weak 53 30 250 % 64 - - 64 161 5\n",
        page_num=30,
        reporting=date(2025, 9, 30),
        period_code="H1FY2026",
    )
    by_class = {r["asset_class"]: r["value"] for r in rows}
    assert by_class["development_strong"] == pytest.approx(0.70)
    assert by_class["development_good"] == pytest.approx(0.90)
    assert by_class["development_satisfactory"] == pytest.approx(1.15)
    assert by_class["development_weak"] == pytest.approx(2.50)


def test_mqg_period_code_uses_march_fiscal_year() -> None:
    assert _derive_mqg_period_code(date(2025, 3, 31)) == "FY2025"
    assert _derive_mqg_period_code(date(2025, 9, 30)) == "H1FY2026"

