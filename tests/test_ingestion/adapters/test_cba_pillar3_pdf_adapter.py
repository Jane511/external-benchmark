"""Tests for the CBA annual Pillar 3 PDF adapter (Option B).

Live-PDF dependency (pdfplumber) is heavy for unit tests, so these tests
drive the adapter's ``_extract_cr6_page`` / ``_extract_cr10_page`` methods
directly with synthesised page text. This keeps tests fast and avoids
committing binary PDFs.
"""
from __future__ import annotations

from datetime import date

import pytest

from ingestion.adapters.cba_pillar3_pdf_adapter import (
    _CR10_GRADES,
    CbaPillar3PdfAdapter,
)


# ---------------------------------------------------------------------------
# Synthetic CR6 page text — mirrors the FY2025 PDF layout
# ---------------------------------------------------------------------------

_CR6_PAGE = (
    "Credit Risk (continued)\n"
    "8.3 Portfolios subject to internal ratings-based approaches (continued)\n"
    "CR6: IRB - Credit risk exposures by portfolio and PD range\n"
    "30 June 2025\n"
    "Portfolio Type PD Range $M $M % $M % % Years $M % $M $M\n"
    "Corporate 0.00 to <0.15 6,430 2,279 41 7,336 0.07 260 28 2.7 1,460 20 2\n"
    "(incl. SME corporate) 0.15 to <0.25 15,946 3,885 43 17,617 0.21 696 27 2.5 6,311 36 10\n"
    "0.25 to <0.50 17,233 3,891 45 19,003 0.37 1,398 23 2.2 7,220 38 16\n"
    "100.00 (Default) 1,891 129 47 1,952 100.00 1,690 30 1.6 2,344 120 662\n"
    "Sub-total 165,350 35,717 50 183,130 2.71 41,510 25 1.9 96,933 53 1,423\n"
    "Residential mortgage 0.00 to <0.15 227,654 53,855 100 281,508 0.08 742,470 14 18,014 6 31\n"
    "0.15 to <0.25 101,884 12,498 100 114,382 0.18 302,015 16 11,223 10 32\n"
    "100.00 (Default) 6,187 15 100 6,202 100.00 16,499 20 13,389 216 285\n"
    "RBNZ regulated entities\n"
    "Non-retail 0.00 to <0.15 741 584 95 1,293 0.07 66 54 2.2 385 30 –\n"
    "0.75 to <2.50 11,191 1,815 81 12,667 1.14 4,910 25 1.8 6,568 52 37\n"
    "Other retail 0.00 to <0.15 1 33 99 34 0.10 9,943 107 10 29 –\n"  # 107% LGD implausible
)


_CR10_PAGE = (
    "Credit Risk (continued)\n"
    "CR10: IRB (specialised lending under the slotting approach)\n"
    "Regulatory categories Residual maturity …\n"
    "Strong Less than 2.5 years 965 384 70% 706 …\n"
    "Good Less than 2.5 years 3,615 775 90% 1,458 …\n"
    "Satisfactory 957 82 115% 153 …\n"
    "Weak 68 – 250% 32 …\n"
    "Default 208 – – 54 …\n"
)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_cr6_extracts_pd_and_lgd_per_band() -> None:
    adapter = CbaPillar3PdfAdapter()
    rows = adapter._extract_cr6_page(
        _CR6_PAGE, page_num=55, reporting=date(2025, 6, 30), period_code="FY2025",
    )
    assert rows, "CR6 extractor produced zero rows"
    metrics = {(r["asset_class"], r["metric_name"], r["pd_band"]) for r in rows}
    # Residential mortgage first band should be present with both PD and LGD.
    assert ("residential_mortgage", "pd", "0.00 to <0.15") in metrics
    assert ("residential_mortgage", "lgd", "0.00 to <0.15") in metrics


def test_cr6_assigns_corporate_sme_to_split_line_block() -> None:
    """'Corporate' + next line '(incl. SME corporate)' must be corporate_sme,
    not the 'corporate' catch-all → corporate_general."""
    adapter = CbaPillar3PdfAdapter()
    rows = adapter._extract_cr6_page(
        _CR6_PAGE, page_num=54, reporting=date(2025, 6, 30), period_code="FY2025",
    )
    classes_first_band = {
        r["asset_class"] for r in rows
        if r["pd_band"] == "0.00 to <0.15" and r["metric_name"] == "pd"
    }
    # "Corporate 0.00 to <0.15 …" row must be attributed to corporate_sme
    # via the parenthetical look-ahead.
    assert "corporate_sme" in classes_first_band


def test_cr6_rbnz_split_header_resolves() -> None:
    adapter = CbaPillar3PdfAdapter()
    rows = adapter._extract_cr6_page(
        _CR6_PAGE, page_num=56, reporting=date(2025, 6, 30), period_code="FY2025",
    )
    rbnz = [r for r in rows if r["asset_class"] == "rbnz_non_retail"]
    assert rbnz, "rbnz_non_retail split-line header not resolved"


def test_cr6_pd_values_converted_to_decimal() -> None:
    adapter = CbaPillar3PdfAdapter()
    rows = adapter._extract_cr6_page(
        _CR6_PAGE, page_num=55, reporting=date(2025, 6, 30), period_code="FY2025",
    )
    # "0.07" PD (%) → 0.0007 (decimal)
    match = [r for r in rows
             if r["asset_class"] == "corporate_sme"
             and r["metric_name"] == "pd"
             and r["pd_band"] == "0.00 to <0.15"]
    assert match
    assert match[0]["value"] == pytest.approx(0.0007, rel=1e-6)


def test_cr6_default_band_allows_100_percent_pd() -> None:
    adapter = CbaPillar3PdfAdapter()
    rows = adapter._extract_cr6_page(
        _CR6_PAGE, page_num=55, reporting=date(2025, 6, 30), period_code="FY2025",
    )
    defaults = [r for r in rows if r["pd_band"] == "100.00 (Default)"
                and r["metric_name"] == "pd"]
    assert defaults
    for r in defaults:
        assert r["value"] == pytest.approx(1.0)


def test_cr6_filters_implausible_lgd(caplog) -> None:
    import logging
    caplog.set_level(logging.WARNING)
    adapter = CbaPillar3PdfAdapter()
    rows = adapter._extract_cr6_page(
        _CR6_PAGE, page_num=55, reporting=date(2025, 6, 30), period_code="FY2025",
    )
    # The "Other retail 0.00 to <0.15 ... 107 ..." row has LGD=107% in
    # the source — must be filtered.
    implausible = [r for r in rows
                   if r["asset_class"] == "retail_other"
                   and r["metric_name"] == "lgd"
                   and r["pd_band"] == "0.00 to <0.15"]
    assert implausible == []
    assert any("implausible" in rec.message.lower() for rec in caplog.records)


def test_cr6_row_records_provenance() -> None:
    adapter = CbaPillar3PdfAdapter()
    rows = adapter._extract_cr6_page(
        _CR6_PAGE, page_num=55, reporting=date(2025, 6, 30), period_code="FY2025",
    )
    sample = rows[0]
    assert sample["source_table"] == "CR6"
    assert sample["source_page"] == 55
    assert sample["period_code"] == "FY2025"
    assert sample["value_basis"] == "exposure_weighted"


def test_cr10_emits_risk_weights_not_pds() -> None:
    adapter = CbaPillar3PdfAdapter()
    rows = adapter._extract_cr10_page(
        _CR10_PAGE, page_num=48, reporting=date(2025, 6, 30), period_code="FY2025",
    )
    assert len(rows) == len(_CR10_GRADES)
    assert all(r["metric_name"] == "risk_weight" for r in rows)
    by_class = {r["asset_class"]: r["value"] for r in rows}
    assert by_class["development_strong"] == pytest.approx(0.70)
    assert by_class["development_good"] == pytest.approx(0.90)
    assert by_class["development_satisfactory"] == pytest.approx(1.15)
    assert by_class["development_weak"] == pytest.approx(2.50)
    assert by_class["development_default"] == pytest.approx(0.0)


def test_cr10_records_supervisory_basis() -> None:
    adapter = CbaPillar3PdfAdapter()
    rows = adapter._extract_cr10_page(
        _CR10_PAGE, page_num=48, reporting=date(2025, 6, 30), period_code="FY2025",
    )
    assert all(r["value_basis"] == "supervisory_prescribed" for r in rows)


def test_validate_output_rejects_missing_column() -> None:
    import pandas as pd
    adapter = CbaPillar3PdfAdapter()
    bad = pd.DataFrame({"asset_class": ["x"]})
    with pytest.raises(ValueError, match="missing required columns"):
        adapter.validate_output(bad)


def test_period_code_derivation_for_cba_fiscal_year() -> None:
    from ingestion.adapters.cba_pillar3_pdf_adapter import _derive_cba_period_code
    assert _derive_cba_period_code(date(2025, 6, 30)) == "FY2025"
    assert _derive_cba_period_code(date(2024, 12, 31)) == "H1FY2025"
