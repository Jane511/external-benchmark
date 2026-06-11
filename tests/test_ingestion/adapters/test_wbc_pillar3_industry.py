"""Tests for WBC Pillar 3 industry-table extraction (Phase 3.B).

Layered:

1. **Pure-text parser tests** against synthetic CRB(e) / CRB(f) snippets
   that exercise the five Phase 3.B guardrails (provenance metadata,
   zero-vs-redacted, no synthetic alignment, no contributing_banks
   column, period-length metadata).
2. **Schema invariant tests** — every emitted row passes
   :func:`assert_row_well_formed`.
3. **Real-PDF smoke test** — round-trips
   ``data/raw/pillar3/WBC_FY2025_Pillar3_Annual.pdf`` and pins five
   golden values from the published table.

Existing WBC CR6 / CR10 / ``normalise`` tests are NOT touched (plan §2
preservation rule). The thin delegation method on
:class:`WbcPillar3PdfAdapter` is exercised in
``test_delegation_calls_extractor`` to confirm in-place extension wires
through correctly.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from ingestion.adapters.pillar3_industry_schema import (
    INDUSTRY_ROW_COLUMNS,
    METRIC_EXPOSURE,
    METRIC_NPE,
    METRIC_PROVISIONS,
    METRIC_WRITE_OFFS,
    REDACTION_DASH_OR_HYPHEN,
    assert_row_well_formed,
)
from ingestion.adapters.wbc_pillar3_industry import (
    WBC_INDUSTRIES,
    extract_wbc_industry_rows,
    parse_crb_e_text,
    parse_crb_f_text,
)
from ingestion.adapters.wbc_pillar3_pdf_adapter import WbcPillar3PdfAdapter


# ---------------------------------------------------------------------------
# Synthetic-text fixtures — small, focused, hermetic
# ---------------------------------------------------------------------------

# Trimmed CRB(e) snippet covering Australia + one row in NZ. Each
# industry data line follows: <label> <m1> <m2> <m3> <m4> <total>.
_CRB_E_MINI = """\
CRB(e): Exposures by geographical areas, industry and residual maturity
The following table presents the total exposure ...
$m < 12 months 1 to 5 years > 5 years No Specified Maturity Total Exposure
As at 30 September 2025
Australia
Accommodation, cafes and restaurants 2,656 9,265 410 229 12,560
Agriculture, forestry and fishing 4,304 11,182 527 2,344 18,357
Construction 1,617 7,428 1,698 1,023 11,766
Finance and insurance 40,350 9,235 800 3,064 53,449
Government, administration and defence 11,986 30,240 48,706 519 91,451
Manufacturing 2,603 9,264 822 2,074 14,763
Mining 809 3,508 298 731 5,346
Property 18,892 55,588 1,662 282 76,424
Property services and business services 2,688 12,791 2,283 1,817 19,579
Services 3,123 13,411 1,795 1,717 20,046
Trade 4,359 12,949 1,753 2,981 22,042
Transport and storage 1,670 11,624 1,958 561 15,813
Utilities 2,100 11,149 1,828 332 15,409
Retail lending 1,503 9,961 552,706 35,117 599,287
Other 309 987 386 9,602 11,284
Total Australia 98,969 208,582 617,632 62,393 987,576
New Zealand
Accommodation, cafes and restaurants 173 133 19 9 334
Agriculture, forestry and fishing 3,401 4,618 75 189 8,283
Construction 149 486 134 113 882
Finance and insurance 6,233 5,178 314 708 12,433
Government, administration and defence 1,826 4,144 1,761 79 7,810
Manufacturing 697 1,554 36 431 2,718
Mining 63 59 5 33 160
Property 2,643 6,528 140 27 9,338
Property services and business services 296 862 37 145 1,340
Services 454 1,956 68 176 2,654
Trade 1,220 1,581 156 1,446 4,403
Transport and storage 102 686 25 87 900
Utilities 451 2,092 290 247 3,080
Retail lending 187 1,296 69,742 2,239 73,464
Other 24 34 15 6 79
Total New Zealand 17,919 31,207 72,817 5,935 127,878
Other overseas
Accommodation, cafes and restaurants 21 32 30 - 83
Agriculture, forestry and fishing 1 - 1 - 2
Construction 24 36 10 - 70
Finance and insurance 10,306 4,714 1 129 15,150
Government, administration and defence 1,104 16,785 - - 17,889
Manufacturing 228 2,238 43 12 2,521
Mining 23 400 - - 423
Property 131 267 293 - 691
Property services and business services 117 1,164 200 - 1,481
Services 12 247 30 - 289
Trade 1,340 1,036 141 176 2,693
Transport and storage 44 322 852 - 1,218
Utilities 91 435 999 - 1,525
Retail lending 31 45 328 - 404
Other 36 29 28 - 93
Total other overseas 13,509 27,750 2,956 317 44,532
"""

_CRB_F_MINI = """\
CRB(f): Non-performing exposures by geographical areas and industry
The following table presents information ...
$m Australia New Zealand Other Overseas Total Australia New Zealand Other Overseas Total months ended
As at 30 September 2025
Accommodation, cafes and restaurants 203 4 3 210 (43) - (1) (44) 8
Agriculture, forestry and fishing 378 81 - 459 (68) (15) - (83) (4)
Construction 296 6 1 303 (67) (1) - (68) 24
Finance and insurance 78 - - 78 (14) - - (14) 4
Government, administration and defence - - - - - - - - -
Manufacturing 274 89 2 365 (107) (24) (1) (132) 10
Mining 32 - 1 33 (9) - - (9) 1
Property 924 6 35 965 (152) - (19) (171) 4
Property services and business services 408 19 5 432 (104) (3) (2) (109) 15
Services 346 15 1 362 (147) (7) - (154) 14
Trade 551 26 5 582 (152) (7) (2) (161) 101
Transport and storage 136 1 1 138 (53) - - (53) 9
Utilities 13 1 - 14 (3) - - (3) 1
Retail lending 5,412 719 15 6,146 (609) (81) (3) (693) 324
Other 37 3 - 40 (12) - - (12) 5
Total 9,088 970 69 10,127 (1,540) (138) (28) (1,706) 516
"""


# ---------------------------------------------------------------------------
# 1. Schema contract
# ---------------------------------------------------------------------------

def test_crb_e_emits_canonical_columns_only() -> None:
    rows = parse_crb_e_text(
        _CRB_E_MINI,
        source_publication="WBC Pillar 3 — September 2025",
        source_page=37,
    )
    df = pd.DataFrame.from_records(rows, columns=INDUSTRY_ROW_COLUMNS)
    assert list(df.columns) == INDUSTRY_ROW_COLUMNS
    assert "contributing_banks" not in df.columns  # guardrail 4


def test_crb_f_emits_canonical_columns_only() -> None:
    rows = parse_crb_f_text(
        _CRB_F_MINI,
        source_publication="WBC Pillar 3 — September 2025",
        source_page=38,
    )
    df = pd.DataFrame.from_records(rows, columns=INDUSTRY_ROW_COLUMNS)
    assert list(df.columns) == INDUSTRY_ROW_COLUMNS


# ---------------------------------------------------------------------------
# 2. Row counts (smoke / shape)
# ---------------------------------------------------------------------------

def test_crb_e_row_count() -> None:
    """15 industries × 3 geographies = 45 exposure rows."""
    rows = parse_crb_e_text(_CRB_E_MINI, source_publication="x", source_page=37)
    assert len(rows) == len(WBC_INDUSTRIES) * 3
    assert {r["geography"] for r in rows} == {
        "Australia", "New Zealand", "Other overseas"
    }


def test_crb_f_row_count() -> None:
    """Per industry: 4 NPE + 4 provisions + 1 write-off = 9 rows.
    × 15 industries = 135 rows."""
    rows = parse_crb_f_text(_CRB_F_MINI, source_publication="x", source_page=38)
    assert len(rows) == len(WBC_INDUSTRIES) * 9


# ---------------------------------------------------------------------------
# 3. Guardrail 1 — provenance on every row
# ---------------------------------------------------------------------------

def test_provenance_on_every_row() -> None:
    rows = parse_crb_e_text(
        _CRB_E_MINI,
        source_publication="WBC Pillar 3 — September 2025",
        source_page=37,
    ) + parse_crb_f_text(
        _CRB_F_MINI,
        source_publication="WBC Pillar 3 — September 2025",
        source_page=38,
    )
    for r in rows:
        assert r["source_publication"] == "WBC Pillar 3 — September 2025"
        assert r["source_table_ref"] in {"CRB(e)", "CRB(f)"}
        assert r["source_page"] in (37, 38)
        assert r["bank_code"] == "wbc"
        assert r["data_source"] == "pillar3_wbc"
        assert r["aggregation_level"] == "single_bank"


# ---------------------------------------------------------------------------
# 4. Guardrail 2 — zero vs redacted
# ---------------------------------------------------------------------------

def test_government_dashes_emit_honest_zero() -> None:
    """Phase 3.B.3 §A.2 harmonisation: Government, admin & defence row
    in CRB(f) is all dashes; per recon §1.8 these are honest-zero (no
    exposure → no NPE / no provision / no write-off), not redacted.

    Replaces the prior 3.B.1 assertion that this row produced
    redacted-with-reason rows. Preservation-rule sign-off for the
    change recorded in :doc:`docs/governance_log.md` Phase 3.B addenda.
    """
    rows = parse_crb_f_text(_CRB_F_MINI, source_publication="x", source_page=38)
    govt = [
        r for r in rows
        if r["industry_published"] == "Government, administration and defence"
    ]
    assert len(govt) == 9
    for r in govt:
        assert r["value_aud_m"] == 0.0
        assert r["redaction_reason"] is None


def test_published_zero_distinct_from_dash() -> None:
    """Modify the synthetic snippet to inject a real 0 vs a dash and
    verify they are distinguished. Construction Australia 0 vs dash."""
    snippet = _CRB_F_MINI.replace(
        "Construction 296 6 1 303 (67) (1) - (68) 24",
        "Construction 0 6 1 7 (67) (1) - (68) 24",
    )
    rows = parse_crb_f_text(snippet, source_publication="x", source_page=38)
    cons = [
        r for r in rows
        if r["industry_published"] == "Construction"
        and r["geography"] == "Australia"
        and r["metric"] == METRIC_NPE
    ]
    assert len(cons) == 1
    assert cons[0]["value_aud_m"] == 0.0
    assert cons[0]["redaction_reason"] is None


# ---------------------------------------------------------------------------
# 5. Guardrail 3 — no synthetic alignment
# ---------------------------------------------------------------------------

def test_as_of_date_taken_from_table_marker() -> None:
    rows = parse_crb_e_text(_CRB_E_MINI, source_publication="x", source_page=37)
    dates = {r["as_of_date"] for r in rows}
    assert dates == {date(2025, 9, 30)}


def test_missing_as_at_raises() -> None:
    bad = _CRB_E_MINI.replace("As at 30 September 2025", "")
    with pytest.raises(ValueError, match="As at"):
        parse_crb_e_text(bad, source_publication="x", source_page=37)


# ---------------------------------------------------------------------------
# 6. Guardrail 4 — no contributing_banks
# ---------------------------------------------------------------------------

def test_no_contributing_banks_column() -> None:
    rows = parse_crb_e_text(_CRB_E_MINI, source_publication="x", source_page=37)
    assert all("contributing_banks" not in r for r in rows)


# ---------------------------------------------------------------------------
# 7. Guardrail 5 — period_length_months metadata
# ---------------------------------------------------------------------------

def test_stocks_have_no_period_length() -> None:
    rows = parse_crb_e_text(_CRB_E_MINI, source_publication="x", source_page=37)
    assert all(r["period_length_months"] is None for r in rows)


def test_write_offs_have_period_length_12() -> None:
    rows = parse_crb_f_text(_CRB_F_MINI, source_publication="x", source_page=38)
    write_offs = [r for r in rows if r["metric"] == METRIC_WRITE_OFFS]
    assert len(write_offs) == len(WBC_INDUSTRIES)
    assert all(r["period_length_months"] == 12 for r in write_offs)
    # Per-bank guardrail: write-offs are Total-only (not per-geog) for WBC.
    assert {r["geography"] for r in write_offs} == {"Total"}


# ---------------------------------------------------------------------------
# 8. Industry mapping is strict — unknown labels raise (no silent drop)
# ---------------------------------------------------------------------------

def test_unknown_industry_label_raises() -> None:
    bad = _CRB_E_MINI.replace(
        "Construction 1,617 7,428 1,698 1,023 11,766",
        "Quantum Computing 1,617 7,428 1,698 1,023 11,766",
    )
    with pytest.raises(ValueError, match="unmatched industry"):
        parse_crb_e_text(bad, source_publication="x", source_page=37)


# ---------------------------------------------------------------------------
# 9. Schema invariant — every emitted row passes assert_row_well_formed
# ---------------------------------------------------------------------------

def test_every_row_passes_well_formed_invariant() -> None:
    rows = parse_crb_e_text(_CRB_E_MINI, source_publication="x", source_page=37)
    rows += parse_crb_f_text(_CRB_F_MINI, source_publication="x", source_page=38)
    for r in rows:
        assert_row_well_formed(r)


# ---------------------------------------------------------------------------
# 10. Real-PDF smoke + golden-row pins
# ---------------------------------------------------------------------------

REAL_PDF = (
    Path(__file__).resolve().parents[3]
    / "data" / "raw" / "pillar3" / "WBC_FY2025_Pillar3_Annual.pdf"
)


@pytest.mark.skipif(not REAL_PDF.exists(),
                    reason=f"WBC FY2025 PDF not present at {REAL_PDF}")
def test_real_pdf_round_trip_and_golden_values() -> None:
    df = extract_wbc_industry_rows(REAL_PDF)
    assert len(df) == 180  # 45 + 60 + 60 + 15

    def get(geo: str, ind: str, met: str) -> float | None:
        s = df[
            (df.geography == geo)
            & (df.industry_published == ind)
            & (df.metric == met)
        ].value_aud_m
        return None if s.empty else s.iloc[0]

    # Five golden values pinned to the FY2025 published table.
    assert get("Australia", "Construction", "exposure_aud_m") == 11_766.0
    assert get("Australia", "Construction", "npe_aud_m") == 296.0
    assert get(
        "Australia", "Construction",
        "individually_assessed_provision_aud_m",
    ) == 67.0
    assert get("Total", "Construction", "write_offs_aud_m") == 24.0
    assert get("New Zealand", "Trade", "npe_aud_m") == 26.0


@pytest.mark.skipif(not REAL_PDF.exists(),
                    reason=f"WBC FY2025 PDF not present at {REAL_PDF}")
def test_delegation_calls_extractor() -> None:
    """The in-place ``extract_industry_rows`` method on
    :class:`WbcPillar3PdfAdapter` must produce the same DataFrame the
    free function produces. Confirms the in-place wiring is live."""
    direct = extract_wbc_industry_rows(REAL_PDF)
    via_adapter = WbcPillar3PdfAdapter().extract_industry_rows(REAL_PDF)
    assert direct.equals(via_adapter)


# ---------------------------------------------------------------------------
# 11. Preservation rule sanity — existing WbcPillar3PdfAdapter API intact
# ---------------------------------------------------------------------------

def test_existing_adapter_api_preserved() -> None:
    a = WbcPillar3PdfAdapter()
    # Methods/attributes the existing adapter provided pre-Phase-3.B.
    assert hasattr(a, "normalise")
    assert hasattr(a, "source_name")
    assert hasattr(a, "canonical_columns")
    assert a.source_name == "wbc_pillar3_annual"
    assert a.FISCAL_YEAR_END_MONTH == 9
