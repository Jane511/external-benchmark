"""Cross-bank integration test (Phase 3.B.2 §6) — parser-vs-map drift.

Runs the WBC, NAB, and ANZ industry extractors against their FY2025
PDFs and asserts every per-bank label resolves to a canonical
harmonisation-map bucket without raising. Also asserts that consumer
rows do not silently route to ``business_lending_anzsic_*`` segments.

This is NOT the aggregation layer — it's a parser-vs-map drift
detector. Adding a new label to a bank's published table without
updating ``config/anzsic_harmonisation_map.yaml`` should make this
test fail loudly.

CBA is intentionally absent — the CBA sibling module is the next
sub-phase (3.B.3).
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from ingestion.adapters.anz_pillar3_industry import extract_anz_industry_rows
from ingestion.adapters.anzsic_harmonisation import (
    is_business_lending,
    resolve,
)
from ingestion.adapters.cba_pillar3_industry import extract_cba_industry_rows
from ingestion.adapters.nab_pillar3_industry import extract_nab_industry_rows
from ingestion.adapters.wbc_pillar3_industry import extract_wbc_industry_rows


_PDF_DIR = Path(__file__).resolve().parents[3] / "data" / "raw" / "pillar3"

_BANKS = (
    ("wbc", _PDF_DIR / "WBC_FY2025_Pillar3_Annual.pdf",
     extract_wbc_industry_rows, 180),
    ("nab", _PDF_DIR / "NAB_FY2025_Pillar3_Annual.pdf",
     extract_nab_industry_rows, 42),
    ("anz", _PDF_DIR / "ANZ_FY2025_Pillar3_Annual.pdf",
     extract_anz_industry_rows, 75),
    ("cba", _PDF_DIR / "CBA_FY2025_Pillar3_Annual.pdf",
     extract_cba_industry_rows, 390),
)


@pytest.mark.parametrize("bank_code,pdf,extractor,expected_rows", _BANKS)
def test_every_published_label_resolves(
    bank_code: str, pdf: Path, extractor, expected_rows: int,
) -> None:
    if not pdf.exists():
        pytest.skip(f"{bank_code.upper()} FY2025 PDF not present at {pdf}")
    df = extractor(pdf)
    assert len(df) == expected_rows, (
        f"{bank_code}: expected {expected_rows} rows, got {len(df)}"
    )
    labels = sorted(df["industry_published"].unique())
    for label in labels:
        # Raises UnknownIndustryLabelError on any drift between parser
        # and harmonisation map.
        canon = resolve(bank_code, label)
        assert canon, f"{bank_code}: label {label!r} resolved to empty bucket"


@pytest.mark.parametrize("bank_code,pdf,extractor,_expected", _BANKS)
def test_no_row_routes_to_unknown_segment(
    bank_code: str, pdf: Path, extractor, _expected: int,
) -> None:
    if not pdf.exists():
        pytest.skip(f"{bank_code.upper()} FY2025 PDF not present at {pdf}")
    df = extractor(pdf)
    for label in df["industry_published"].unique():
        canon = resolve(bank_code, label)
        # is_business_lending raises if canonical key is unknown.
        is_business_lending(canon)


def test_anz_consumer_rows_do_not_route_to_business_lending() -> None:
    pdf = _PDF_DIR / "ANZ_FY2025_Pillar3_Annual.pdf"
    if not pdf.exists():
        pytest.skip(f"ANZ FY2025 PDF not present at {pdf}")
    df = extract_anz_industry_rows(pdf)
    consumer_labels = {"Personal Lending", "Residential Mortgage"}
    consumer_rows = df[df["industry_published"].isin(consumer_labels)]
    assert len(consumer_rows) > 0
    for label in consumer_rows["industry_published"].unique():
        canon = resolve("anz", label)
        assert not canon.startswith("business_lending_"), (
            f"ANZ {label!r} routed to {canon!r} — must be a consumer bucket"
        )
        assert is_business_lending(canon) is False


@pytest.mark.parametrize("bank_code,pdf,extractor,_expected", _BANKS)
def test_government_row_emits_honest_zero_across_banks(
    bank_code: str, pdf: Path, extractor, _expected: int,
) -> None:
    """Phase 3.B.3 §A.2 harmonisation invariant: every bank's Government
    & Official Institutions row, where the published metric cell is a
    dash, emits ``value_aud_m=0.0`` with no ``redaction_reason``.

    Fails loudly if a future bank parser breaks the harmonisation.
    """
    if not pdf.exists():
        pytest.skip(f"{bank_code.upper()} FY2025 PDF not present at {pdf}")
    from ingestion.adapters.pillar3_industry_schema import (
        _GOVERNMENT_INDUSTRY_PER_BANK,
    )
    govt_label = _GOVERNMENT_INDUSTRY_PER_BANK[bank_code]
    df = extractor(pdf)
    govt_rows = df[df["industry_published"] == govt_label]
    if govt_rows.empty:
        pytest.skip(f"{bank_code}: Government row not present in this extract")
    # For every Government metric row that the bank published as a
    # dash (i.e. not a real number), the parser must emit honest-zero.
    # We can detect "would have been a dash" by checking that the
    # value_aud_m is exactly 0.0 — any positive number means the bank
    # published a real figure, which the helper must not override.
    metric_rows = govt_rows[govt_rows["metric"] != "exposure_aud_m"]
    if metric_rows.empty:
        pytest.skip(f"{bank_code}: Government row has no NPE/provision/write-off rows")
    for _, r in metric_rows.iterrows():
        # Every cell is either a published number (positive) or honest
        # zero (0.0 with no redaction_reason). Never null + redacted.
        if r["value_aud_m"] == 0.0:
            assert pd.isna(r["redaction_reason"]), (
                f"{bank_code} Government row metric={r['metric']!r}: "
                f"honest-zero must have redaction_reason=None, got "
                f"{r['redaction_reason']!r}"
            )
        else:
            # If non-zero, it's a real published number; that's fine.
            assert pd.notna(r["value_aud_m"]) and r["value_aud_m"] > 0


def test_nab_consumer_rows_do_not_route_to_business_lending() -> None:
    pdf = _PDF_DIR / "NAB_FY2025_Pillar3_Annual.pdf"
    if not pdf.exists():
        pytest.skip(f"NAB FY2025 PDF not present at {pdf}")
    df = extract_nab_industry_rows(pdf)
    for label in ("Personal", "Residential mortgages"):
        if label in df["industry_published"].values:
            canon = resolve("nab", label)
            assert not canon.startswith("business_lending_")
            assert is_business_lending(canon) is False
