"""Tests for the Judo Pillar 3 PDF adapter.

Live-PDF dependency (pdfplumber) is heavy for unit tests, so these
tests drive the adapter's text-driven extractor with synthesised page
text mirroring Judo's APS 330 layout. Same pattern as the CBA Pillar 3
PDF adapter tests.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from ingestion.adapters.judo_disclosure_adapter import JudoDisclosureAdapter
from ingestion.adapters.non_bank_base import CANONICAL_OBSERVATION_COLUMNS
from src.models import DataDefinitionClass, RawObservation, SourceType


# Synthetic CR6 page text — mimics Judo's quarterly Pillar 3 layout
# (the columns are: gross EAD, off-balance, CCF%, EAD post-CRM, PD%,
# borrowers, LGD%, RWA, density, EL).
_CR6_PAGE = (
    "Pillar 3 disclosure (APS 330)\n"
    "For the quarter ended 31 March 2026\n"
    "CR6: IRB - credit risk exposures by portfolio and PD range\n"
    "Portfolio PD Range $M $M % $M % # % $M % $M\n"
    "SME lending 0.00 to <0.15 1,210 250 50 1,335 0.10 1,200 32 800 60 1\n"
    "0.15 to <0.50 2,540 380 50 2,730 0.32 3,400 35 1,920 70 9\n"
    "0.50 to <2.50 1,810 220 50 1,920 1.20 2,700 36 1,580 82 23\n"
    "100.00 (Default) 60 5 50 62 100.00 80 40 90 145 25\n"
    "Commercial real estate 0.00 to <0.15 480 60 50 510 0.12 220 30 280 55 1\n"
    "0.50 to <2.50 980 110 50 1,035 1.45 320 32 760 73 15\n"
    "100.00 (Default) 25 4 50 27 100.00 28 35 30 110 9\n"
    "Construction 0.50 to <2.50 220 30 50 235 1.80 90 40 195 83 6\n"
    "100.00 (Default) 12 2 50 13 100.00 15 45 16 123 5\n"
)


def test_judo_extracts_pd_per_band_and_segment() -> None:
    adapter = JudoDisclosureAdapter()
    rows = adapter._extract_cr6_text(
        _CR6_PAGE, as_of=date(2026, 3, 31), page_num=2,
    )
    assert rows, "Judo CR6 extractor produced zero rows"

    segments = {r["segment"] for r in rows}
    assert "sme_corporate" in segments
    assert "commercial_property" in segments
    assert "development" in segments

    # Every row should be parameter='pd' with BASEL_PD_ONE_YEAR
    for r in rows:
        assert r["parameter"] == "pd"
        assert r["data_definition_class"] == DataDefinitionClass.BASEL_PD_ONE_YEAR
        assert 0.0 <= r["value"] <= 1.0
        assert r["source_id"] == "judo"
        assert r["source_type"] == SourceType.NON_BANK_LISTED
        assert r["as_of_date"] == date(2026, 3, 31)


def test_judo_first_band_pd_is_decimal_not_percent() -> None:
    """The PD column in CR6 is a percent — adapter must divide by 100."""
    adapter = JudoDisclosureAdapter()
    rows = adapter._extract_cr6_text(
        _CR6_PAGE, as_of=date(2026, 3, 31),
    )
    sme_first_band = next(
        r for r in rows
        if r["segment"] == "sme_corporate" and "0.00 to <0.15" in r["page_or_table_ref"]
    )
    # Source says "0.10" % — adapter should report 0.001
    assert abs(sme_first_band["value"] - 0.001) < 1e-9


def test_judo_default_band_is_capped_at_one() -> None:
    """100.00% PD must be reported as 1.0 (Pydantic [0, 1] bound)."""
    adapter = JudoDisclosureAdapter()
    rows = adapter._extract_cr6_text(_CR6_PAGE, as_of=date(2026, 3, 31))
    default_rows = [r for r in rows if "Default" in r["page_or_table_ref"]]
    assert default_rows, "Default band rows missing"
    for r in default_rows:
        assert r["value"] == 1.0


def test_judo_rows_validate_against_raw_observation() -> None:
    """Every emitted row must round-trip through RawObservation Pydantic."""
    adapter = JudoDisclosureAdapter()
    rows = adapter._extract_cr6_text(_CR6_PAGE, as_of=date(2026, 3, 31))
    for r in rows:
        RawObservation(**{k: v for k, v in r.items() if k != "_skip"})


def test_judo_normalise_missing_file_returns_empty(tmp_path: Path) -> None:
    adapter = JudoDisclosureAdapter()
    df = adapter.normalise(tmp_path / "does_not_exist.pdf")
    assert isinstance(df, pd.DataFrame)
    assert df.empty
    assert set(df.columns) == set(CANONICAL_OBSERVATION_COLUMNS)
