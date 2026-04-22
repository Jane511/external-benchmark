"""Regression tests for the CR6/CR9 boundary fix.

Background: the base CBA Pillar 3 adapter previously matched CR6 by
substring, which caused CR9 pages (whose explanatory text cross-references
CR6) to be parsed as if they were CR6 — producing bogus sub-5% LGD values
across all four Big 4 bank adapters.

These tests lock in the fix so it cannot silently regress.
"""

from ingestion.adapters.cba_pillar3_pdf_adapter import CbaPillar3PdfAdapter


def test_cr6_header_in_prose_does_not_trigger_extraction():
    """Page containing 'CR6' only as a cross-reference in prose must be skipped."""
    # Synthetic page text mimicking NAB's CR9 explanatory paragraph.
    cr9_text = (
        "CR9: IRB - backtesting of probability of default per portfolio\n"
        "The approach to measure the number of borrowers is outlined in "
        "CR6: IRB - credit risk exposures by portfolio and PD range.\n"
        "0.00 to <0.15  0.0020  0.1234  extra tokens here\n"
    )
    assert CbaPillar3PdfAdapter.CR6_HEADER_RE.search(cr9_text) is None, (
        "CR9 cross-reference to CR6 must not trigger CR6 extraction"
    )


def test_cr6_genuine_header_triggers_extraction():
    """Page whose first non-whitespace content is a CR6 header must be accepted."""
    cr6_text = (
        "CR6: IRB - credit risk exposures by portfolio "
        "and probability of default range\n"
        "Credit risk exposures subject to the A-IRB approach by asset class and PD band\n"
    )
    assert CbaPillar3PdfAdapter.CR6_HEADER_RE.search(cr6_text) is not None


def test_cr6_boundary_truncates_at_next_cr_table():
    """CR6 rows before CR7/CR8/CR9/CR10 header kept; after are dropped."""
    mixed_text = (
        "CR6: IRB - credit risk exposures by portfolio\n"
        "Residential mortgage 0.00 to <0.15  0.5000  0.2300  ...\n"
        "CR7: IRB - effect of credit derivatives used as CRM technique\n"
        "some other row 0.99 0.99\n"
    )
    match = CbaPillar3PdfAdapter.CR6_BOUNDARY_RE.search(mixed_text)
    assert match is not None, "CR7 header must be detected as a CR6 boundary"
    truncated = mixed_text[: match.start()]
    assert "CR7" not in truncated
    assert "0.00 to <0.15" in truncated, "CR6 rows before boundary must survive"
    assert "0.99 0.99" not in truncated, "Rows after boundary must be dropped"


def test_cr6_boundary_detects_cr9():
    """CR9 header on the same page as CR6 (rare but possible) must truncate."""
    text = "CR6: IRB\nrow1\nCR9: backtesting\nrow2"
    m = CbaPillar3PdfAdapter.CR6_BOUNDARY_RE.search(text)
    assert m is not None
    assert "row1" in text[: m.start()]
    assert "row2" not in text[: m.start()]
