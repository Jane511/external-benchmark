"""NAB annual Pillar 3 PDF adapter.

Subclass of ``CbaPillar3PdfAdapter``. NAB's CR6 uses the same column
layout (PD at token-4, LGD at token-6 after the PD range) but with
different portfolio labels — e.g. ``"Retail SME"`` (word order reversed
vs CBA's ``"SME retail"``) and ``"Bank"`` in place of ``"Financial
institution"``. NAB's fiscal year ends 30 September rather than CBA's
30 June.
"""

from __future__ import annotations

from ingestion.adapters.cba_pillar3_pdf_adapter import (
    CbaPillar3PdfAdapter,
)


class NabPillar3PdfAdapter(CbaPillar3PdfAdapter):
    """Extract CR6 PD/LGD and CR10 risk weights from NAB's FY Pillar 3 PDF."""

    _SOURCE_NAME = "nab_pillar3_annual"

    # NAB fiscal year ends 30 September.
    FISCAL_YEAR_END_MONTH = 9

    # Patterns prepended to the CBA base set. Longest-first so the NAB-
    # specific labels win before the CBA catch-all ("corporate", "bank").
    PORTFOLIO_PATTERNS = (
        # NAB-specific ordering / labelling first — more specific wins.
        ("retail sme",                       "retail_sme"),
        ("large corporate",                  "corporate_general"),
        ("specialised lending income-producing real estate",
                                             "commercial_property_investment"),
        ("sovereign and central bank",       "sovereign"),
        ("bank",                             "financial_institution"),
    ) + CbaPillar3PdfAdapter.PORTFOLIO_PATTERNS
