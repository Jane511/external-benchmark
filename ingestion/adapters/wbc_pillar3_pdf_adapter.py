"""Westpac annual Pillar 3 PDF adapter.

Subclass of ``CbaPillar3PdfAdapter``. WBC's CR6 uses the same column
layout as CBA but with a handful of label variants, notably hyphenated
``"RBNZ Regulated Entities - Retail"`` and ``"RBNZ Regulated Entities
- Non-retail"`` (CBA uses an unhyphenated form). WBC's fiscal year
ends 30 September.
"""

from __future__ import annotations

from ingestion.adapters.cba_pillar3_pdf_adapter import (
    CbaPillar3PdfAdapter,
)


class WbcPillar3PdfAdapter(CbaPillar3PdfAdapter):
    """Extract CR6 PD/LGD and CR10 risk weights from Westpac's FY Pillar 3 PDF."""

    _SOURCE_NAME = "wbc_pillar3_annual"

    FISCAL_YEAR_END_MONTH = 9

    PORTFOLIO_PATTERNS = (
        # Hyphenated RBNZ variants — must match before the CBA
        # unhyphenated fallbacks so Pattern-A combine resolves correctly.
        ("rbnz regulated entities - non-retail", "rbnz_non_retail"),
        ("rbnz regulated entities - retail",     "rbnz_retail"),
        ("retail sme",                           "retail_sme"),
        ("large corporate",                      "corporate_general"),
    ) + CbaPillar3PdfAdapter.PORTFOLIO_PATTERNS
