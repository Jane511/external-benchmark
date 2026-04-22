"""ANZ annual Pillar 3 PDF adapter.

Subclass of ``CbaPillar3PdfAdapter``. ANZ's CR6 uses the same column
layout but prefixes every data row with a 1-or-2 digit row index
(e.g. ``"19 0.00 to <0.15 ..."``). The ``ROW_INDEX_PREFIX_RE`` hook on
the base class strips that prefix before portfolio matching. ANZ's
fiscal year ends 30 September.
"""

from __future__ import annotations

import re

from ingestion.adapters.cba_pillar3_pdf_adapter import (
    CbaPillar3PdfAdapter,
)


class AnzPillar3PdfAdapter(CbaPillar3PdfAdapter):
    """Extract CR6 PD/LGD and CR10 risk weights from ANZ's FY Pillar 3 PDF."""

    _SOURCE_NAME = "anz_pillar3_annual"

    FISCAL_YEAR_END_MONTH = 9

    # Strip leading 1- or 2-digit row index + whitespace from each line's
    # pre-PD-range prefix. ANZ prints e.g. "19 0.00 to <0.15 …" where "19"
    # is an ordinal table row number.
    ROW_INDEX_PREFIX_RE = re.compile(r"^\d{1,2}\s+")

    PORTFOLIO_PATTERNS = (
        # ANZ uses "(QRR)" / "(SME)" suffixes and sometimes different
        # ordering. Most-specific first.
        ("qualifying revolving retail (qrr)", "retail_qrr"),
        ("retail sme",                        "retail_sme"),
        ("large corporate",                   "corporate_general"),
    ) + CbaPillar3PdfAdapter.PORTFOLIO_PATTERNS
