"""ANZ annual Pillar 3 PDF adapter.

Subclass of ``CbaPillar3PdfAdapter``. ANZ's CR6 uses the same column
layout but prefixes every data row with a 1-or-2 digit row index
(e.g. ``"19 0.00 to <0.15 ..."``). The ``ROW_INDEX_PREFIX_RE`` hook on
the base class strips that prefix before portfolio matching. ANZ's
fiscal year ends 30 September.

Phase 3.B.2 addition (2026-05-04): the ``extract_industry_rows()``
method delegates to
:func:`ingestion.adapters.anz_pillar3_industry.extract_anz_industry_rows`
to emit per-bank ANZSIC industry rows (gross carrying split into
loans / off-balance-sheet / other; total NPE; AASB 9 individually-
assessed provision). Pre-existing CR6/CR10/``normalise`` behaviour
unchanged.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from ingestion.adapters.anz_pillar3_industry import (
    extract_anz_industry_rows,
)
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

    # Phase 3.B.2 addition. Independent of the CR6/CR10 ``normalise`` path.
    def extract_industry_rows(self, file_path: Path) -> pd.DataFrame:
        """Per-bank Pillar 3 industry-table extraction (ANZ industry table)."""
        return extract_anz_industry_rows(Path(file_path))
