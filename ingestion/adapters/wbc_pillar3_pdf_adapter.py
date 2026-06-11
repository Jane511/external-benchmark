"""Westpac annual Pillar 3 PDF adapter.

Subclass of ``CbaPillar3PdfAdapter``. WBC's CR6 uses the same column
layout as CBA but with a handful of label variants, notably hyphenated
``"RBNZ Regulated Entities - Retail"`` and ``"RBNZ Regulated Entities
- Non-retail"`` (CBA uses an unhyphenated form). WBC's fiscal year
ends 30 September.

Phase 3.B addition (2026-05-04): the ``extract_industry_rows()``
method below is a sibling-module delegation to
:func:`ingestion.adapters.wbc_pillar3_industry.extract_wbc_industry_rows`.
It emits per-bank industry rows (CRB(e) exposures + CRB(f) NPE,
provisions, write-offs) under the canonical schema in
:mod:`pillar3_industry_schema`. Pre-existing CR6 / CR10 / ``normalise``
behaviour is unchanged.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from ingestion.adapters.cba_pillar3_pdf_adapter import (
    CbaPillar3PdfAdapter,
)
from ingestion.adapters.wbc_pillar3_industry import (
    extract_wbc_industry_rows,
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

    # Phase 3.B addition. Independent of the CR6/CR10 ``normalise`` path.
    def extract_industry_rows(self, file_path: Path) -> pd.DataFrame:
        """Per-bank Pillar 3 industry-table extraction (CRB(e) + CRB(f))."""
        return extract_wbc_industry_rows(Path(file_path))
