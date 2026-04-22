# Pillar 3 PDF Scrapers Specification (NAB / WBC / ANZ)

## Goal
Extract IRB credit risk tables (PD, LGD by asset class) from NAB/WBC/ANZ Pillar 3 
disclosure PDFs. Reuse FileDownloader from caching layer. Match CBA's output schema.

## Approach: pdfplumber table extraction

Each bank publishes a Pillar 3 PDF following APS 330 format. Tables are standardised 
but layouts differ slightly (column widths, page breaks, headers). Use pdfplumber's 
table extraction with bank-specific locators.

## Shared logic (extend `ingestion/pillar3/base.py`)

Add helpers to Pillar3BaseScraper:
- `_find_table_by_header_pattern(pdf, pattern: str) -> tuple[int, Table]` — locate a table 
  by searching for header text on any page. Returns (page_num, table).
- `_extract_pd_lgd_row(row: list, pd_col: int, lgd_col: int, exposure_col: int) -> dict` 
  — parse a single row; coerce "0.72%" → 0.0072, handle "—" / "n/a" → None.
- `_normalise_asset_class_label(raw_label: str) -> str` — e.g., "Residential mortgage" / 
  "Housing" / "Retail residential mortgage" → "residential_mortgage".

## Per-bank scrapers

Each of nab.py / wbc.py / anz.py follows this pattern:

```python
class Pillar3NabScraper(Pillar3BaseScraper):
    source_name = "nab_pillar3"
    
    def __init__(self, source_path=None, config=None, reporting_date=None, 
                 force_refresh=False, cache_base="data/raw", **extras):
        # same pattern as Pillar3CbaScraper
        ...
    
    def _resolve_source_path(self) -> Path:
        # Identical to CBA: H1/H2 derivation + FileDownloader
        ...
    
    def fetch(self) -> list[ScrapedDataPoint]:
        pdf_path = self._resolve_source_path()
        with pdfplumber.open(pdf_path) as pdf:
            points = []
            points.extend(self._extract_irb_credit_risk_table(pdf))
            points.extend(self._extract_specialised_lending_table(pdf))
        return self.validate(points)
    
    def _extract_irb_credit_risk_table(self, pdf) -> list[ScrapedDataPoint]:
        # Bank-specific: locate CR6 / IRB credit risk table, extract PD/LGD per asset class
        ...
```

## Asset class coverage (per bank)

Each bank should produce ScrapedDataPoints for (minimum):
- residential_mortgage: PD + LGD
- commercial_property_investment: PD + LGD
- corporate_sme: PD + LGD
- specialised_lending slotting grades: PD for Strong/Good/Satisfactory/Weak (LGD via slotting is regulatory, skip)

Expected entries per bank per half-year: 6 base + 4 slotting = 10, matching CBA.

## Bank-specific table location hints

NAB: Search for header "CR6" or "IRB — Credit risk exposures by portfolio and PD range"
WBC: Search for header "Credit Risk — Exposures by portfolio type and PD band"  
ANZ: Search for header "IRB Approach — Credit Risk Exposures"

Note: These are starting points. Claude Code may need to iterate with real PDFs. 
Build the module with a clear extension point for table location overrides per bank.

## Validation (inherit from Pillar3BaseScraper)

Existing ranges apply:
- Residential mortgage PD ∈ [0.003, 0.025]
- CRE PD ∈ [0.01, 0.08]
- Corporate SME PD ∈ [0.005, 0.08]
- All LGD ∈ [0, 1.0]
- Slotting grades: Strong < Good < Satisfactory < Weak ordering preserved

Peer comparison (already in base): flag if any bank's value >3x from 4-bank median.

## Source ID format

Match CBA pattern: `{BANK}_{ASSET_CLASS}_{PD|LGD}_{FY{YEAR}}`
Examples: `NAB_RESIDENTIAL_MORTGAGE_PD_FY2025`, `WBC_DEVELOPMENT_GOOD_PD_FY2025`

## Fixtures

Since PDF fixtures are heavy and bank-specific, use JSON-extracted tables as primary 
fixtures + one real PDF sample per bank for integration tests (gitignored if large).

- tests/fixtures/nab_pillar3_tables.json — hand-extracted PD/LGD table as list of dicts
- tests/fixtures/wbc_pillar3_tables.json — same
- tests/fixtures/anz_pillar3_tables.json — same

Integration tests use a `_parse_extracted_tables()` path that bypasses pdfplumber 
and feeds the JSON directly. This lets you test the transform logic without real PDFs.

## CLI additions

- `benchmark ingest pillar3 nab [--source-path X] [--reporting-date Y] [--force-refresh]`
- `benchmark ingest pillar3 wbc ...`
- `benchmark ingest pillar3 anz ...`
- `benchmark ingest pillar3` (runs all 4 banks, reports per-bank success/failure)
- Remove NotImplementedError from stubs once real implementations land.

## Dependencies

Add to pyproject.toml [project.optional-dependencies.ingestion]: