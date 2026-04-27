# Adapter Pattern for Live Publisher Files

> **Brief 1 update (2026-04-27):** the engine now publishes raw,
> source-attributable `RawObservation` rows (see [`src/models.py`](../src/models.py))
> and consumers read them through `PeerObservations.for_segment()`. The
> canonical DataFrame shape for new adapters is the
> `CANONICAL_OBSERVATION_COLUMNS` list defined in
> [`ingestion/adapters/non_bank_base.py`](../ingestion/adapters/non_bank_base.py):
>
>     ["source_id", "source_type", "segment", "product",
>      "parameter", "value", "as_of_date", "reporting_basis",
>      "methodology_note", "sample_size_n", "period_start",
>      "period_end", "source_url", "page_or_table_ref"]
>
> NEW non-bank adapters should subclass `NonBankDisclosureAdapter` and
> map their published segment labels via
> [`ingestion/segment_mapping.yaml`](../ingestion/segment_mapping.yaml).
> The legacy Big-4 Pillar 3 adapters keep their existing
> `["asset_class", "metric_name", ...]` shape for the transitional
> period — they continue to feed the legacy `BenchmarkEntry` table and
> are migrated to `RawObservation` via
> [`scripts/migrate_to_raw_observations.py`](../scripts/migrate_to_raw_observations.py).

## Why

Every scraper in `ingestion/` was originally written against a **canonical
fixture shape** — a simple tabular layout (long format, known column names,
known sheet name) that tests can construct programmatically. The live
publisher files (APRA XLSX, bank Pillar 3 documents) use completely
different layouts: wide formats, multi-sheet workbooks, metric labels in
row cells rather than column headers, merged banners, etc.

An adapter is a thin, single-responsibility module that **reads the real
file and returns a canonical-shaped DataFrame**. The scraper then iterates
that DataFrame into `ScrapedDataPoint` objects exactly as it would for a
fixture. Transform/validation/registry code downstream is untouched.

```
Live file  →  Adapter  →  Canonical DataFrame  →  Existing transform
Fixture    →  (direct read)  →  Canonical DataFrame  →  Existing transform
```

Both paths converge on the same DataFrame shape, so the `scraped_to_entry`
pipeline and every fixture-based test stay on their original code path.

## When to use an adapter

Use an adapter when **all** of these hold:

1. The real publisher file has a structurally different shape from the
   fixture (wide vs long, multi-sheet vs single-sheet, merged headers,
   metric-as-row-label, etc.).
2. `sources.yaml` cannot fix it with a simple sheet/column name change —
   i.e. the canonical shape is not just renamed but genuinely absent in
   the live file.
3. The fixture is still useful for testing (you want to keep it).

Don't use an adapter when:

- The live file is just a rename of the fixture shape — update
  `sources.yaml` instead.
- The adapter would need to fabricate data (e.g. synthesise a dimension
  that doesn't exist in the real file). Fabrication should be called out
  and debated, not buried in an adapter.

## How detection works

Each scraper that supports live files picks a **detection sentinel** from
`sources.yaml` — typically the canonical sheet name. If that sheet exists
in the opened workbook, the scraper reads it directly (fixture path).
Otherwise the scraper instantiates the adapter and delegates.

Concrete example — `ApraAdiScraper.scrape`:

```python
canonical_sheet = self._config.get("sheet")           # e.g. "Asset Quality"
if self._has_canonical_sheet(path, canonical_sheet):
    return self._scrape_canonical(path)
return self._scrape_via_adapter(path)
```

`_has_canonical_sheet` is a cheap `openpyxl.load_workbook` + `sheetnames`
check. The fixture (which always has `"Asset Quality"`) never triggers the
adapter; the live APRA workbook (which doesn't) always does.

## How to add an adapter for a new source

1. **Inspect the live file first.** Write a small script that opens the
   workbook / PDF and dumps sheet names, row labels, first-row samples.
   Save the dump to `outputs/<source>_structure.md`. That document is
   the spec the adapter targets. Do not guess.
2. **Subclass `AbstractAdapter`** in
   `ingestion/adapters/<source>_adapter.py`:
   - `source_name` — the `sources.yaml` key.
   - `canonical_columns` — the column contract.
   - `normalise(file_path) → DataFrame` — read real file, reshape, return.
   - Extension tunables as class attributes (sheet candidates, row maps,
     column rename maps, plausibility ranges) so future layout tweaks are
     one-line edits.
3. **Wire the adapter into the scraper's `scrape()`** behind the
   detection sentinel. Do not remove the canonical path — fixtures still
   need it.
4. **Return an empty (but shape-valid) DataFrame** if the adapter finds
   no usable data. Downstream code shouldn't have to special-case None.
5. **Log what was tried** when matching fails — sheet candidates,
   row indices, column patterns. Missing data is the #1 source of
   silent bugs; verbose logs make triage trivial.
6. **Write tests in `tests/test_ingestion/adapters/`** against a
   synthetic mimicry of the live structure (build it with `openpyxl` in
   a fixture, as `tests/test_ingestion/adapters/test_apra_performance_adapter.py`
   does). Do not commit real publisher files. Tests should cover:
   - Happy path: canonical columns present, sensible row count.
   - Missing sheets → warning + empty frame (not crash).
   - Implausible values → logged + filtered.
   - Edge cases unique to the source (pre-cutoff blanks, merged headers).
7. **Write an integration test** that feeds both a live-shaped workbook
   and the canonical fixture to the scraper, asserts the adapter was
   invoked in the first case and bypassed in the second, and confirms
   the produced `ScrapedDataPoint` list is non-empty in each.
8. **Dry-run against the real file**, verify the entry count is in the
   expected ballpark, commit, inspect `benchmarks.db`.

## Relationship to fixture tests

Fixture tests validate the **canonical contract**: given a canonical-shaped
input, the scraper + transform + registry pipeline produces correct
entries. That contract is what the adapter's `canonical_columns` / output
DataFrame obey. If adapters and fixtures drift apart you'll see:

- Fixture tests pass; live ingestion returns wrong values → adapter bug.
- Fixture tests fail → canonical contract itself changed; update both.

As long as adapter output ≡ fixture shape, both paths produce the same
`ScrapedDataPoint`s and neither needs to know about the other.

## Where adapter-specific provenance lives

Every `ScrapedDataPoint` the adapter produces MUST carry audit-trail
fields in `quality_indicators` so the eventual `BenchmarkEntry.notes`
string records which file, sheet, row the value came from. Example from
the APRA Performance adapter:

```python
quality_indicators={
    "coverage": "apra_sector_aggregate",
    "sector": "banks",
    "source_sheet": "Tab 2d",
    "aps220_row": 53,
    "adapter": "ApraPerformanceAdapter",
}
```

Produces a `notes` value like:

```
coverage=apra_sector_aggregate; sector=banks; source_sheet=Tab 2d;
aps220_row=53; adapter=ApraPerformanceAdapter
```

This is the MRC audit trail. Always include at minimum the adapter class
name, the source sheet, and any disambiguation (sector/bank/period) that
doesn't already appear in the `source_id`.

## Current adapter inventory

| Adapter | Status | Source | Produces |
| ------- | ------ | ------ | -------- |
| `ApraPerformanceAdapter` | live (Path A) | APRA Quarterly ADI Performance | `adi_sector_total` NPL / 90+DPD rows for All ADIs, Banks, Major banks (APS-220 era, Mar 2022+). 96 entries. |
| `ApraQpexAdapter`        | live (Path B) | APRA Property Exposures | `residential_mortgage` (2019+) and `commercial_property_investment` (APS-220 era 2022+) NPL ratios via row arithmetic. Numerator and denominator preserved on every row. **Label-based row lookup** — no hardcoded row indices, so APRA's 1-row offset between Tab 1a and Tab 2a/4a doesn't break extraction. 132 entries. |
| `CbaPillar3PdfAdapter`   | live (CBA Option B) | CBA half-year / full-year Pillar 3 PDF | CR6 PD + LGD per portfolio × PD band (IRB AIRB + FIRB), CR10 supervisory risk weights for specialised-lending slotting. **Text-line parsing with a PD-range regex** (pdfplumber's `extract_tables` drops the left-most category label). Multi-line portfolio labels resolved via one-line look-ahead (Pattern B: `"Corporate"` + `"(incl. SME corporate)"` → `corporate_sme`) and previous-line combine (Pattern A: `"RBNZ regulated entities"` + `"Non-retail …"`). Slotting emits **`risk_weight`** — APS 113-prescribed values — never fabricates PDs. ~127 entries. |
| `CbaPillar3QuarterlyAdapter` | live (CBA Option A) | CBA quarterly APS 330 XLSX | Per-portfolio `npl_ratio` computed as `CRB(f)(ii).non_performing / EAD_CRWA.ead`. Label-based row lookup on both sheets; first-match wins. Mirrors the APRA QPEX arithmetic pattern — numerator / denominator / formula preserved in `quality_indicators`. ~7 entries per quarter. |
| NAB / WBC / ANZ          | see regex-broadening note | Pillar 3 PDFs | Pillar 3 tables via `pdfplumber` — adapter pattern is overkill; broadening the table regex is simpler |

## When arithmetic is involved (lessons from Path B)

Some live files publish raw dollar figures rather than pre-computed
ratios, so the adapter has to divide numerator by denominator. Two
things the QPEX adapter got right that future arithmetic adapters should
copy:

1. **Preserve numerator and denominator as their own canonical columns**
   (`numerator_value`, `denominator_value`) so MRC can re-derive the
   ratio without opening the source file. These flow through into the
   `quality_indicators` on each `ScrapedDataPoint` and end up in the
   entry's `notes` string. Also include a human-readable
   `arithmetic` formula string.
2. **Use label-based row lookup, not row indices**, whenever a file's
   structure differs across sheets (QPEX has a 1-row offset between Tab
   1a and Tab 2a/4a because of an extra footnote row). A class-level
   `_RowMatcher(required=(...), forbidden=(...))` spec keeps "match
   the total, not the sub-breakdown" declarations visible in one place
   and independent of layout drift.

See `ingestion/adapters/apra_qpex_adapter.py` for the reference implementation.
