# Industry-Analysis Integration Spec

## Context

**Project:** `external_benchmark_engine`
**Goal:** Wire the `industry-analysis` project's parquet exports into the benchmark engine to enable Report 2 generation.

Report 2 (the "Environment and Industry Overlay Report") was deferred in the original benchmark engine spec pending the `industry-analysis` project reaching a stable state. Now that `industry-analysis` has been polished to a v1.0 release, we can complete the integration and generate Report 2.

## Inputs

The `industry-analysis` project produces these canonical exports:

- `industry-analysis/data/exports/industry_risk_scores.parquet`
- `industry-analysis/data/exports/property_market_overlays.parquet`
- `industry-analysis/data/exports/downturn_overlay_table.parquet`
- `industry-analysis/data/exports/macro_regime_flags.parquet`

Our job is to read these files and wire their data into the existing `external_benchmark_engine` reporting pipeline to produce a new Report 2 output.

## Integration Steps

### Step 1: Create `ingestion/industry_context.py`

In the `external_benchmark_engine` project:

1. Create a new file `ingestion/industry_context.py`
2. Write a function `load_industry_analysis_exports()`
3. The function should:
   - Take a `data_dir` parameter pointing to the `industry-analysis/data/exports/` directory
   - Load each of the four parquet files into a DataFrame
   - Return a dict of DataFrames keyed by file basename, e.g. `{"industry_risk_scores": df1, "property_market_overlays": df2, ...}`

Tips:
- Use `pandas.read_parquet()` to load the files
- Use `pathlib.Path(data_dir).glob("*.parquet")` to find all parquet files in the directory
- Use `parquet_file.stem` to get the filename without extension for the dict keys

### Step 2: Wire the data into `reports/environment_report.py`

1. Update the existing `reports/environment_report.py` to call `load_industry_analysis_exports()`
2. Pass the `data_dir` pointing to `industry-analysis/data/exports/`
3. Use the returned dict of DataFrames to populate the report sections

The report should have these sections:

1. Executive Summary
   - High-level findings from industry risk scores and property market overlays
   - Current macro regime flag and its interpretation

2. Industry Risk Outlook
   - Table of industry risk scores, ordered by `industry_base_risk_score` desc
   - Commentary on top 3 highest-risk industries and what's driving their scores
   
3. Property Market Outlook
   - Table of property market segments, grouped by `cycle_stage` (Downturn, Slowing, Neutral, Growth)
   - Commentary on which segments are most at-risk and which are benefiting from tailwinds

4. Downturn Scenario Overlays
   - The `downturn_overlay_table` rendered as-is
   - Interpretation of what each scenario means (Mild = X, Moderate = Y, Severe = Z)
   
5. Methodology Notes
   - Surface the "Construction ranking methodology review item" from industry-analysis here
   - Explain that industry risk scores reflect structural factors only, not real-time sector stress
   - Mention the three options considered (accept as-is, add sector-stress overlay, document limitation)
   - State that this report takes the "document limitation" path pending further methodology review

### Step 3: Update `reports/environment_report.py` to emit three file formats

Just like Report 1, we want Report 2 available as:
- Word document (`Report_Environment_Q1_2026.docx`)
- HTML (`Report_Environment_Q1_2026.html`) 
- Markdown (Board and Technical variants)

1. Refactor `reports/environment_report.py` to return a structured report object, just like `reports/benchmark_report.py` does
2. The report object should be a dict with keys for `title`, `subtitle`, `generation_date`, `sections` (list of dicts, each with `name` and `content`), etc.
3. Create `reports/render_environment_docx.py`, `reports/render_environment_html.py`, and `reports/render_environment_md.py`
4. These are the equivalent of the existing `reports/render_benchmark_*.py` files, just for Report 2
5. Wire these renderers up to the existing `scripts/generate_reports.py` so we can run:

```
python scripts/generate_reports.py environment --format docx
python scripts/generate_reports.py environment --format html
python scripts/generate_reports.py environment --format markdown
```

And get Report 2 in all three formats.

### Step 4: Add tests

1. Add a new file `tests/test_industry_context.py`
2. Write a test that calls `load_industry_analysis_exports()` on a fixture directory with sample parquet files
3. Assert that the returned dict has the expected keys and DataFrame shapes
4. Add a new file `tests/test_environment_report.py`
5. Write a test that calls `reports.environment_report.generate()` and asserts the returned report object has the expected sections and data

### Step 5: Generate the reports

1. Ensure `industry-analysis/data/exports/` contains fresh parquet files (re-run the industry-analysis pipeline if needed)
2. Run `python scripts/generate_reports.py environment --format all`
3. Confirm that `outputs/reports/Report_Environment_Q1_2026.docx`, `.html`, `_Board.md`, and `_Technical.md` are all generated
4. Open the DOCX and eyeball the content - it should contain all the sections and commentary we specified above

### Step 6: Documentation

1. Update `README.md` to mention Report 2 and its inputs from industry-analysis
2. Update `CHANGELOG.md` to note the new Report 2 feature and the industry-analysis integration
3. Update `METHODOLOGY.md` to mention that industry risk scores and property market overlays come from the industry-analysis project, and to surface the Construction ranking caveat

## Acceptance Criteria

- [ ] `ingestion/industry_context.py` exists and correctly loads the industry-analysis parquet files
- [ ] `reports/environment_report.py` generates a structured report object with the expected sections and data
- [ ] `reports/render_environment_*.py` files exist for DOCX, HTML, and Markdown
- [ ] `scripts/generate_reports.py environment --format all` generates Report 2 in all four file formats
- [ ] `tests/test_industry_context.py` and `tests/test_environment_report.py` exist and pass
- [ ] `README.md`, `CHANGELOG.md`, and `METHODOLOGY.md` are updated to reflect the industry-analysis integration and Report 2
- [ ] Manual review of `Report_Environment_Q1_2026.docx` confirms it contains all expected sections and data

## Out of Scope

- Bidirectional integration (we're only reading from industry-analysis, not writing back)
- Modifying the industry-analysis pipeline or exports
- Changing the format or structure of Report 1
- Adding new data sources or risk models (we're just wiring existing data into a new report)

## Risks and Mitigations

- **Data drift:** If the industry-analysis pipeline changes its export schemas, our ingestion code will break. Mitigation: pin to a specific version of industry-analysis (e.g. v1.0) and upgrade deliberately.

- **Stale data:** If we don't re-run industry-analysis, we'll be generating Report 2 with stale data. Mitigation: add a freshness check in `ingestion/industry_context.py` that warns if the parquet files are older than X days.

- **Performance:** Loading all that parquet data could slow down report generation. Mitigation: profile the code and optimize if needed (e.g. load only the columns we need, not the entire DataFrames).

## Open Questions

1. Do we want bidirectional integration (i.e. should benchmark engine write anything back to industry-analysis)? Current assumption: no, but we should discuss with stakeholders.

2. How often do we expect to refresh the industry-analysis data? Current assumption: quarterly, but we should align with their release cadence.

3. Do we need any special handling for industries or property segments that appear in one project but not the other (e.g. if industry-analysis adds a new industry category)? Current assumption: no, but we should think through edge cases.

## Definition of Done

1. `external_benchmark_engine` generates a polished, standalone Report 2 in DOCX, HTML, and Markdown variants
2. Report 2 accurately reflects the latest industry-analysis data and renders all sections and commentary as designed
3. All tests pass and documentation is updated
4. Stakeholders have reviewed and approved the final Report 2 output

## Effort Estimate

2 days (16 hours) for initial integration and report generation
1 day (8 hours) for testing, documentation, and iteration
TOTAL: 3 days (24 hours)

Note: this may vary depending on how much refactoring of existing code is required and any unexpected data wrangling challenges. We'll reassess after the first day.

## Sign-off

___ Business Sponsor ___ Technical Lead ___ Product Owner
