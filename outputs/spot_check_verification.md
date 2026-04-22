# Spot-Check Verification — `<PERIOD>`

Record of manual verification of extracted Pillar 3 values against the source PDFs. One row per sampled `source_id`. Populate this file before the DOCX leaves 1LoD for committee review.

**How to sample:** run `python scripts/pick_spot_checks.py` from the project root. It prints 5 randomly-selected rows with enough context to locate each in the source PDF.

## Sign-off

| Reviewed by | Role            | Date         | Outcome                    |
|-------------|-----------------|--------------|----------------------------|
|             | Model Owner     |              | Pass / Fail / Pass w/ notes |

## Samples

| # | source_id | Extracted value | Source PDF + page | PDF value | Match? | Notes |
|---|-----------|-----------------|-------------------|-----------|--------|-------|
| 1 |           |                 |                   |           |        |       |
| 2 |           |                 |                   |           |        |       |
| 3 |           |                 |                   |           |        |       |
| 4 |           |                 |                   |           |        |       |
| 5 |           |                 |                   |           |        |       |

## Any mismatches

_None this cycle._

_(If mismatches are found: record the discrepancy, trace to the adapter, open a remediation ticket, and do not sign off until resolved.)_
