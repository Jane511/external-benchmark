# External Benchmark Report — Q1 2026
_Generated: 2026-04-27T01:50:49+00:00_

> **The engine publishes raw, source-attributable observations only.** No adjustments — definition alignment, selection bias, downturn overlays — are applied. These have moved to consuming projects (PD workbook for PD, LGD project for LGD, etc.) so each use case can manage its own complete adjustment chain. Consumers of this report apply their own adjustments per their model documentation.

## 1. Executive Summary
- 10 raw observations across 2 canonical segments.
- 9 distinct sources contributing: 4 Big 4 + 5 non-bank / aggregate.
- Every value in this report is the source-published raw figure. No multipliers, no triangulation, no adjustment.

## 2. Per-source raw observations by segment
### commercial_property

| Source | Source type | Param | Value | As-of | Vintage | Methodology | Page/Table |
| --- | --- | --- | ---:| --- | --- | --- | --- |
| anz | bank_pillar3 | pd | 2.6500% | 2026-02-26 | Pillar 3 quarterly disclosure | CR6 EAD-weighted Average PD | ANZ Pillar 3 Q1 2026 — Table CR6 row 6 (Commercial property) |
| cba | bank_pillar3 | pd | 2.5000% | 2026-03-13 | Pillar 3 trailing 4-quarter average | CR6 EAD-weighted Average PD across PD bands | CBA Pillar 3 Q1 2026 — Table CR6 row 4 (Commercial property) |
| judo | non_bank_listed | pd | 4.2000% | 2025-12-28 | Half-yearly Pillar 3 disclosure (ADI status since 2019) | Average PD on commercial real estate book — H1 FY26 | Judo H1 FY26 Pillar 3 — CR6 commercial real estate |
| liberty | non_bank_listed | pd | 5.3000% | 2025-10-09 | Annual report — credit risk section | 90+ days arrears proxy on commercial property loans (no Pillar 3 published) | Liberty FY25 Annual Report — Credit risk note 7 |
| nab | bank_pillar3 | pd | 2.8500% | 2026-02-26 | Pillar 3 quarterly disclosure | CR6 EAD-weighted Average PD | NAB Pillar 3 Q1 2026 — Table CR6 row 5 (Commercial real estate) |
| pepper | non_bank_listed | pd | 4.8000% | 2025-11-28 | Half-yearly results — credit performance | Net credit losses % on commercial loan book — H1 FY26 | Pepper H1 FY26 results pack — slide 18 |
| wbc | bank_pillar3 | pd | 2.4000% | 2026-02-26 | Pillar 3 quarterly disclosure | CR6 EAD-weighted Average PD | Westpac Pillar 3 Q1 2026 — Table CR6 row 4 |

### residential_mortgage

| Source | Source type | Param | Value | As-of | Vintage | Methodology | Page/Table |
| --- | --- | --- | ---:| --- | --- | --- | --- |
| cba | bank_pillar3 | pd | 0.4200% | 2026-03-13 | Pillar 3 trailing 4-quarter average | CR6 EAD-weighted Average PD on residential mortgages | CBA Pillar 3 Q1 2026 — Table CR6 row 1 |
| resimac | non_bank_listed | pd | 0.8500% | 2025-12-28 | Half-yearly results — Prime / Specialist split | Prime mortgage 90+ arrears proxy | Resimac H1 FY26 — slide 12 (Prime arrears) |
| sp_spin | rating_agency_index | pd | 0.9100% | 2026-04-07 | S&P RMBS Performance Index (SPIN), monthly | Total prime + non-conforming arrears proxy for default rate | SPIN March 2026 — total arrears headline |


## 3. Cross-source validation summary

| Segment | N | Spread % | Big 4 spread % | Non-bank/Big 4 ratio | Outliers | Stale sources |
| --- | ---:| ---:| ---:| ---:| --- | --- |
| commercial_property | 7 | 101.8% | 17.5% | 1.86x | - | judo, liberty, pepper |
| residential_mortgage | 3 | 57.6% | - | 2.10x | cba | resimac |

## 4. Big 4 vs non-bank disclosure spread (informational only)

_The values below are raw published figures from each cohort. The engine does NOT recommend any uplift or adjustment from this spread. Consuming projects decide how (or whether) to use it._

| Segment | Big 4 median | Non-bank median | Ratio | Big 4 N | Non-bank N |
| --- | ---:| ---:| ---:| ---:| ---:|
| commercial_property | 2.5750% | 4.8000% | 1.86x | 4 | 3 |
| residential_mortgage | 0.4200% | 0.8800% | 2.10x | 1 | 2 |

## 5. Provenance & methodology footnotes
- **anz** (bank_pillar3): Pillar 3 quarterly disclosure — https://www.anz.com.au/shareholder/centre/reporting/regulatory-disclosure/
- **cba** (bank_pillar3): Pillar 3 trailing 4-quarter average — https://www.commbank.com.au/about-us/investors/regulatory-disclosures.html
- **judo** (non_bank_listed): Half-yearly Pillar 3 disclosure (ADI status since 2019) — https://www.judo.bank/investor-centre/
- **liberty** (non_bank_listed): Annual report — credit risk section — https://www.libertyfinancial.com.au/about/investor-information
- **nab** (bank_pillar3): Pillar 3 quarterly disclosure — https://www.nab.com.au/about-us/shareholder-centre/regulatory-disclosures
- **pepper** (non_bank_listed): Half-yearly results — credit performance — https://www.peppermoney.com.au/investors/
- **resimac** (non_bank_listed): Half-yearly results — Prime / Specialist split — https://www.resimac.com.au/about-resimac/investor-relations/
- **sp_spin** (rating_agency_index): S&P RMBS Performance Index (SPIN), monthly — https://www.spglobal.com/ratings/en/regulatory/topic/spin
- **wbc** (bank_pillar3): Pillar 3 quarterly disclosure — https://www.westpac.com.au/about-westpac/investor-centre/financial-information/regulatory-disclosures/
