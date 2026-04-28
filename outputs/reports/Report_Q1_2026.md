# External Benchmark Report — Q1 2026
_Generated: 2026-04-28T04:33:53+00:00_

> **The engine publishes raw, source-attributable observations only.** No adjustments — definition alignment, selection bias, downturn overlays — are applied. These have moved to consuming projects (PD workbook for PD, LGD project for LGD, etc.) so each use case can manage its own complete adjustment chain. Consumers of this report apply their own adjustments per their model documentation.

## 1. Executive Summary
- 20 raw observations across 11 canonical segments.
- 20 distinct sources contributing: 16 Big 4 + 4 non-bank / aggregate.
- Every value in this report is the source-published raw figure. No multipliers, no triangulation, no adjustment.

## 2. Per-source raw observations by segment
### commercial_property

| Source | Source type | Param | Value | As-of | Vintage | Methodology | Page/Table |
| --- | --- | --- | ---:| --- | --- | --- | --- |
| ANZ_PILLAR3_CRE_PD_2024H2 | bank_pillar3 | pd | 2.1000% | 2024-12-31 | legacy BenchmarkEntry v1 | ANZ Pillar 3 commercial property PD, H2 2024. | - |
| CBA_PILLAR3_CRE_PD_2024H2 | bank_pillar3 | pd | 2.5000% | 2024-12-31 | legacy BenchmarkEntry v1 | CBA Pillar 3 Table CR6 commercial property PD, H2 2024. | - |
| NAB_PILLAR3_CRE_PD_2024H2 | bank_pillar3 | pd | 2.2000% | 2024-12-31 | legacy BenchmarkEntry v1 | NAB Pillar 3 commercial property PD, H2 2024. | - |
| WBC_PILLAR3_CRE_PD_2024H2 | bank_pillar3 | pd | 2.6000% | 2024-12-31 | legacy BenchmarkEntry v1 | WBC Pillar 3 commercial property PD, H2 2024. | - |

### commercial_property_investment

| Source | Source type | Param | Value | As-of | Vintage | Methodology | Page/Table |
| --- | --- | --- | ---:| --- | --- | --- | --- |
| ANZ_PILLAR3_CRE_2024H2 | bank_pillar3 | pd | 2.1000% | 2024-12-31 | legacy BenchmarkEntry v1 | migrated from Australia and New Zealand Banking Group | - |
| CBA_PILLAR3_CRE_2024H2 | bank_pillar3 | pd | 2.5000% | 2024-12-31 | legacy BenchmarkEntry v1 | migrated from Commonwealth Bank of Australia | - |
| NAB_PILLAR3_CRE_2024H2 | bank_pillar3 | pd | 2.2000% | 2024-12-31 | legacy BenchmarkEntry v1 | migrated from National Australia Bank | - |
| WBC_PILLAR3_CRE_2024H2 | bank_pillar3 | pd | 2.6000% | 2024-12-31 | legacy BenchmarkEntry v1 | migrated from Westpac Banking Corporation | - |

### corporate_sme

| Source | Source type | Param | Value | As-of | Vintage | Methodology | Page/Table |
| --- | --- | --- | ---:| --- | --- | --- | --- |
| ANZ_PILLAR3_CORP_SME_2024H2 | bank_pillar3 | pd | 3.4000% | 2024-12-31 | legacy BenchmarkEntry v1 | migrated from Australia and New Zealand Banking Group | - |
| CBA_PILLAR3_CORP_SME_2024H2 | bank_pillar3 | pd | 2.8000% | 2024-12-31 | legacy BenchmarkEntry v1 | migrated from Commonwealth Bank of Australia | - |
| NAB_PILLAR3_CORP_SME_2024H2 | bank_pillar3 | pd | 3.2000% | 2024-12-31 | legacy BenchmarkEntry v1 | migrated from National Australia Bank | - |
| WBC_PILLAR3_CORP_SME_2024H2 | bank_pillar3 | pd | 2.7000% | 2024-12-31 | legacy BenchmarkEntry v1 | migrated from Westpac Banking Corporation | - |

### development

| Source | Source type | Param | Value | As-of | Vintage | Methodology | Page/Table |
| --- | --- | --- | ---:| --- | --- | --- | --- |
| APS113_SLOTTING_GOOD_PD | apra_performance | pd | 0.8000% | 2024-12-31 | legacy BenchmarkEntry v1 | migrated from APRA | - |
| APS113_SLOTTING_SATIS_PD | apra_performance | pd | 2.8000% | 2024-12-31 | legacy BenchmarkEntry v1 | migrated from APRA | - |
| APS113_SLOTTING_STRONG_PD | apra_performance | pd | 0.4000% | 2024-12-31 | legacy BenchmarkEntry v1 | APS 113 specialised lending: Strong grade PD | - |
| APS113_SLOTTING_WEAK_PD | apra_performance | pd | 8.0000% | 2024-12-31 | legacy BenchmarkEntry v1 | migrated from APRA | - |

### residential_mortgage

| Source | Source type | Param | Value | As-of | Vintage | Methodology | Page/Table |
| --- | --- | --- | ---:| --- | --- | --- | --- |
| ANZ_PILLAR3_RES_2024H2 | bank_pillar3 | pd | 0.8000% | 2024-12-31 | legacy BenchmarkEntry v1 | migrated from Australia and New Zealand Banking Group | - |
| CBA_PILLAR3_RES_2024H2 | bank_pillar3 | pd | 0.7200% | 2024-12-31 | legacy BenchmarkEntry v1 | CBA residential PD disclosure H2 2024 | - |
| NAB_PILLAR3_RES_2024H2 | bank_pillar3 | pd | 0.9000% | 2024-12-31 | legacy BenchmarkEntry v1 | migrated from National Australia Bank | - |
| WBC_PILLAR3_RES_2024H2 | bank_pillar3 | pd | 0.8800% | 2024-12-31 | legacy BenchmarkEntry v1 | migrated from Westpac Banking Corporation | - |


## 3. Cross-source validation summary

| Segment | N | Spread % | Big 4 spread % | Non-bank/Big 4 ratio | Outliers | Stale sources |
| --- | ---:| ---:| ---:| ---:| --- | --- |
| commercial_property | 4 | 21.3% | 21.3% | 1.00x | - | ANZ_PILLAR3_CRE_PD_2024H2, CBA_PILLAR3_CRE_PD_2024H2, NAB_PILLAR3_CRE_PD_2024H2, WBC_PILLAR3_CRE_PD_2024H2 |
| commercial_property_investment | 4 | 21.3% | 21.3% | 1.00x | - | ANZ_PILLAR3_CRE_2024H2, CBA_PILLAR3_CRE_2024H2, NAB_PILLAR3_CRE_2024H2, WBC_PILLAR3_CRE_2024H2 |
| corporate_sme | 4 | 23.3% | 23.3% | 1.00x | - | ANZ_PILLAR3_CORP_SME_2024H2, CBA_PILLAR3_CORP_SME_2024H2, NAB_PILLAR3_CORP_SME_2024H2, WBC_PILLAR3_CORP_SME_2024H2 |
| development | 4 | 422.2% | - | - | APS113_SLOTTING_GOOD_PD, APS113_SLOTTING_STRONG_PD, APS113_SLOTTING_WEAK_PD | APS113_SLOTTING_GOOD_PD, APS113_SLOTTING_SATIS_PD, APS113_SLOTTING_STRONG_PD, APS113_SLOTTING_WEAK_PD |
| residential_mortgage | 4 | 21.4% | 21.4% | 1.00x | - | ANZ_PILLAR3_RES_2024H2, CBA_PILLAR3_RES_2024H2, NAB_PILLAR3_RES_2024H2, WBC_PILLAR3_RES_2024H2 |

## 4. Big 4 vs non-bank disclosure spread (informational only)

_The values below are raw published figures from each cohort. The engine does NOT recommend any uplift or adjustment from this spread. Consuming projects decide how (or whether) to use it._

| Segment | Big 4 median | Non-bank median | Ratio | Big 4 N | Non-bank N |
| --- | ---:| ---:| ---:| ---:| ---:|
| commercial_property | 2.3500% | - | - | 4 | 0 |
| commercial_property_investment | 2.3500% | - | - | 4 | 0 |
| corporate_sme | 3.0000% | - | - | 4 | 0 |
| development | - | 1.8000% | - | 0 | 4 |
| residential_mortgage | 0.8400% | - | - | 4 | 0 |

## 5. Provenance & methodology footnotes
- **ANZ_PILLAR3_CORP_SME_2024H2** (bank_pillar3): legacy BenchmarkEntry v1 — https://www.anz.com/pillar3
- **ANZ_PILLAR3_CRE_2024H2** (bank_pillar3): legacy BenchmarkEntry v1 — https://www.anz.com/pillar3
- **ANZ_PILLAR3_CRE_PD_2024H2** (bank_pillar3): legacy BenchmarkEntry v1 — https://www.anz.com/shareholder/centre/reporting/regulatory-disclosure/
- **ANZ_PILLAR3_RES_2024H2** (bank_pillar3): legacy BenchmarkEntry v1 — https://www.anz.com/pillar3
- **APS113_SLOTTING_GOOD_PD** (apra_performance): legacy BenchmarkEntry v1 — https://www.apra.gov.au/aps-113
- **APS113_SLOTTING_SATIS_PD** (apra_performance): legacy BenchmarkEntry v1 — https://www.apra.gov.au/aps-113
- **APS113_SLOTTING_STRONG_PD** (apra_performance): legacy BenchmarkEntry v1 — https://www.apra.gov.au/aps-113
- **APS113_SLOTTING_WEAK_PD** (apra_performance): legacy BenchmarkEntry v1 — https://www.apra.gov.au/aps-113
- **CBA_PILLAR3_CORP_SME_2024H2** (bank_pillar3): legacy BenchmarkEntry v1 — https://www.commbank.com.au/pillar3
- **CBA_PILLAR3_CRE_2024H2** (bank_pillar3): legacy BenchmarkEntry v1 — https://www.commbank.com.au/pillar3
- **CBA_PILLAR3_CRE_PD_2024H2** (bank_pillar3): legacy BenchmarkEntry v1 — https://www.commbank.com.au/about-us/investors/annual-reports.html
- **CBA_PILLAR3_RES_2024H2** (bank_pillar3): legacy BenchmarkEntry v1 — https://www.commbank.com.au/pillar3
- **NAB_PILLAR3_CORP_SME_2024H2** (bank_pillar3): legacy BenchmarkEntry v1 — https://www.nab.com.au/pillar3
- **NAB_PILLAR3_CRE_2024H2** (bank_pillar3): legacy BenchmarkEntry v1 — https://www.nab.com.au/pillar3
- **NAB_PILLAR3_CRE_PD_2024H2** (bank_pillar3): legacy BenchmarkEntry v1 — https://www.nab.com.au/about-us/shareholder-centre/regulatory-disclosures
- **NAB_PILLAR3_RES_2024H2** (bank_pillar3): legacy BenchmarkEntry v1 — https://www.nab.com.au/pillar3
- **WBC_PILLAR3_CORP_SME_2024H2** (bank_pillar3): legacy BenchmarkEntry v1 — https://www.westpac.com.au/pillar3
- **WBC_PILLAR3_CRE_2024H2** (bank_pillar3): legacy BenchmarkEntry v1 — https://www.westpac.com.au/pillar3
- **WBC_PILLAR3_CRE_PD_2024H2** (bank_pillar3): legacy BenchmarkEntry v1 — https://www.westpac.com.au/financial-information/regulatory-disclosures/
- **WBC_PILLAR3_RES_2024H2** (bank_pillar3): legacy BenchmarkEntry v1 — https://www.westpac.com.au/pillar3

## 6. Raw data inventory

_Walk of `data/raw` — 33 file(s) staged across 5 source families. Includes `_MANUAL.md` / `*_GATE.md` notes for sources that require manual download._

### apra

| File | Subfolder | Kind | Size | Modified (UTC) |
| --- | --- | --- | ---:| --- |
| Quarterly%20authorised%20deposit-taking%20institution%20performance-September%202004%20to%20December%202025.xlsx | - | xlsx | 1,829,863 | 2026-04-21T08:31:18+00:00 |
| Quarterly%20authorised%20deposit-taking%20institution%20property%20exposures%20statistics%20December%202025.xlsx | - | xlsx | 542,614 | 2026-04-21T08:31:18+00:00 |

### external_indices

| File | Subfolder | Kind | Size | Modified (UTC) |
| --- | --- | --- | ---:| --- |
| _MANUAL.md | sp_spin | manual_note | 2,105 | 2026-04-28T04:18:21+00:00 |
| rmbs arrears statistics australia excluding non capital market issuance feb.2026.pdf | sp_spin | pdf | 1,802,775 | 2026-04-28T01:47:23+00:00 |
| rmbs arrears statistics australia excluding non capital market issuance feb.2026.xls | sp_spin | xls | 1,035,264 | 2026-04-28T01:48:25+00:00 |

### non_bank

| File | Subfolder | Kind | Size | Modified (UTC) |
| --- | --- | --- | ---:| --- |
| _MANUAL.md | judo | manual_note | 554 | 2026-04-28T01:15:28+00:00 |
| _MANUAL.md | liberty | manual_note | 781 | 2026-04-28T01:15:29+00:00 |
| 69e58bc63e1cf5d30c16c8ac_2603_20-_20MREIF_20Monthly_20Report.pdf | metrics_credit | pdf | 548,389 | 2026-04-28T01:15:39+00:00 |
| MONEYME_1H26_Interim_Report_and_Results.pdf | moneyme | pdf | 4,091,522 | 2026-04-28T01:15:33+00:00 |
| _MANUAL.md | pepper | manual_note | 645 | 2026-04-28T01:16:13+00:00 |
| Pepper_20Green_20Annual_20Review_202024.pdf | pepper | pdf | 199,252 | 2026-04-27T22:39:50+00:00 |
| Results_presentation_df72650cc7.pdf | plenti | pdf | 2,266,622 | 2026-04-28T01:15:34+00:00 |
| _MANUAL.md | qualitas | manual_note | 632 | 2026-04-28T01:15:36+00:00 |
| _MANUAL.md | resimac | manual_note | 770 | 2026-04-28T01:15:31+00:00 |
| _MANUAL.md | wisr | manual_note | 694 | 2026-04-28T01:15:35+00:00 |

### pillar3

| File | Subfolder | Kind | Size | Modified (UTC) |
| --- | --- | --- | ---:| --- |
| 2026-first-quarter-pillar-3-report.pdf | - | pdf | 1,543,042 | 2026-04-21T08:31:30+00:00 |
| ANZ_FY2025_Pillar3_Annual.pdf | - | pdf | 1,923,829 | 2026-04-21T12:49:38+00:00 |
| ANZ_H1_2025_Pillar3.pdf | - | pdf | 15 | 2026-04-21T12:55:39+00:00 |
| Basel-III-Pillar3-quantitative-info-30Sept25.xlsx | - | xlsx | 722,633 | 2026-04-21T08:31:30+00:00 |
| CBA_FY2025_Pillar3_Annual.pdf | - | pdf | 2,354,558 | 2026-04-21T11:33:32+00:00 |
| CBA_H1_2026_Pillar3_Quantitative.xlsx | - | xlsx | 58,521 | 2026-04-21T09:33:01+00:00 |
| December-2025-Pillar-3-disclosure.pdf | - | pdf | 493,014 | 2026-04-21T08:31:32+00:00 |
| mannual validation.md | - | manual_note | 7,265 | 2026-04-22T03:04:43+00:00 |
| NAB_FY2025_Pillar3_Annual.pdf | - | pdf | 5,204,265 | 2026-04-21T12:49:37+00:00 |
| NAB_H1_2025_Pillar3.pdf | - | pdf | 15 | 2026-04-21T12:55:39+00:00 |
| wbc-december-Pillar-3-report.pdf | - | pdf | 1,061,399 | 2026-04-21T08:31:31+00:00 |
| WBC_FY2025_Pillar3_Annual.pdf | - | pdf | 2,059,066 | 2026-04-21T12:49:37+00:00 |
| WBC_H1_2025_Pillar3.pdf | - | pdf | 15 | 2026-04-21T12:55:39+00:00 |
| WBC_H1_2026_Pillar3.pdf | - | pdf | 61,374 | 2026-04-21T12:55:49+00:00 |

### rba

| File | Subfolder | Kind | Size | Modified (UTC) |
| --- | --- | --- | ---:| --- |
| financial-stability-review-2025-10.pdf | - | pdf | 3,837,206 | 2026-04-28T02:15:33+00:00 |
| financial-stability-review-2026-03.pdf | - | pdf | 3,272,062 | 2026-04-27T22:39:34+00:00 |
| SECURITISATION_GATE.md | - | manual_note | 1,856 | 2026-04-28T01:03:14+00:00 |
| securitisation_landing.html | - | html_snapshot | 28,364 | 2026-04-27T22:39:34+00:00 |

