# Australian Credit Risk Benchmarks - Q2 2026
_Generated: 2026-06-16T01:50:10+00:00 | Data as-of: 2024-12-31–2026-02-28_

## Executive summary

This report consolidates externally-disclosed credit-risk parameters for Australian bank and non-bank lenders into a single set of model-ready benchmarks. It is built from public Basel Pillar 3 disclosures, APRA and RBA statistics, and non-bank lender reports, and is aligned to the APRA APS 113 / Basel IRB framework.

Every figure is a source-published value — no adjustment, triangulation, or modelling overlay — so each number traces back to a named disclosure and reporting date.

### What this report covers

- **Probability of default (PD)** — likelihood a borrower defaults within 12 months, by credit segment (Section 1).
- **Loss given default (LGD)** — share of exposure not recovered after default (Section 2).
- **Expected loss (EL = PD × LGD)** — the headline credit-loss rate per segment (Section 3).
- **Stress testing** — PD, LGD and EAD under a base / mild / severe scenario set (mild = Basel CRE36.51 two-quarters-zero-growth), stressed PD floored at APS 113 regulatory bands (Section 4).
- **Portfolio monitoring** — arrears, non-performing, impaired and loss-rate metrics for early-warning tracking (Section 5).
- **Per-bank industry exposures** — Big 4 exposure, non-performing, provision and write-off by industry sector (Section 6).

### Coverage at a glance

- 295 source observations across 16 credit segments.
- 4 banks in the industry-exposure view (ANZ, CBA, NAB, WBC), plus non-bank lenders and regulatory references.
- Data as-of window: 2024-12-31–2026-02-28.

### How to read the numbers

- Rates are decimals in [0, 1]; for example, 0.03 represents three percent.
- Expected-loss rate = PD × LGD, shown in basis points (bps); 1 bp = 0.01%, so 14 bps = 0.14%.
- Stressed PD/LGD/EAD apply per-scenario multipliers (config/stress_scenarios.yaml); stressed PD is floored at the APS 113 reality-check bands. Multipliers are illustrative, not calibrated regulatory parameters.
- "As-of" is the disclosure date of the most recent source.

## 1. PD Inputs

| Segment | Product | PD decimal | Source | As-of |
| --- | --- | --- | --- | --- |
| Commercial Property | commercial_property | 0.02 | ANZ_PILLAR3_CRE_PD_2024H2 | 2024-12-31 |
| Commercial Property | commercial_property | 0.03 | CBA_PILLAR3_CRE_PD_2024H2 | 2024-12-31 |
| Commercial Property | commercial_property | 0.01 | MACQUARIE_BANK_COMMERCIAL_PROPERTY_INVESTMENT_PD_H1FY2026 | 2025-09-30 |
| Commercial Property | commercial_property | 0.02 | NAB_PILLAR3_CRE_PD_2024H2 | 2024-12-31 |
| Commercial Property | commercial_property | 0.03 | WBC_PILLAR3_CRE_PD_2024H2 | 2024-12-31 |
| Corporate General | corporate_general | 0.02 | MACQUARIE_BANK_CORPORATE_GENERAL_PD_H1FY2026 | 2025-09-30 |
| Corporate SME | term_loan | 0.03 | ANZ_PILLAR3_CORP_SME_2024H2 | 2024-12-31 |
| Corporate SME | term_loan | 0.03 | CBA_PILLAR3_CORP_SME_2024H2 | 2024-12-31 |
| Corporate SME | term_loan | 0.03 | MACQUARIE_BANK_CORPORATE_SME_PD_H1FY2026 | 2025-09-30 |
| Corporate SME | term_loan | 0.03 | NAB_PILLAR3_CORP_SME_2024H2 | 2024-12-31 |
| Corporate SME | term_loan | 0.03 | WBC_PILLAR3_CORP_SME_2024H2 | 2024-12-31 |
| Development | development | 0.01 | APS113_SLOTTING_GOOD_PD | 2024-12-31 |
| Development | development | 0.03 | APS113_SLOTTING_SATIS_PD | 2024-12-31 |
| Development | development | 0.00 | APS113_SLOTTING_STRONG_PD | 2024-12-31 |
| Development | development | 0.08 | APS113_SLOTTING_WEAK_PD | 2024-12-31 |
| Financial Institution | financial_institution | 0.00 | MACQUARIE_BANK_FINANCIAL_INSTITUTION_PD_H1FY2026 | 2025-09-30 |
| Residential Mortgage | residential_mortgage | 0.01 | ANZ_PILLAR3_RES_2024H2 | 2024-12-31 |
| Residential Mortgage | residential_mortgage | 0.01 | CBA_PILLAR3_RES_2024H2 | 2024-12-31 |
| Residential Mortgage | residential_mortgage | 0.01 | MACQUARIE_BANK_RESIDENTIAL_MORTGAGE_PD_H1FY2026 | 2025-09-30 |
| Residential Mortgage | residential_mortgage | 0.01 | NAB_PILLAR3_RES_2024H2 | 2024-12-31 |
| Residential Mortgage | residential_mortgage | 0.01 | WBC_PILLAR3_RES_2024H2 | 2024-12-31 |
| Retail Other | retail_other | 0.02 | MACQUARIE_BANK_RETAIL_OTHER_PD_H1FY2026 | 2025-09-30 |
| Retail SME | retail_sme | 0.03 | MACQUARIE_BANK_RETAIL_SME_PD_H1FY2026 | 2025-09-30 |
| Sovereign | sovereign | 0.01 | MACQUARIE_BANK_SOVEREIGN_PD_H1FY2026 | 2025-09-30 |

## 2. LGD Inputs

| Segment | Product | LGD decimal | Source | As-of |
| --- | --- | --- | --- | --- |
| Commercial Property | commercial_property | 0.17 | APS113_CRE_LGD_FLOOR | 2024-12-31 |
| Commercial Property | commercial_property | 0.24 | MACQUARIE_BANK_COMMERCIAL_PROPERTY_INVESTMENT_LGD_H1FY2026 | 2025-09-30 |
| Corporate General | corporate_general | 0.45 | MACQUARIE_BANK_CORPORATE_GENERAL_LGD_H1FY2026 | 2025-09-30 |
| Corporate SME | term_loan | 0.42 | MACQUARIE_BANK_CORPORATE_SME_LGD_H1FY2026 | 2025-09-30 |
| Development | development | 0.33 | APS113_SLOTTING_GOOD_LGD | 2024-12-31 |
| Development | development | 0.38 | APS113_SLOTTING_SATIS_LGD | 2024-12-31 |
| Development | development | 0.28 | APS113_SLOTTING_STRONG_LGD | 2024-12-31 |
| Development | development | 0.45 | APS113_SLOTTING_WEAK_LGD | 2024-12-31 |
| Financial Institution | financial_institution | 0.49 | MACQUARIE_BANK_FINANCIAL_INSTITUTION_LGD_H1FY2026 | 2025-09-30 |
| Invoice Finance | invoice_finance | 0.35 | APS113_INVOICE_LGD_FLOOR | 2024-12-31 |
| Residential Mortgage | residential_mortgage | 0.07 | APS113_RES_LGD_FLOOR | 2024-12-31 |
| Residential Mortgage | residential_mortgage | 0.22 | CBA_PILLAR3_RES_LGD_2024H2 | 2024-12-31 |
| Residential Mortgage | residential_mortgage | 0.13 | MACQUARIE_BANK_RESIDENTIAL_MORTGAGE_LGD_H1FY2026 | 2025-09-30 |
| Residential Mortgage | residential_mortgage | 0.24 | NAB_PILLAR3_RES_LGD_2024H2 | 2024-12-31 |
| Retail Other | retail_other | 0.36 | MACQUARIE_BANK_RETAIL_OTHER_LGD_H1FY2026 | 2025-09-30 |
| Retail SME | retail_sme | 0.41 | MACQUARIE_BANK_RETAIL_SME_LGD_H1FY2026 | 2025-09-30 |
| Sovereign | sovereign | 0.05 | MACQUARIE_BANK_SOVEREIGN_LGD_H1FY2026 | 2025-09-30 |
| Working Capital Unsecured | line_of_credit | 0.45 | APS113_UNSECURED_LGD | 2024-12-31 |
| Working Capital Unsecured | line_of_credit | 0.48 | CBA_PILLAR3_SME_UNSECURED_LGD | 2024-12-31 |

## 3. Expected Loss Inputs

| Segment | Product | PD decimal | LGD decimal | EL rate (bps) | PD N | LGD N | As-of |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Commercial Property | commercial_property | 0.02 | 0.21 | 46 bps | 5 | 2 | 2025-09-30 |
| Corporate General | corporate_general | 0.02 | 0.45 | 77 bps | 1 | 1 | 2025-09-30 |
| Corporate SME | term_loan | 0.03 | 0.42 | 117 bps | 5 | 1 | 2025-09-30 |
| Development | development | 0.02 | 0.35 | 63 bps | 4 | 4 | 2024-12-31 |
| Financial Institution | financial_institution | 0.00 | 0.49 | 24 bps | 1 | 1 | 2025-09-30 |
| Residential Mortgage | residential_mortgage | 0.01 | 0.17 | 14 bps | 5 | 4 | 2025-09-30 |
| Retail Other | retail_other | 0.02 | 0.36 | 57 bps | 1 | 1 | 2025-09-30 |
| Retail SME | retail_sme | 0.03 | 0.41 | 120 bps | 1 | 1 | 2025-09-30 |
| Sovereign | sovereign | 0.01 | 0.05 | 5 bps | 1 | 1 | 2025-09-30 |

## 4. Stress Testing Inputs

| Segment | Product | Scenario | Base EL (bps) | Stressed PD decimal | Stressed LGD decimal | Stressed EL (bps) | Stressed EL incl EAD (bps) | As-of |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Commercial Property | commercial_property | base | 46 bps | 0.02 | 0.21 | 46 bps | 46 bps | 2025-09-30 |
| Commercial Property | commercial_property | mild | 46 bps | 0.05 | 0.25 | 125 bps | 138 bps | 2025-09-30 |
| Commercial Property | commercial_property | severe | 46 bps | 0.06 | 0.29 | 161 bps | 201 bps | 2025-09-30 |
| Corporate General | corporate_general | base | 77 bps | 0.02 | 0.45 | 77 bps | 77 bps | 2025-09-30 |
| Corporate General | corporate_general | mild | 77 bps | 0.03 | 0.55 | 139 bps | 153 bps | 2025-09-30 |
| Corporate General | corporate_general | severe | 77 bps | 0.04 | 0.64 | 270 bps | 337 bps | 2025-09-30 |
| Corporate SME | term_loan | base | 117 bps | 0.03 | 0.42 | 117 bps | 117 bps | 2025-09-30 |
| Corporate SME | term_loan | mild | 117 bps | 0.06 | 0.50 | 301 bps | 331 bps | 2025-09-30 |
| Corporate SME | term_loan | severe | 117 bps | 0.07 | 0.59 | 410 bps | 512 bps | 2025-09-30 |
| Development | development | base | 63 bps | 0.02 | 0.35 | 63 bps | 63 bps | 2024-12-31 |
| Development | development | mild | 63 bps | 0.05 | 0.42 | 210 bps | 231 bps | 2024-12-31 |
| Development | development | severe | 63 bps | 0.05 | 0.49 | 245 bps | 306 bps | 2024-12-31 |
| Financial Institution | financial_institution | base | 24 bps | 0.00 | 0.49 | 24 bps | 24 bps | 2025-09-30 |
| Financial Institution | financial_institution | mild | 24 bps | 0.01 | 0.59 | 43 bps | 48 bps | 2025-09-30 |
| Financial Institution | financial_institution | severe | 24 bps | 0.01 | 0.69 | 85 bps | 106 bps | 2025-09-30 |
| Residential Mortgage | residential_mortgage | base | 14 bps | 0.01 | 0.17 | 14 bps | 14 bps | 2025-09-30 |
| Residential Mortgage | residential_mortgage | mild | 14 bps | 0.01 | 0.21 | 25 bps | 28 bps | 2025-09-30 |
| Residential Mortgage | residential_mortgage | severe | 14 bps | 0.02 | 0.24 | 49 bps | 61 bps | 2025-09-30 |
| Retail Other | retail_other | base | 57 bps | 0.02 | 0.36 | 57 bps | 57 bps | 2025-09-30 |
| Retail Other | retail_other | mild | 57 bps | 0.02 | 0.44 | 102 bps | 112 bps | 2025-09-30 |
| Retail Other | retail_other | severe | 57 bps | 0.04 | 0.51 | 198 bps | 247 bps | 2025-09-30 |
| Retail SME | retail_sme | base | 120 bps | 0.03 | 0.41 | 120 bps | 120 bps | 2025-09-30 |
| Retail SME | retail_sme | mild | 120 bps | 0.04 | 0.50 | 215 bps | 237 bps | 2025-09-30 |
| Retail SME | retail_sme | severe | 120 bps | 0.07 | 0.58 | 419 bps | 523 bps | 2025-09-30 |
| Sovereign | sovereign | base | 5 bps | 0.01 | 0.05 | 5 bps | 5 bps | 2025-09-30 |
| Sovereign | sovereign | mild | 5 bps | 0.01 | 0.06 | 8 bps | 9 bps | 2025-09-30 |
| Sovereign | sovereign | severe | 5 bps | 0.02 | 0.07 | 16 bps | 21 bps | 2025-09-30 |

## 4a. Stress Scenarios & Governance

**Scenario macro paths** — shocks map from a stated path:

- **Base (current environment)** — Current conditions, no recession overlay. GDP growth around trend, unemployment broadly stable, cash rate and property prices at the latest observed levels (RBA SMP / FSR baseline).
- **Mild recession (Basel CRE36.51 minimum)** — Basel CRE36.51 mandatory minimum — two consecutive quarters of zero GDP growth. Unemployment rises ~1.0-1.5pp, modest property-price softening (~5-10%), and some drawdown of undrawn limits as borrowers seek liquidity (PD/LGD/EAD all stressed).
- **Severe recession (GFC-like)** — GFC-like severe-but-plausible downturn (APS 220 para 72). Multi-quarter contraction, unemployment +3-4pp, property prices -20% to -30%, and a sharp utilisation spike on revolving facilities into default.

## Reverse stress — multiplier that breaches the reality-check band

| Segment | Product | Base PD | Upper band | Breach PD × |
| --- | --- | --- | --- | --- |
| Commercial Property | commercial_property | 0.02 | 0.05 | 2.27x |
| Corporate SME | term_loan | 0.03 | 0.06 | 2.14x |
| Development | development | 0.02 | 0.05 | 2.78x |

- No diversification benefit is assumed (APG 113 para 92): each scenario is applied per segment with no offsetting correlation or portfolio-diversification relief.
- Feeds limits / capital (APS 220 para 73, APG 110): the stressed EL rate is read against the consuming book's risk-appetite limits and ICAAP capital-vs-buffer assessment; a breach triggers an origination / pricing / limit-tightening review.
- Scenario multipliers are illustrative reality-check overlays, not calibrated regulatory parameters. In production they would be derived from a full macroeconomic scenario model and independently validated at least annually, with scenarios, assumptions and limitations documented. Last reviewed 2026-04-28; next review due 2026-10-31.

## 5. Portfolio Monitor Inputs

| Segment | Product | Arrears decimal | NPL decimal | Impaired decimal | Loss rate decimal | Sources | As-of |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ADI Sector Total | adi_sector_total | 0.01 | 0.01 | - | - | 6 | 2025-12-31 |
| Bridging Residential | bridging | - | - | - | 0.08 | 1 | 2024-12-31 |
| Commercial Property | commercial_property | - | 0.01 | 0.01 | - | 4 | 2025-12-31 |
| Consumer Secured | consumer_secured | - | - | 0.00 | - | 1 | 2025-12-31 |
| Corporate SME | term_loan | 0.02 | - | 0.03 | 0.01 | 4 | 2025-12-31 |
| Invoice Finance | invoice_finance | 0.01 | - | - | - | 1 | 2024-12-31 |
| Residential Mortgage | residential_mortgage | 0.01 | 0.01 | - | 0.00 | 8 | 2026-02-28 |
| Residential Mortgage Specialist | residential_mortgage_specialist | 0.03 | - | 0.01 | 0.00 | 7 | 2026-02-28 |
| SME Corporate | term_loan | 0.01 | 0.01 | - | - | 2 | 2024-12-31 |

## 6. Per-Bank Industry Inputs

| Bank | Industry | Exposure AUDm | NPE AUDm | NPE decimal | Provision AUDm | Write-offs AUDm | Write-off decimal | As-of |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| CBA | Agriculture & Forestry | 33156.00 | 343.00 | 0.01 | 35.00 | 8.00 | 0.00 | 2025-06-30 |
| CBA | Business Services | 15143.00 | 177.00 | 0.01 | 59.00 | 20.00 | 0.00 | 2025-06-30 |
| CBA | Commercial Property | 97549.00 | 353.00 | 0.00 | 30.00 | 2.00 | 0.00 | 2025-06-30 |
| CBA | Construction | 11623.00 | 329.00 | 0.03 | 117.00 | 55.00 | 0.00 | 2025-06-30 |
| CBA | Consumer | 828841.00 | 8205.00 | 0.01 | 592.00 | 408.00 | 0.00 | 2025-06-30 |
| CBA | Electricity, Gas & Water | 14049.00 | - | - | - | - | - | 2025-06-30 |
| CBA | Entertainment, Leisure & Tourism | 19340.00 | 217.00 | 0.01 | 121.00 | 13.00 | 0.00 | 2025-06-30 |
| CBA | Finance & Insurance | 40764.00 | 23.00 | 0.00 | 12.00 | 6.00 | 0.00 | 2025-06-30 |
| CBA | Government Administration & Defence | 153882.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 2025-06-30 |
| CBA | Health & Community Services | 15212.00 | 361.00 | 0.02 | 115.00 | 1.00 | 0.00 | 2025-06-30 |
| CBA | Manufacturing | 16717.00 | 236.00 | 0.01 | 93.00 | 38.00 | 0.00 | 2025-06-30 |
| CBA | Mining, Oil & Gas | 3434.00 | 9.00 | 0.00 | 2.00 | 1.00 | 0.00 | 2025-06-30 |
| CBA | Other | 29667.00 | 144.00 | 0.00 | 37.00 | 23.00 | 0.00 | 2025-06-30 |
| CBA | Transport & Storage | 22708.00 | 217.00 | 0.01 | 88.00 | 9.00 | 0.00 | 2025-06-30 |
| CBA | Wholesale & Retail Trade | 29785.00 | 365.00 | 0.01 | 216.00 | 29.00 | 0.00 | 2025-06-30 |
| NAB | Accommodation and hospitality | 14704.00 | 265.00 | 0.02 | 25.00 | - | - | 2025-09-30 |
| NAB | Agriculture, forestry, fishing and mining | 69428.00 | 1809.00 | 0.03 | 132.00 | - | - | 2025-09-30 |
| NAB | Business services and property services | 23174.00 | 544.00 | 0.02 | 141.00 | - | - | 2025-09-30 |
| NAB | Commercial property | 94697.00 | 1125.00 | 0.01 | 32.00 | - | - | 2025-09-30 |
| NAB | Construction | 15140.00 | 393.00 | 0.03 | 82.00 | - | - | 2025-09-30 |
| NAB | Finance and insurance | 170647.00 | 103.00 | 0.00 | 24.00 | - | - | 2025-09-30 |
| NAB | Government and public authorities | 75817.00 | 0.00 | 0.00 | 0.00 | - | - | 2025-09-30 |
| NAB | Manufacturing | 21932.00 | 659.00 | 0.03 | 239.00 | - | - | 2025-09-30 |
| NAB | Other | 31383.00 | 326.00 | 0.01 | 62.00 | - | - | 2025-09-30 |
| NAB | Personal | 20790.00 | 176.00 | 0.01 | 2.00 | - | - | 2025-09-30 |
| NAB | Residential mortgages | 496085.00 | 5401.00 | 0.01 | 71.00 | - | - | 2025-09-30 |
| NAB | Retail and wholesale trade | 36531.00 | 647.00 | 0.02 | 168.00 | - | - | 2025-09-30 |
| NAB | Transport and storage | 22584.00 | 376.00 | 0.02 | 76.00 | - | - | 2025-09-30 |
| NAB | Utilities | 25787.00 | 270.00 | 0.01 | 109.00 | - | - | 2025-09-30 |
| WBC | Accommodation, cafes and restaurants | 12977.00 | 210.00 | 0.02 | 44.00 | 8.00 | 0.00 | 2025-09-30 |
| WBC | Agriculture, forestry and fishing | 26642.00 | 459.00 | 0.02 | 83.00 | 4.00 | 0.00 | 2025-09-30 |
| WBC | Construction | 12718.00 | 303.00 | 0.02 | 68.00 | 24.00 | 0.00 | 2025-09-30 |
| WBC | Finance and insurance | 81032.00 | 78.00 | 0.00 | 14.00 | 4.00 | 0.00 | 2025-09-30 |
| WBC | Government, administration and defence | 117150.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 2025-09-30 |
| WBC | Manufacturing | 20002.00 | 365.00 | 0.02 | 132.00 | 10.00 | 0.00 | 2025-09-30 |
| WBC | Mining | 5929.00 | 33.00 | 0.01 | 9.00 | 1.00 | 0.00 | 2025-09-30 |
| WBC | Other | 11456.00 | 40.00 | 0.00 | 12.00 | 5.00 | 0.00 | 2025-09-30 |
| WBC | Property | 86453.00 | 965.00 | 0.01 | 171.00 | 4.00 | 0.00 | 2025-09-30 |
| WBC | Property services and business services | 22400.00 | 432.00 | 0.02 | 109.00 | 15.00 | 0.00 | 2025-09-30 |
| WBC | Retail lending | 673155.00 | 6146.00 | 0.01 | 693.00 | 324.00 | 0.00 | 2025-09-30 |
| WBC | Services | 22989.00 | 362.00 | 0.02 | 154.00 | 14.00 | 0.00 | 2025-09-30 |
| WBC | Trade | 29138.00 | 582.00 | 0.02 | 161.00 | 101.00 | 0.00 | 2025-09-30 |
| WBC | Transport and storage | 17931.00 | 138.00 | 0.01 | 53.00 | 9.00 | 0.00 | 2025-09-30 |
| WBC | Utilities | 20014.00 | 14.00 | 0.00 | 3.00 | 1.00 | 0.00 | 2025-09-30 |
| ANZ | Agriculture, Forestry, Fishing & Mining | 55628.00 | 671.00 | 0.01 | 33.00 | - | - | 2025-09-30 |
| ANZ | Business & Property Services | 23532.00 | 111.00 | 0.00 | 21.00 | - | - | 2025-09-30 |
| ANZ | Commercial Property | 80758.00 | 462.00 | 0.01 | 36.00 | - | - | 2025-09-30 |
| ANZ | Construction | 13211.00 | 148.00 | 0.01 | 19.00 | - | - | 2025-09-30 |
| ANZ | Electricity, Gas & Water Supply | 23658.00 | 4.00 | 0.00 | 1.00 | - | - | 2025-09-30 |
| ANZ | Entertainment, Leisure & Tourism | 17670.00 | 151.00 | 0.01 | 16.00 | - | - | 2025-09-30 |
| ANZ | Financial, Investment & Insurance | 402472.00 | 23.00 | 0.00 | 6.00 | - | - | 2025-09-30 |
| ANZ | Government & Official Institutions | 146648.00 | 0.00 | 0.00 | 0.00 | - | - | 2025-09-30 |
| ANZ | Manufacturing | 50831.00 | 216.00 | 0.00 | 28.00 | - | - | 2025-09-30 |
| ANZ | Other | 32415.00 | 202.00 | 0.01 | 61.00 | - | - | 2025-09-30 |
| ANZ | Personal Lending | 20788.00 | 99.00 | 0.00 | 15.00 | - | - | 2025-09-30 |
| ANZ | Residential Mortgage | 554118.00 | 5987.00 | 0.01 | 57.00 | - | - | 2025-09-30 |
| ANZ | Retail Trade | 17969.00 | 219.00 | 0.01 | 83.00 | - | - | 2025-09-30 |
| ANZ | Transport & Storage | 21170.00 | 62.00 | 0.00 | 12.00 | - | - | 2025-09-30 |
| ANZ | Wholesale Trade | 25252.00 | 55.00 | 0.00 | 11.00 | - | - | 2025-09-30 |
