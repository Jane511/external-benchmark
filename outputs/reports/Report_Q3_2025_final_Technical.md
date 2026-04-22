# External Benchmark Calibration Report — Q3 2025 — Technical Appendix

_Model Risk Committee · Generated 2026-04-22T04:53:31.726840+00:00_

## 1. Executive Summary

This calibration draws on 625 external benchmark entries across 6 publishers (APRA, CBA, NAB, WBC, ANZ, ASIC+ABS) covering 34 asset-class segments. The flagship CBA CRE PD of 2.50% flows through the adjustment chain to 2.50% for bank institutions and 5.38% for private credit — the engine's canonical regression test. HIGH-severity peer divergence: WBC corporate_general lgd at 6.10× peer median.

- Segments covered: 34
- Benchmark entries: 625
- Entries by source type: {'pillar3': 321, 'apra_adi': 228, 'insolvency': 76}
- Stale sources flagged: 0
- Institution type: bank
- Period: Q3 2025

**Flagship:** raw CBA CRE PD 2.5000% -> Bank 2.5000% / PC 5.3763% (**2.15x** ratio).

## 2. Source Register
| source_id | publisher | source_type | asset_class | data_type | value | retrieval_date | quality_score |
|---|---|---|---|---|---|---|---|
| ANZ_CORPORATE_GENERAL_0P00_TO_LT0P15_LGD_LGD_2025Q3 | ANZ | pillar3 | corporate_general | lgd | 0.52 | 2026-04-22 | HIGH |
| ANZ_CORPORATE_GENERAL_0P15_TO_LT0P25_LGD_LGD_2025Q3 | ANZ | pillar3 | corporate_general | lgd | 0.5 | 2026-04-22 | HIGH |
| ANZ_CORPORATE_GENERAL_0P25_TO_LT0P50_LGD_LGD_2025Q3 | ANZ | pillar3 | corporate_general | lgd | 0.29 | 2026-04-22 | HIGH |
| ANZ_CORPORATE_GENERAL_0P50_TO_LT0P75_LGD_LGD_2025Q3 | ANZ | pillar3 | corporate_general | lgd | 0.31 | 2026-04-22 | HIGH |
| ANZ_CORPORATE_GENERAL_0P75_TO_LT2P50_LGD_LGD_2025Q3 | ANZ | pillar3 | corporate_general | lgd | 0.32 | 2026-04-22 | HIGH |
| ANZ_CORPORATE_GENERAL_0P75_TO_LT2P50_PD_PD_2025Q3 | ANZ | pillar3 | corporate_general | pd | 0.013999999999999999 | 2026-04-22 | HIGH |
| ANZ_CORPORATE_GENERAL_100P00_DEFAULT_LGD_LGD_2025Q3 | ANZ | pillar3 | corporate_general | lgd | 0.32 | 2026-04-22 | HIGH |
| ANZ_CORPORATE_GENERAL_10P00_TO_LT100P00_LGD_LGD_2025Q3 | ANZ | pillar3 | corporate_general | lgd | 0.4 | 2026-04-22 | HIGH |
| ANZ_CORPORATE_GENERAL_2P50_TO_LT10P00_LGD_LGD_2025Q3 | ANZ | pillar3 | corporate_general | lgd | 0.3 | 2026-04-22 | HIGH |
| ANZ_CORPORATE_GENERAL_2P50_TO_LT10P00_PD_PD_2025Q3 | ANZ | pillar3 | corporate_general | pd | 0.0479 | 2026-04-22 | HIGH |
| ANZ_DEVELOPMENT_DEFAULT_RISK_WEIGHT_SUPERVISORY_VALUE_2025Q3 | ANZ | pillar3 | development_default | supervisory_value | 0.0 | 2026-04-22 | HIGH |
| ANZ_DEVELOPMENT_GOOD_RISK_WEIGHT_SUPERVISORY_VALUE_2025Q3 | ANZ | pillar3 | development_good | supervisory_value | 0.9 | 2026-04-22 | HIGH |
| ANZ_DEVELOPMENT_STRONG_RISK_WEIGHT_SUPERVISORY_VALUE_2025Q3 | ANZ | pillar3 | development_strong | supervisory_value | 0.7 | 2026-04-22 | HIGH |
| ANZ_FINANCIAL_INSTITUTION_0P00_TO_LT0P15_LGD_LGD_2025Q3 | ANZ | pillar3 | financial_institution | lgd | 0.48 | 2026-04-22 | HIGH |
| ANZ_FINANCIAL_INSTITUTION_0P00_TO_LT0P15_PD_PD_2025Q3 | ANZ | pillar3 | financial_institution | pd | 0.0006 | 2026-04-22 | HIGH |
| ANZ_FINANCIAL_INSTITUTION_0P15_TO_LT0P25_LGD_LGD_2025Q3 | ANZ | pillar3 | financial_institution | lgd | 0.51 | 2026-04-22 | HIGH |
| ANZ_FINANCIAL_INSTITUTION_0P15_TO_LT0P25_PD_PD_2025Q3 | ANZ | pillar3 | financial_institution | pd | 0.002 | 2026-04-22 | HIGH |
| ANZ_FINANCIAL_INSTITUTION_0P25_TO_LT0P50_LGD_LGD_2025Q3 | ANZ | pillar3 | financial_institution | lgd | 0.48 | 2026-04-22 | HIGH |
| ANZ_FINANCIAL_INSTITUTION_0P25_TO_LT0P50_PD_PD_2025Q3 | ANZ | pillar3 | financial_institution | pd | 0.0034999999999999996 | 2026-04-22 | HIGH |
| ANZ_FINANCIAL_INSTITUTION_0P50_TO_LT0P75_LGD_LGD_2025Q3 | ANZ | pillar3 | financial_institution | lgd | 0.49 | 2026-04-22 | HIGH |
| ANZ_FINANCIAL_INSTITUTION_0P50_TO_LT0P75_PD_PD_2025Q3 | ANZ | pillar3 | financial_institution | pd | 0.0059 | 2026-04-22 | HIGH |
| ANZ_FINANCIAL_INSTITUTION_0P75_TO_LT2P50_LGD_LGD_2025Q3 | ANZ | pillar3 | financial_institution | lgd | 0.42 | 2026-04-22 | HIGH |
| ANZ_FINANCIAL_INSTITUTION_0P75_TO_LT2P50_PD_PD_2025Q3 | ANZ | pillar3 | financial_institution | pd | 0.0128 | 2026-04-22 | HIGH |
| ANZ_FINANCIAL_INSTITUTION_100P00_DEFAULT_LGD_LGD_2025Q3 | ANZ | pillar3 | financial_institution | lgd | 0.5 | 2026-04-22 | HIGH |
| ANZ_FINANCIAL_INSTITUTION_100P00_DEFAULT_PD_PD_2025Q3 | ANZ | pillar3 | financial_institution | pd | 1.0 | 2026-04-22 | HIGH |
| ANZ_FINANCIAL_INSTITUTION_10P00_TO_LT100P00_LGD_LGD_2025Q3 | ANZ | pillar3 | financial_institution | lgd | 0.48 | 2026-04-22 | HIGH |
| ANZ_FINANCIAL_INSTITUTION_10P00_TO_LT100P00_PD_PD_2025Q3 | ANZ | pillar3 | financial_institution | pd | 0.3486 | 2026-04-22 | HIGH |
| ANZ_FINANCIAL_INSTITUTION_2P50_TO_LT10P00_LGD_LGD_2025Q3 | ANZ | pillar3 | financial_institution | lgd | 0.41 | 2026-04-22 | HIGH |
| ANZ_FINANCIAL_INSTITUTION_2P50_TO_LT10P00_PD_PD_2025Q3 | ANZ | pillar3 | financial_institution | pd | 0.0409 | 2026-04-22 | HIGH |
| ANZ_RESIDENTIAL_MORTGAGE_0P00_TO_LT0P15_LGD_LGD_2025Q3 | ANZ | pillar3 | residential_mortgage | lgd | 0.16 | 2026-04-22 | HIGH |
| ANZ_RESIDENTIAL_MORTGAGE_0P15_TO_LT0P25_LGD_LGD_2025Q3 | ANZ | pillar3 | residential_mortgage | lgd | 0.17 | 2026-04-22 | HIGH |
| ANZ_RESIDENTIAL_MORTGAGE_0P25_TO_LT0P50_LGD_LGD_2025Q3 | ANZ | pillar3 | residential_mortgage | lgd | 0.18 | 2026-04-22 | HIGH |
| ANZ_RESIDENTIAL_MORTGAGE_0P25_TO_LT0P50_PD_PD_2025Q3 | ANZ | pillar3 | residential_mortgage | pd | 0.0037 | 2026-04-22 | HIGH |
| ANZ_RESIDENTIAL_MORTGAGE_0P50_TO_LT0P75_LGD_LGD_2025Q3 | ANZ | pillar3 | residential_mortgage | lgd | 0.19 | 2026-04-22 | HIGH |
| ANZ_RESIDENTIAL_MORTGAGE_0P50_TO_LT0P75_PD_PD_2025Q3 | ANZ | pillar3 | residential_mortgage | pd | 0.0066 | 2026-04-22 | HIGH |
| ANZ_RESIDENTIAL_MORTGAGE_0P75_TO_LT2P50_LGD_LGD_2025Q3 | ANZ | pillar3 | residential_mortgage | lgd | 0.2 | 2026-04-22 | HIGH |
| ANZ_RESIDENTIAL_MORTGAGE_0P75_TO_LT2P50_PD_PD_2025Q3 | ANZ | pillar3 | residential_mortgage | pd | 0.0137 | 2026-04-22 | HIGH |
| ANZ_RESIDENTIAL_MORTGAGE_100P00_DEFAULT_LGD_LGD_2025Q3 | ANZ | pillar3 | residential_mortgage | lgd | 0.2 | 2026-04-22 | HIGH |
| ANZ_RESIDENTIAL_MORTGAGE_10P00_TO_LT100P00_LGD_LGD_2025Q3 | ANZ | pillar3 | residential_mortgage | lgd | 0.2 | 2026-04-22 | HIGH |
| ANZ_RESIDENTIAL_MORTGAGE_2P50_TO_LT10P00_LGD_LGD_2025Q3 | ANZ | pillar3 | residential_mortgage | lgd | 0.21 | 2026-04-22 | HIGH |
| ANZ_RETAIL_OTHER_0P00_TO_LT0P15_LGD_LGD_2025Q3 | ANZ | pillar3 | retail_other | lgd | 0.77 | 2026-04-22 | HIGH |
| ANZ_RETAIL_OTHER_0P00_TO_LT0P15_PD_PD_2025Q3 | ANZ | pillar3 | retail_other | pd | 0.0011 | 2026-04-22 | HIGH |
| ANZ_RETAIL_OTHER_0P15_TO_LT0P25_LGD_LGD_2025Q3 | ANZ | pillar3 | retail_other | lgd | 0.78 | 2026-04-22 | HIGH |
| ANZ_RETAIL_OTHER_0P15_TO_LT0P25_PD_PD_2025Q3 | ANZ | pillar3 | retail_other | pd | 0.0019 | 2026-04-22 | HIGH |
| ANZ_RETAIL_OTHER_0P25_TO_LT0P50_LGD_LGD_2025Q3 | ANZ | pillar3 | retail_other | lgd | 0.78 | 2026-04-22 | HIGH |
| ANZ_RETAIL_OTHER_0P25_TO_LT0P50_PD_PD_2025Q3 | ANZ | pillar3 | retail_other | pd | 0.0034000000000000002 | 2026-04-22 | HIGH |
| ANZ_RETAIL_OTHER_0P50_TO_LT0P75_LGD_LGD_2025Q3 | ANZ | pillar3 | retail_other | lgd | 0.81 | 2026-04-22 | HIGH |
| ANZ_RETAIL_OTHER_0P50_TO_LT0P75_PD_PD_2025Q3 | ANZ | pillar3 | retail_other | pd | 0.0062 | 2026-04-22 | HIGH |
| ANZ_RETAIL_OTHER_0P75_TO_LT2P50_LGD_LGD_2025Q3 | ANZ | pillar3 | retail_other | lgd | 0.78 | 2026-04-22 | HIGH |
| ANZ_RETAIL_OTHER_0P75_TO_LT2P50_PD_PD_2025Q3 | ANZ | pillar3 | retail_other | pd | 0.0128 | 2026-04-22 | HIGH |
| ANZ_RETAIL_OTHER_100P00_DEFAULT_LGD_LGD_2025Q3 | ANZ | pillar3 | retail_other | lgd | 0.81 | 2026-04-22 | HIGH |
| ANZ_RETAIL_OTHER_100P00_DEFAULT_PD_PD_2025Q3 | ANZ | pillar3 | retail_other | pd | 1.0 | 2026-04-22 | HIGH |
| ANZ_RETAIL_OTHER_10P00_TO_LT100P00_LGD_LGD_2025Q3 | ANZ | pillar3 | retail_other | lgd | 0.86 | 2026-04-22 | HIGH |
| ANZ_RETAIL_OTHER_10P00_TO_LT100P00_PD_PD_2025Q3 | ANZ | pillar3 | retail_other | pd | 0.18460000000000001 | 2026-04-22 | HIGH |
| ANZ_RETAIL_OTHER_2P50_TO_LT10P00_LGD_LGD_2025Q3 | ANZ | pillar3 | retail_other | lgd | 0.86 | 2026-04-22 | HIGH |
| ANZ_RETAIL_OTHER_2P50_TO_LT10P00_PD_PD_2025Q3 | ANZ | pillar3 | retail_other | pd | 0.045899999999999996 | 2026-04-22 | HIGH |
| ANZ_RETAIL_QRR_0P00_TO_LT0P15_LGD_LGD_2025Q3 | ANZ | pillar3 | retail_qrr | lgd | 0.74 | 2026-04-22 | HIGH |
| ANZ_RETAIL_QRR_0P00_TO_LT0P15_PD_PD_2025Q3 | ANZ | pillar3 | retail_qrr | pd | 0.0011 | 2026-04-22 | HIGH |
| ANZ_RETAIL_QRR_0P15_TO_LT0P25_LGD_LGD_2025Q3 | ANZ | pillar3 | retail_qrr | lgd | 0.74 | 2026-04-22 | HIGH |
| ANZ_RETAIL_QRR_0P15_TO_LT0P25_PD_PD_2025Q3 | ANZ | pillar3 | retail_qrr | pd | 0.0019 | 2026-04-22 | HIGH |
| ANZ_RETAIL_QRR_0P25_TO_LT0P50_LGD_LGD_2025Q3 | ANZ | pillar3 | retail_qrr | lgd | 0.75 | 2026-04-22 | HIGH |
| ANZ_RETAIL_QRR_0P25_TO_LT0P50_PD_PD_2025Q3 | ANZ | pillar3 | retail_qrr | pd | 0.0036 | 2026-04-22 | HIGH |
| ANZ_RETAIL_QRR_0P50_TO_LT0P75_LGD_LGD_2025Q3 | ANZ | pillar3 | retail_qrr | lgd | 0.74 | 2026-04-22 | HIGH |
| ANZ_RETAIL_QRR_0P50_TO_LT0P75_PD_PD_2025Q3 | ANZ | pillar3 | retail_qrr | pd | 0.006500000000000001 | 2026-04-22 | HIGH |
| ANZ_RETAIL_QRR_0P75_TO_LT2P50_LGD_LGD_2025Q3 | ANZ | pillar3 | retail_qrr | lgd | 0.79 | 2026-04-22 | HIGH |
| ANZ_RETAIL_QRR_0P75_TO_LT2P50_PD_PD_2025Q3 | ANZ | pillar3 | retail_qrr | pd | 0.013500000000000002 | 2026-04-22 | HIGH |
| ANZ_RETAIL_QRR_100P00_DEFAULT_LGD_LGD_2025Q3 | ANZ | pillar3 | retail_qrr | lgd | 0.76 | 2026-04-22 | HIGH |
| ANZ_RETAIL_QRR_100P00_DEFAULT_PD_PD_2025Q3 | ANZ | pillar3 | retail_qrr | pd | 1.0 | 2026-04-22 | HIGH |
| ANZ_RETAIL_QRR_10P00_TO_LT100P00_LGD_LGD_2025Q3 | ANZ | pillar3 | retail_qrr | lgd | 0.81 | 2026-04-22 | HIGH |
| ANZ_RETAIL_QRR_10P00_TO_LT100P00_PD_PD_2025Q3 | ANZ | pillar3 | retail_qrr | pd | 0.1977 | 2026-04-22 | HIGH |
| ANZ_RETAIL_QRR_2P50_TO_LT10P00_LGD_LGD_2025Q3 | ANZ | pillar3 | retail_qrr | lgd | 0.82 | 2026-04-22 | HIGH |
| ANZ_RETAIL_QRR_2P50_TO_LT10P00_PD_PD_2025Q3 | ANZ | pillar3 | retail_qrr | pd | 0.0407 | 2026-04-22 | HIGH |
| ANZ_RETAIL_SME_0P00_TO_LT0P15_LGD_LGD_2025Q3 | ANZ | pillar3 | retail_sme | lgd | 0.14 | 2026-04-22 | HIGH |
| ANZ_RETAIL_SME_0P15_TO_LT0P25_LGD_LGD_2025Q3 | ANZ | pillar3 | retail_sme | lgd | 0.17 | 2026-04-22 | HIGH |
| ANZ_RETAIL_SME_0P25_TO_LT0P50_LGD_LGD_2025Q3 | ANZ | pillar3 | retail_sme | lgd | 0.27 | 2026-04-22 | HIGH |
| ANZ_RETAIL_SME_0P50_TO_LT0P75_LGD_LGD_2025Q3 | ANZ | pillar3 | retail_sme | lgd | 0.38 | 2026-04-22 | HIGH |
| ANZ_RETAIL_SME_0P75_TO_LT2P50_LGD_LGD_2025Q3 | ANZ | pillar3 | retail_sme | lgd | 0.26 | 2026-04-22 | HIGH |
| ANZ_RETAIL_SME_0P75_TO_LT2P50_PD_PD_2025Q3 | ANZ | pillar3 | retail_sme | pd | 0.016 | 2026-04-22 | HIGH |
| ANZ_RETAIL_SME_100P00_DEFAULT_LGD_LGD_2025Q3 | ANZ | pillar3 | retail_sme | lgd | 0.4 | 2026-04-22 | HIGH |
| ANZ_RETAIL_SME_10P00_TO_LT100P00_LGD_LGD_2025Q3 | ANZ | pillar3 | retail_sme | lgd | 0.5 | 2026-04-22 | HIGH |
| ANZ_RETAIL_SME_2P50_TO_LT10P00_LGD_LGD_2025Q3 | ANZ | pillar3 | retail_sme | lgd | 0.29 | 2026-04-22 | HIGH |
| ANZ_RETAIL_SME_2P50_TO_LT10P00_PD_PD_2025Q3 | ANZ | pillar3 | retail_sme | pd | 0.044199999999999996 | 2026-04-22 | HIGH |
| ANZ_SOVEREIGN_0P00_TO_LT0P15_LGD_LGD_2025Q3 | ANZ | pillar3 | sovereign | lgd | 0.09 | 2026-04-22 | HIGH |
| ANZ_SOVEREIGN_0P00_TO_LT0P15_PD_PD_2025Q3 | ANZ | pillar3 | sovereign | pd | 0.0002 | 2026-04-22 | HIGH |
| ANZ_SOVEREIGN_0P15_TO_LT0P25_LGD_LGD_2025Q3 | ANZ | pillar3 | sovereign | lgd | 0.5 | 2026-04-22 | HIGH |
| ANZ_SOVEREIGN_0P15_TO_LT0P25_PD_PD_2025Q3 | ANZ | pillar3 | sovereign | pd | 0.002 | 2026-04-22 | HIGH |
| ANZ_SOVEREIGN_0P25_TO_LT0P50_LGD_LGD_2025Q3 | ANZ | pillar3 | sovereign | lgd | 0.5 | 2026-04-22 | HIGH |
| ANZ_SOVEREIGN_0P25_TO_LT0P50_PD_PD_2025Q3 | ANZ | pillar3 | sovereign | pd | 0.0027 | 2026-04-22 | HIGH |
| ANZ_SOVEREIGN_0P50_TO_LT0P75_LGD_LGD_2025Q3 | ANZ | pillar3 | sovereign | lgd | 0.5 | 2026-04-22 | HIGH |
| ANZ_SOVEREIGN_0P50_TO_LT0P75_PD_PD_2025Q3 | ANZ | pillar3 | sovereign | pd | 0.0058 | 2026-04-22 | HIGH |
| ANZ_SOVEREIGN_0P75_TO_LT2P50_LGD_LGD_2025Q3 | ANZ | pillar3 | sovereign | lgd | 0.5 | 2026-04-22 | HIGH |
| ANZ_SOVEREIGN_0P75_TO_LT2P50_PD_PD_2025Q3 | ANZ | pillar3 | sovereign | pd | 0.0132 | 2026-04-22 | HIGH |
| ANZ_SOVEREIGN_100P00_DEFAULT_LGD_LGD_2025Q3 | ANZ | pillar3 | sovereign | lgd | 0.5 | 2026-04-22 | HIGH |
| ANZ_SOVEREIGN_100P00_DEFAULT_PD_PD_2025Q3 | ANZ | pillar3 | sovereign | pd | 1.0 | 2026-04-22 | HIGH |
| ANZ_SOVEREIGN_10P00_TO_LT100P00_LGD_LGD_2025Q3 | ANZ | pillar3 | sovereign | lgd | 0.5 | 2026-04-22 | HIGH |
| ANZ_SOVEREIGN_10P00_TO_LT100P00_PD_PD_2025Q3 | ANZ | pillar3 | sovereign | pd | 0.2391 | 2026-04-22 | HIGH |
| ANZ_SOVEREIGN_2P50_TO_LT10P00_LGD_LGD_2025Q3 | ANZ | pillar3 | sovereign | lgd | 0.5 | 2026-04-22 | HIGH |
| ANZ_SOVEREIGN_2P50_TO_LT10P00_PD_PD_2025Q3 | ANZ | pillar3 | sovereign | pd | 0.05 | 2026-04-22 | HIGH |
| APRA_ADI_SECTOR_TOTAL_ALL_ADIS_NINETY_DPD_RATE_IMPAIRED_RATIO_2022Q1 | APRA | apra_adi | adi_sector_total | impaired_ratio | 0.006 | 2026-04-21 | HIGH |
| APRA_ADI_SECTOR_TOTAL_ALL_ADIS_NINETY_DPD_RATE_IMPAIRED_RATIO_2022Q2 | APRA | apra_adi | adi_sector_total | impaired_ratio | 0.005 | 2026-04-21 | HIGH |
## 3. Adjustment Audit Trail
### corporate_sme
| source_id | name | multiplier | rationale |
|---|---|---|---|
| CBA_CORPORATE_SME_0P75_TO_LT2P50_PD_PD_FY2025 | peer_mix | 1.0 | Peer composition mix vs benchmark basket |
| CBA_CORPORATE_SME_2P50_TO_LT10P00_PD_PD_FY2025 | peer_mix | 1.0 | Peer composition mix vs benchmark basket |
### residential_mortgage
| source_id | name | multiplier | rationale |
|---|---|---|---|
| ANZ_RESIDENTIAL_MORTGAGE_0P25_TO_LT0P50_PD_PD_2025Q3 | peer_mix | 1.0 | Peer composition mix vs benchmark basket |
| ANZ_RESIDENTIAL_MORTGAGE_0P50_TO_LT0P75_PD_PD_2025Q3 | peer_mix | 1.0 | Peer composition mix vs benchmark basket |
| ANZ_RESIDENTIAL_MORTGAGE_0P75_TO_LT2P50_PD_PD_2025Q3 | peer_mix | 1.0 | Peer composition mix vs benchmark basket |
| CBA_RESIDENTIAL_MORTGAGE_0P25_TO_LT0P50_PD_PD_FY2025 | peer_mix | 1.0 | Peer composition mix vs benchmark basket |
| CBA_RESIDENTIAL_MORTGAGE_0P50_TO_LT0P75_PD_PD_FY2025 | peer_mix | 1.0 | Peer composition mix vs benchmark basket |
| CBA_RESIDENTIAL_MORTGAGE_0P75_TO_LT2P50_PD_PD_FY2025 | peer_mix | 1.0 | Peer composition mix vs benchmark basket |
| WBC_RESIDENTIAL_MORTGAGE_0P75_TO_LT2P50_PD_PD_2025Q3 | peer_mix | 1.0 | Peer composition mix vs benchmark basket |
## 4. Triangulated Values

Triangulation synthesises multiple sources into a single benchmark per asset-class × data-type × period using peer-median across the Big 4 cohort. Outliers exceeding 3× the median are excluded and surfaced in Section 8.

| segment | benchmark_value | method | source_count | confidence_n |
|---|---|---|---|---|
| corporate_sme | 0.02635 | weighted_by_years | 2 | 40 |
| residential_mortgage | 0.008328571428571428 | weighted_by_years | 7 | 140 |
## 5. Calibration Outputs

Each segment's triangulated value is run through five calibration methods: central tendency, logistic recalibration, Bayesian blending, external blending, and Pluto-Tasche (comparison only). Methods converge when the cohort is large; they diverge when confidence_n is small.

### corporate_sme
| method | value | floor_triggered | extras |
|---|---|---|---|
| central_tendency | 0.02635 | False |  |
| logistic_recalibration | 0.02635 | False | confidence_n=40 |
| bayesian_blending | 0.02635 | False | confidence_n=40 |
| external_blending | 0.02635 | False | internal_weight=0.9 |
| pluto_tasche | 0.02635 | False | role=comparison_only |
### residential_mortgage
| method | value | floor_triggered | extras |
|---|---|---|---|
| central_tendency | 0.008328571428571428 | False |  |
| logistic_recalibration | 0.008328571428571428 | False | confidence_n=140 |
| bayesian_blending | 0.008328571428571428 | False | confidence_n=140 |
| external_blending | 0.008328571428571428 | False | internal_weight=0.9 |
| pluto_tasche | 0.008328571428571428 | False | role=comparison_only |
## 6. Downturn LGD

Downturn LGD values are raw component-level benchmarks, not run through the PD adjustment chain. No regulatory floor applies. Values are sourced from Pillar 3 A-IRB disclosures and represent exposure-weighted long-run averages with a product-specific downturn uplift applied.

| product | long_run_lgd | uplift | downturn_lgd | lgd_for_capital | lgd_for_ecl |
|---|---|---|---|---|---|
| residential_property | 0.25 | 1.45 | 0.3625 | 0.3625 | 0.25 |
| commercial_property | 0.25 | 1.75 | 0.4375 | 0.4375 | 0.25 |
| development | 0.25 | 2.0 | 0.5 | 0.5 | 0.25 |
| corporate_sme_secured | 0.25 | 1.5 | 0.375 | 0.375 | 0.25 |
| corporate_sme_unsecured | 0.25 | 1.6 | 0.4 | 0.4 | 0.25 |
| trade_finance | 0.25 | 1.4 | 0.35 | 0.35 | 0.25 |
## 7. Bank vs Private Credit Comparison

The same raw CBA CRE PD (2.50%) produces 2.50% for a bank-profile institution and 5.38% for private credit. The 2.15× spread reflects selection bias, higher LVR, and shorter sponsor trading history in the PC cohort — all calibrated per MRC-approved multiplier ranges documented in Section 3.

| chain | adjusted_value | steps | final_multiplier |
|---|---|---|---|
| Bank | 0.025 | 1 | 1.0 |
| Private Credit | 0.053762500000000005 | 3 | 2.1505 |

**PC / Bank ratio: 2.15x**

## 8. Data Governance

Governance reports run automatically against every benchmark at each calibration cycle. Flags are grouped by rule and dimension below; detailed findings are in the technical appendix.

| report_type | flag_count | finding_count |
|---|---|---|
| stale | 0 | 625 |
| quality | 76 | 625 |
| coverage | 0 | 2 |
| pillar3_divergence | 68 | 318 |

**Data governance flags (grouped):**

- **low_quality · ASIC_ABS · frequency** — 76 findings across 19 ANZSIC industry divisions, FY2022Q2–FY2025Q2. Quarterly frequency-dimension flags on annual-cadence failure-rate data; expected behaviour of the quality rule, not a data issue.
- **pillar3_divergence · ANZ · vs peer median** — 24 findings. Sources where a bank's value diverges materially from the Big 4 peer median. See detailed table in Technical Appendix §8.
- **pillar3_divergence · CBA · vs peer median** — 26 findings. Sources where a bank's value diverges materially from the Big 4 peer median. See detailed table in Technical Appendix §8.
- **pillar3_divergence · WBC · vs peer median** — 18 findings. Sources where a bank's value diverges materially from the Big 4 peer median. See detailed table in Technical Appendix §8.

## 9. Version History

This calibration is reproducible from source_id + version. Prior versions are preserved via immutable supersession; a point-in-time re-run of any previous period will reproduce the exact values used at that cycle.

_No prior period registry available for comparison._
| source_id | current_version | latest_value | value_date | superseded_by |
|---|---|---|---|---|
| ANZ_CORPORATE_GENERAL_0P00_TO_LT0P15_LGD_LGD_2025Q3 | 6 | 0.52 | 2025-09-30 | (current) |
| ANZ_CORPORATE_GENERAL_0P15_TO_LT0P25_LGD_LGD_2025Q3 | 6 | 0.5 | 2025-09-30 | (current) |
| ANZ_CORPORATE_GENERAL_0P25_TO_LT0P50_LGD_LGD_2025Q3 | 6 | 0.29 | 2025-09-30 | (current) |
| ANZ_CORPORATE_GENERAL_0P50_TO_LT0P75_LGD_LGD_2025Q3 | 6 | 0.31 | 2025-09-30 | (current) |
| ANZ_CORPORATE_GENERAL_0P75_TO_LT2P50_LGD_LGD_2025Q3 | 6 | 0.32 | 2025-09-30 | (current) |
| ANZ_CORPORATE_GENERAL_0P75_TO_LT2P50_PD_PD_2025Q3 | 6 | 0.013999999999999999 | 2025-09-30 | (current) |
| ANZ_CORPORATE_GENERAL_100P00_DEFAULT_LGD_LGD_2025Q3 | 6 | 0.32 | 2025-09-30 | (current) |
| ANZ_CORPORATE_GENERAL_10P00_TO_LT100P00_LGD_LGD_2025Q3 | 6 | 0.4 | 2025-09-30 | (current) |
| ANZ_CORPORATE_GENERAL_2P50_TO_LT10P00_LGD_LGD_2025Q3 | 6 | 0.3 | 2025-09-30 | (current) |
| ANZ_CORPORATE_GENERAL_2P50_TO_LT10P00_PD_PD_2025Q3 | 6 | 0.0479 | 2025-09-30 | (current) |
| ANZ_DEVELOPMENT_DEFAULT_RISK_WEIGHT_SUPERVISORY_VALUE_2025Q3 | 1 | 0.0 | 2025-09-30 | (current) |
| ANZ_DEVELOPMENT_GOOD_RISK_WEIGHT_SUPERVISORY_VALUE_2025Q3 | 1 | 0.9 | 2025-09-30 | (current) |
| ANZ_DEVELOPMENT_STRONG_RISK_WEIGHT_SUPERVISORY_VALUE_2025Q3 | 1 | 0.7 | 2025-09-30 | (current) |
| ANZ_FINANCIAL_INSTITUTION_0P00_TO_LT0P15_LGD_LGD_2025Q3 | 1 | 0.48 | 2025-09-30 | (current) |
| ANZ_FINANCIAL_INSTITUTION_0P00_TO_LT0P15_PD_PD_2025Q3 | 1 | 0.0006 | 2025-09-30 | (current) |
| ANZ_FINANCIAL_INSTITUTION_0P15_TO_LT0P25_LGD_LGD_2025Q3 | 2 | 0.51 | 2025-09-30 | (current) |
| ANZ_FINANCIAL_INSTITUTION_0P15_TO_LT0P25_PD_PD_2025Q3 | 1 | 0.002 | 2025-09-30 | (current) |
| ANZ_FINANCIAL_INSTITUTION_0P25_TO_LT0P50_LGD_LGD_2025Q3 | 2 | 0.48 | 2025-09-30 | (current) |
| ANZ_FINANCIAL_INSTITUTION_0P25_TO_LT0P50_PD_PD_2025Q3 | 2 | 0.0034999999999999996 | 2025-09-30 | (current) |
| ANZ_FINANCIAL_INSTITUTION_0P50_TO_LT0P75_LGD_LGD_2025Q3 | 2 | 0.49 | 2025-09-30 | (current) |
| ANZ_FINANCIAL_INSTITUTION_0P50_TO_LT0P75_PD_PD_2025Q3 | 2 | 0.0059 | 2025-09-30 | (current) |
| ANZ_FINANCIAL_INSTITUTION_0P75_TO_LT2P50_LGD_LGD_2025Q3 | 2 | 0.42 | 2025-09-30 | (current) |
| ANZ_FINANCIAL_INSTITUTION_0P75_TO_LT2P50_PD_PD_2025Q3 | 2 | 0.0128 | 2025-09-30 | (current) |
| ANZ_FINANCIAL_INSTITUTION_100P00_DEFAULT_LGD_LGD_2025Q3 | 1 | 0.5 | 2025-09-30 | (current) |
| ANZ_FINANCIAL_INSTITUTION_100P00_DEFAULT_PD_PD_2025Q3 | 1 | 1.0 | 2025-09-30 | (current) |
| ANZ_FINANCIAL_INSTITUTION_10P00_TO_LT100P00_LGD_LGD_2025Q3 | 2 | 0.48 | 2025-09-30 | (current) |
| ANZ_FINANCIAL_INSTITUTION_10P00_TO_LT100P00_PD_PD_2025Q3 | 2 | 0.3486 | 2025-09-30 | (current) |
| ANZ_FINANCIAL_INSTITUTION_2P50_TO_LT10P00_LGD_LGD_2025Q3 | 2 | 0.41 | 2025-09-30 | (current) |
| ANZ_FINANCIAL_INSTITUTION_2P50_TO_LT10P00_PD_PD_2025Q3 | 2 | 0.0409 | 2025-09-30 | (current) |
| ANZ_RESIDENTIAL_MORTGAGE_0P00_TO_LT0P15_LGD_LGD_2025Q3 | 4 | 0.16 | 2025-09-30 | (current) |
| ANZ_RESIDENTIAL_MORTGAGE_0P15_TO_LT0P25_LGD_LGD_2025Q3 | 4 | 0.17 | 2025-09-30 | (current) |
| ANZ_RESIDENTIAL_MORTGAGE_0P25_TO_LT0P50_LGD_LGD_2025Q3 | 4 | 0.18 | 2025-09-30 | (current) |
| ANZ_RESIDENTIAL_MORTGAGE_0P25_TO_LT0P50_PD_PD_2025Q3 | 4 | 0.0037 | 2025-09-30 | (current) |
| ANZ_RESIDENTIAL_MORTGAGE_0P50_TO_LT0P75_LGD_LGD_2025Q3 | 4 | 0.19 | 2025-09-30 | (current) |
| ANZ_RESIDENTIAL_MORTGAGE_0P50_TO_LT0P75_PD_PD_2025Q3 | 4 | 0.0066 | 2025-09-30 | (current) |
| ANZ_RESIDENTIAL_MORTGAGE_0P75_TO_LT2P50_LGD_LGD_2025Q3 | 4 | 0.2 | 2025-09-30 | (current) |
| ANZ_RESIDENTIAL_MORTGAGE_0P75_TO_LT2P50_PD_PD_2025Q3 | 4 | 0.0137 | 2025-09-30 | (current) |
| ANZ_RESIDENTIAL_MORTGAGE_100P00_DEFAULT_LGD_LGD_2025Q3 | 4 | 0.2 | 2025-09-30 | (current) |
| ANZ_RESIDENTIAL_MORTGAGE_10P00_TO_LT100P00_LGD_LGD_2025Q3 | 4 | 0.2 | 2025-09-30 | (current) |
| ANZ_RESIDENTIAL_MORTGAGE_2P50_TO_LT10P00_LGD_LGD_2025Q3 | 4 | 0.21 | 2025-09-30 | (current) |
| ANZ_RETAIL_OTHER_0P00_TO_LT0P15_LGD_LGD_2025Q3 | 4 | 0.77 | 2025-09-30 | (current) |
| ANZ_RETAIL_OTHER_0P00_TO_LT0P15_PD_PD_2025Q3 | 4 | 0.0011 | 2025-09-30 | (current) |
| ANZ_RETAIL_OTHER_0P15_TO_LT0P25_LGD_LGD_2025Q3 | 4 | 0.78 | 2025-09-30 | (current) |
| ANZ_RETAIL_OTHER_0P15_TO_LT0P25_PD_PD_2025Q3 | 1 | 0.0019 | 2025-09-30 | (current) |
| ANZ_RETAIL_OTHER_0P25_TO_LT0P50_LGD_LGD_2025Q3 | 4 | 0.78 | 2025-09-30 | (current) |
| ANZ_RETAIL_OTHER_0P25_TO_LT0P50_PD_PD_2025Q3 | 4 | 0.0034000000000000002 | 2025-09-30 | (current) |
| ANZ_RETAIL_OTHER_0P50_TO_LT0P75_LGD_LGD_2025Q3 | 4 | 0.81 | 2025-09-30 | (current) |
| ANZ_RETAIL_OTHER_0P50_TO_LT0P75_PD_PD_2025Q3 | 4 | 0.0062 | 2025-09-30 | (current) |
| ANZ_RETAIL_OTHER_0P75_TO_LT2P50_LGD_LGD_2025Q3 | 4 | 0.78 | 2025-09-30 | (current) |
| ANZ_RETAIL_OTHER_0P75_TO_LT2P50_PD_PD_2025Q3 | 4 | 0.0128 | 2025-09-30 | (current) |
| ANZ_RETAIL_OTHER_100P00_DEFAULT_LGD_LGD_2025Q3 | 3 | 0.81 | 2025-09-30 | (current) |
| ANZ_RETAIL_OTHER_100P00_DEFAULT_PD_PD_2025Q3 | 1 | 1.0 | 2025-09-30 | (current) |
| ANZ_RETAIL_OTHER_10P00_TO_LT100P00_LGD_LGD_2025Q3 | 4 | 0.86 | 2025-09-30 | (current) |
| ANZ_RETAIL_OTHER_10P00_TO_LT100P00_PD_PD_2025Q3 | 4 | 0.18460000000000001 | 2025-09-30 | (current) |
| ANZ_RETAIL_OTHER_2P50_TO_LT10P00_LGD_LGD_2025Q3 | 4 | 0.86 | 2025-09-30 | (current) |
| ANZ_RETAIL_OTHER_2P50_TO_LT10P00_PD_PD_2025Q3 | 4 | 0.045899999999999996 | 2025-09-30 | (current) |
| ANZ_RETAIL_QRR_0P00_TO_LT0P15_LGD_LGD_2025Q3 | 1 | 0.74 | 2025-09-30 | (current) |
| ANZ_RETAIL_QRR_0P00_TO_LT0P15_PD_PD_2025Q3 | 1 | 0.0011 | 2025-09-30 | (current) |
| ANZ_RETAIL_QRR_0P15_TO_LT0P25_LGD_LGD_2025Q3 | 1 | 0.74 | 2025-09-30 | (current) |
| ANZ_RETAIL_QRR_0P15_TO_LT0P25_PD_PD_2025Q3 | 1 | 0.0019 | 2025-09-30 | (current) |
| ANZ_RETAIL_QRR_0P25_TO_LT0P50_LGD_LGD_2025Q3 | 1 | 0.75 | 2025-09-30 | (current) |
| ANZ_RETAIL_QRR_0P25_TO_LT0P50_PD_PD_2025Q3 | 1 | 0.0036 | 2025-09-30 | (current) |
| ANZ_RETAIL_QRR_0P50_TO_LT0P75_LGD_LGD_2025Q3 | 1 | 0.74 | 2025-09-30 | (current) |
| ANZ_RETAIL_QRR_0P50_TO_LT0P75_PD_PD_2025Q3 | 1 | 0.006500000000000001 | 2025-09-30 | (current) |
| ANZ_RETAIL_QRR_0P75_TO_LT2P50_LGD_LGD_2025Q3 | 1 | 0.79 | 2025-09-30 | (current) |
| ANZ_RETAIL_QRR_0P75_TO_LT2P50_PD_PD_2025Q3 | 2 | 0.013500000000000002 | 2025-09-30 | (current) |
| ANZ_RETAIL_QRR_100P00_DEFAULT_LGD_LGD_2025Q3 | 1 | 0.76 | 2025-09-30 | (current) |
| ANZ_RETAIL_QRR_100P00_DEFAULT_PD_PD_2025Q3 | 1 | 1.0 | 2025-09-30 | (current) |
| ANZ_RETAIL_QRR_10P00_TO_LT100P00_LGD_LGD_2025Q3 | 1 | 0.81 | 2025-09-30 | (current) |
| ANZ_RETAIL_QRR_10P00_TO_LT100P00_PD_PD_2025Q3 | 2 | 0.1977 | 2025-09-30 | (current) |
| ANZ_RETAIL_QRR_2P50_TO_LT10P00_LGD_LGD_2025Q3 | 1 | 0.82 | 2025-09-30 | (current) |
| ANZ_RETAIL_QRR_2P50_TO_LT10P00_PD_PD_2025Q3 | 1 | 0.0407 | 2025-09-30 | (current) |
| ANZ_RETAIL_SME_0P00_TO_LT0P15_LGD_LGD_2025Q3 | 2 | 0.14 | 2025-09-30 | (current) |
| ANZ_RETAIL_SME_0P15_TO_LT0P25_LGD_LGD_2025Q3 | 2 | 0.17 | 2025-09-30 | (current) |
| ANZ_RETAIL_SME_0P25_TO_LT0P50_LGD_LGD_2025Q3 | 1 | 0.27 | 2025-09-30 | (current) |
| ANZ_RETAIL_SME_0P50_TO_LT0P75_LGD_LGD_2025Q3 | 1 | 0.38 | 2025-09-30 | (current) |
| ANZ_RETAIL_SME_0P75_TO_LT2P50_LGD_LGD_2025Q3 | 1 | 0.26 | 2025-09-30 | (current) |
| ANZ_RETAIL_SME_0P75_TO_LT2P50_PD_PD_2025Q3 | 1 | 0.016 | 2025-09-30 | (current) |
| ANZ_RETAIL_SME_100P00_DEFAULT_LGD_LGD_2025Q3 | 2 | 0.4 | 2025-09-30 | (current) |
| ANZ_RETAIL_SME_10P00_TO_LT100P00_LGD_LGD_2025Q3 | 2 | 0.5 | 2025-09-30 | (current) |
## 10. Source Documentation

Provenance for every publisher contributing to the registry, including download cadence, extraction method, and known gaps.

| source_id | publisher | url | retrieval_date | quality_score | notes |
|---|---|---|---|---|---|
| ANZ_CORPORATE_GENERAL_0P00_TO_LT0P15_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=0.00 to <0.15; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_CORPORATE_GENERAL_0P15_TO_LT0P25_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=0.15 to <0.25; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_CORPORATE_GENERAL_0P25_TO_LT0P50_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=0.25 to <0.50; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_CORPORATE_GENERAL_0P50_TO_LT0P75_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=0.50 to <0.75; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_CORPORATE_GENERAL_0P75_TO_LT2P50_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=0.75 to <2.50; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_CORPORATE_GENERAL_0P75_TO_LT2P50_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=0.75 to <2.50; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_CORPORATE_GENERAL_100P00_DEFAULT_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=100.00 (Default); value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_CORPORATE_GENERAL_10P00_TO_LT100P00_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=10.00 to <100.00; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_CORPORATE_GENERAL_2P50_TO_LT10P00_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=2.50 to <10.00; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_CORPORATE_GENERAL_2P50_TO_LT10P00_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=2.50 to <10.00; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_DEVELOPMENT_DEFAULT_RISK_WEIGHT_SUPERVISORY_VALUE_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR10; source_page=3; pd_band=all; value_basis=supervisory_prescribed; adapter=AnzPillar3PdfAdapter |
| ANZ_DEVELOPMENT_GOOD_RISK_WEIGHT_SUPERVISORY_VALUE_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR10; source_page=70; pd_band=all; value_basis=supervisory_prescribed; adapter=AnzPillar3PdfAdapter |
| ANZ_DEVELOPMENT_STRONG_RISK_WEIGHT_SUPERVISORY_VALUE_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR10; source_page=70; pd_band=all; value_basis=supervisory_prescribed; adapter=AnzPillar3PdfAdapter |
| ANZ_FINANCIAL_INSTITUTION_0P00_TO_LT0P15_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=59; pd_band=0.00 to <0.15; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_FINANCIAL_INSTITUTION_0P00_TO_LT0P15_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=59; pd_band=0.00 to <0.15; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_FINANCIAL_INSTITUTION_0P15_TO_LT0P25_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=63; pd_band=0.15 to <0.25; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_FINANCIAL_INSTITUTION_0P15_TO_LT0P25_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=59; pd_band=0.15 to <0.25; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_FINANCIAL_INSTITUTION_0P25_TO_LT0P50_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=63; pd_band=0.25 to <0.50; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_FINANCIAL_INSTITUTION_0P25_TO_LT0P50_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=63; pd_band=0.25 to <0.50; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_FINANCIAL_INSTITUTION_0P50_TO_LT0P75_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=63; pd_band=0.50 to <0.75; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_FINANCIAL_INSTITUTION_0P50_TO_LT0P75_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=63; pd_band=0.50 to <0.75; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_FINANCIAL_INSTITUTION_0P75_TO_LT2P50_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=63; pd_band=0.75 to <2.50; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_FINANCIAL_INSTITUTION_0P75_TO_LT2P50_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=63; pd_band=0.75 to <2.50; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_FINANCIAL_INSTITUTION_100P00_DEFAULT_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=59; pd_band=100.00 (Default); value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_FINANCIAL_INSTITUTION_100P00_DEFAULT_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=59; pd_band=100.00 (Default); value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_FINANCIAL_INSTITUTION_10P00_TO_LT100P00_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=63; pd_band=10.00 to <100.00; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_FINANCIAL_INSTITUTION_10P00_TO_LT100P00_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=63; pd_band=10.00 to <100.00; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_FINANCIAL_INSTITUTION_2P50_TO_LT10P00_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=63; pd_band=2.50 to <10.00; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_FINANCIAL_INSTITUTION_2P50_TO_LT10P00_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=63; pd_band=2.50 to <10.00; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RESIDENTIAL_MORTGAGE_0P00_TO_LT0P15_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=0.00 to <0.15; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RESIDENTIAL_MORTGAGE_0P15_TO_LT0P25_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=0.15 to <0.25; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RESIDENTIAL_MORTGAGE_0P25_TO_LT0P50_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=0.25 to <0.50; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RESIDENTIAL_MORTGAGE_0P25_TO_LT0P50_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=0.25 to <0.50; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RESIDENTIAL_MORTGAGE_0P50_TO_LT0P75_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=0.50 to <0.75; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RESIDENTIAL_MORTGAGE_0P50_TO_LT0P75_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=0.50 to <0.75; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RESIDENTIAL_MORTGAGE_0P75_TO_LT2P50_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=0.75 to <2.50; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RESIDENTIAL_MORTGAGE_0P75_TO_LT2P50_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=0.75 to <2.50; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RESIDENTIAL_MORTGAGE_100P00_DEFAULT_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=100.00 (Default); value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RESIDENTIAL_MORTGAGE_10P00_TO_LT100P00_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=10.00 to <100.00; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RESIDENTIAL_MORTGAGE_2P50_TO_LT10P00_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=2.50 to <10.00; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_OTHER_0P00_TO_LT0P15_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=0.00 to <0.15; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_OTHER_0P00_TO_LT0P15_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=0.00 to <0.15; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_OTHER_0P15_TO_LT0P25_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=0.15 to <0.25; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_OTHER_0P15_TO_LT0P25_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=58; pd_band=0.15 to <0.25; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_OTHER_0P25_TO_LT0P50_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=0.25 to <0.50; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_OTHER_0P25_TO_LT0P50_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=0.25 to <0.50; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_OTHER_0P50_TO_LT0P75_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=0.50 to <0.75; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_OTHER_0P50_TO_LT0P75_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=0.50 to <0.75; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_OTHER_0P75_TO_LT2P50_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=0.75 to <2.50; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_OTHER_0P75_TO_LT2P50_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=0.75 to <2.50; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_OTHER_100P00_DEFAULT_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=100.00 (Default); value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_OTHER_100P00_DEFAULT_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=58; pd_band=100.00 (Default); value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_OTHER_10P00_TO_LT100P00_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=10.00 to <100.00; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_OTHER_10P00_TO_LT100P00_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=10.00 to <100.00; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_OTHER_2P50_TO_LT10P00_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=2.50 to <10.00; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_OTHER_2P50_TO_LT10P00_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=64; pd_band=2.50 to <10.00; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_QRR_0P00_TO_LT0P15_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=58; pd_band=0.00 to <0.15; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_QRR_0P00_TO_LT0P15_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=58; pd_band=0.00 to <0.15; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_QRR_0P15_TO_LT0P25_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=58; pd_band=0.15 to <0.25; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_QRR_0P15_TO_LT0P25_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=58; pd_band=0.15 to <0.25; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_QRR_0P25_TO_LT0P50_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=58; pd_band=0.25 to <0.50; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_QRR_0P25_TO_LT0P50_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=58; pd_band=0.25 to <0.50; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_QRR_0P50_TO_LT0P75_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=58; pd_band=0.50 to <0.75; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_QRR_0P50_TO_LT0P75_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=58; pd_band=0.50 to <0.75; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_QRR_0P75_TO_LT2P50_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=58; pd_band=0.75 to <2.50; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_QRR_0P75_TO_LT2P50_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=62; pd_band=0.75 to <2.50; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_QRR_100P00_DEFAULT_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=58; pd_band=100.00 (Default); value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_QRR_100P00_DEFAULT_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=58; pd_band=100.00 (Default); value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_QRR_10P00_TO_LT100P00_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=58; pd_band=10.00 to <100.00; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_QRR_10P00_TO_LT100P00_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=62; pd_band=10.00 to <100.00; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_QRR_2P50_TO_LT10P00_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=58; pd_band=2.50 to <10.00; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_QRR_2P50_TO_LT10P00_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=58; pd_band=2.50 to <10.00; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_SME_0P00_TO_LT0P15_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=62; pd_band=0.00 to <0.15; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_SME_0P15_TO_LT0P25_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=62; pd_band=0.15 to <0.25; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_SME_0P25_TO_LT0P50_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=58; pd_band=0.25 to <0.50; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_SME_0P50_TO_LT0P75_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=58; pd_band=0.50 to <0.75; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_SME_0P75_TO_LT2P50_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=58; pd_band=0.75 to <2.50; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_SME_0P75_TO_LT2P50_PD_PD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=58; pd_band=0.75 to <2.50; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_SME_100P00_DEFAULT_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=62; pd_band=100.00 (Default); value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |
| ANZ_RETAIL_SME_10P00_TO_LT100P00_LGD_LGD_2025Q3 | ANZ | https://www.anz.com.au/shareholder/centre/reporting/pillar-3-disclosure/ | 2026-04-22 | HIGH | coverage=anz_pillar3_annual_pdf; source_table=CR6; source_page=62; pd_band=10.00 to <100.00; value_basis=exposure_weighted; adapter=AnzPillar3PdfAdapter |

### Unavailable sources (gaps documented for MRC)

**ICC Trade Register** (International Chamber of Commerce) — UNAVAILABLE — paid tier required

ICC restructured to paid-only tiers (€2,500–€30,000) effective 2025 edition. Free 2024 edition no longer accessible. Trade finance products (import LC, export LC, performance guarantees, SCF payables) currently calibrated using internal model only. Re-evaluate ICC paid subscription when trade finance exposure exceeds materiality threshold.


## 11. Committee Sign-Off

The following signatures are required to adopt this calibration for the Q3 2025 cycle. Names, dates, and signatures to be filled in at the formal sign-off meeting. A signed copy will be retained in the governance archive and a PDF-exported version will supersede this draft.

### 3 Lines of Defence

| Line | Role | Name | Date | Signature |
|------|------|------|------|-----------|
| 1LoD | Model Owner |  |  |  |
| 2LoD | Model Validation |  |  |  |
| 3LoD | Internal Audit |  |  |  |

---
_Generated by External Benchmark Engine_