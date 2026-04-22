# External Benchmark Calibration Report — Q3 2025

**Prepared for:** Model Risk Committee  
**Period:** Q3 2025  
**Date:** 2026-04-22  
**Classification:** Board / Executive Committee

---

## 1. Executive Summary

**Purpose.** This report benchmarks our internal credit-risk estimates (Probability of Default — PD, Loss Given Default — LGD) against the Big 4 Australian banks' public disclosures, APRA system-wide statistics and ABS/ASIC industry data. It is the Model Risk Committee's record of the **external anchor** used to calibrate the internal PD model this period.

**Key messages for the Board:**

1. **Peer data is complete.** 625 external data points this period spanning ANZ, CBA, NAB, WBC across 13 asset classes, plus APRA sector statistics and ABS/ASIC industry failure rates. No sources are stale; nothing blocks calibration.

2. **Bank vs private-credit PD gap remains material.** For commercial real estate (CRE), a raw 2.50% Pillar 3 PD in major banks translates to a **5.38% PD for private-credit style lending** once selection bias, loan-to-value and short trading-history adjustments are layered on (**2.15× uplift**). This is the main structural difference between bank and non-bank credit risk and should inform concentration-limit decisions.

3. **System credit-stress signal: deteriorating.** The APRA-reported impaired-loan ratio across all Australian ADIs is **1.03%** — within historical norms but +0.16 pp versus 0.87% three years ago. Direction of travel, not level, is the watch-item for forward ECL staging.

4. **Calibrated PDs within expected ranges.** Segments calibrated this period: **Corporate SME, Residential Mortgage**. All values sit in line with peer medians; no regulatory floors triggered. These feed directly into the internal PD model's long-run average anchor.

5. **Data governance: clean.** Zero stale sources. The 76 quality flags visible in the Technical Appendix all relate to ABS/ASIC industry data (annual publication cadence) — policy treats these as directional context only, not as calibration inputs, so the flags are expected and **not** a remediation item.

## 2. Peer Benchmark Comparison

_Median PD and LGD by asset class, sourced from Pillar 3 disclosures. Default band (100%) excluded._

### Corporate (General)

_Lending to large corporates (non-financial, non-property) — listed companies and large private enterprises._

| metric | ANZ | CBA | NAB | WBC | Peer median |
|---|---|---|---|---|---|
| PD (median, %) | 3.09 | 2.27 | 2.65 | 3.09 | 2.38 |
| LGD (median, %) | 32.00 | 42.00 | 26.00 | 40.00 | 35.00 |

### Corporate SME

_Small and medium business lending (typical SME business-banking book)._

| metric | ANZ | CBA | NAB | WBC | Peer median |
|---|---|---|---|---|---|
| PD (median, %) | — | 2.63 | — | — | 2.63 |
| LGD (median, %) | — | 25.00 | — | — | 25.00 |

### Financial Institution

_Lending to other banks, insurers and non-bank financial firms (interbank / wholesale counterparties)._

| metric | ANZ | CBA | NAB | WBC | Peer median |
|---|---|---|---|---|---|
| PD (median, %) | 0.59 | 0.46 | — | 4.78 | 0.83 |
| LGD (median, %) | 48.00 | 46.00 | — | 36.00 | 45.50 |

### RBNZ Non-Retail (NZ branches)

_Wholesale and corporate exposures booked in NZ branches, regulated by RBNZ rather than APRA._

| metric | ANZ | CBA | NAB | WBC | Peer median |
|---|---|---|---|---|---|
| PD (median, %) | — | 0.55 | — | — | 0.55 |
| LGD (median, %) | — | 28.00 | — | — | 28.00 |

### RBNZ Retail (NZ branches)

_Retail and mortgage exposures booked in NZ branches, regulated by RBNZ._

| metric | ANZ | CBA | NAB | WBC | Peer median |
|---|---|---|---|---|---|
| PD (median, %) | — | 0.66 | — | 4.17 | 1.38 |
| LGD (median, %) | — | 21.00 | — | 39.00 | 24.00 |

### Residential Mortgage

_Owner-occupied and investor home loans secured by residential property — the largest asset class in Australian banks._

| metric | ANZ | CBA | NAB | WBC | Peer median |
|---|---|---|---|---|---|
| PD (median, %) | 0.66 | 0.57 | — | 1.18 | 0.66 |
| LGD (median, %) | 19.00 | 17.00 | — | 16.00 | 17.00 |

### Retail — Other

_Unsecured retail lending other than credit cards — e.g. personal loans, overdrafts._

| metric | ANZ | CBA | NAB | WBC | Peer median |
|---|---|---|---|---|---|
| PD (median, %) | 0.62 | 1.05 | — | 1.58 | 0.97 |
| LGD (median, %) | 78.00 | 84.00 | — | 78.00 | 78.00 |

### Retail — Qualifying Revolving (QRR)

_Qualifying Revolving Retail — mainly credit cards and revolving overdrafts meeting APS 113 criteria._

| metric | ANZ | CBA | NAB | WBC | Peer median |
|---|---|---|---|---|---|
| PD (median, %) | 0.65 | 0.60 | — | 0.61 | 0.61 |
| LGD (median, %) | 75.00 | 84.00 | — | 81.00 | 81.00 |

### Retail SME

_Lending to sole-traders and micro-businesses treated under retail (not corporate) IRB rules._

| metric | ANZ | CBA | NAB | WBC | Peer median |
|---|---|---|---|---|---|
| PD (median, %) | 3.01 | 2.76 | — | 3.06 | 2.88 |
| LGD (median, %) | 27.00 | 37.00 | — | 34.50 | 35.00 |

### Sovereign

_Exposures to governments and central banks (Commonwealth, states, foreign sovereigns)._

| metric | ANZ | CBA | NAB | WBC | Peer median |
|---|---|---|---|---|---|
| PD (median, %) | 0.58 | 0.46 | — | 1.37 | 0.55 |
| LGD (median, %) | 50.00 | 28.50 | — | 48.00 | 50.00 |

## 3. Industry Context — ABS & ASIC

**APRA ADI sector — impaired exposure ratio**

_**APRA ADI sector** = all Authorised Deposit-taking Institutions in Australia combined (Big 4 + regional banks + mutuals + foreign bank branches), regulated by the Australian Prudential Regulation Authority. The figures below are the system-wide view, published quarterly in APRA's Monthly ADI Statistics._

_**90+ DPD / impaired ratio** = the share of gross loans either (a) more than 90 days past due (**DPD**) or (b) classified as impaired (borrower unable to meet obligations without security enforcement). It is the headline industry credit-quality indicator — rising values signal deteriorating system-wide credit health._

| metric | latest_pct | as_of | 3y_prior_pct | 3y_prior_date |
|---|---|---|---|---|
| ADI sector 90+ DPD / impaired ratio | 1.03 | 2025-12-31 | 0.87 | 2019-03-31 |

**What this means for credit:**

- ADI sector impaired/90+ DPD ratio of **1.03%** is **deteriorating** versus 0.87% three years prior (absolute change +0.16 pp, relative +18.4%).
- Level is within the typical post-cycle range; no system-wide stress signal, but worth monitoring.
- Direction of travel is upward — factor into forward ECL staging assumptions and concentration-limit review.

**ASIC / ABS business failure rates by industry (latest)**

| industry | failure_rate_pct | as_of | publisher |
|---|---|---|---|
| Accommodation & Food Services | 2.19 | 2025-06-30 | ASIC_ABS |
| Administrative & Support Services | 0.63 | 2025-06-30 | ASIC_ABS |
| Agriculture, Forestry & Fishing | 0.10 | 2025-06-30 | ASIC_ABS |
| Arts & Recreation Services | 0.67 | 2025-06-30 | ASIC_ABS |
| Construction | 0.78 | 2025-06-30 | ASIC_ABS |
| Education & Training | 0.38 | 2025-06-30 | ASIC_ABS |
| Financial & Insurance Services | 0.33 | 2025-06-30 | ASIC_ABS |
| Healthcare & Social Assistance | 0.23 | 2025-06-30 | ASIC_ABS |
| Information Media & Telecoms | 1.31 | 2025-06-30 | ASIC_ABS |
| Manufacturing | 0.70 | 2025-06-30 | ASIC_ABS |
| Mining | 1.66 | 2025-06-30 | ASIC_ABS |
| Other Services | 1.12 | 2025-06-30 | ASIC_ABS |
| Professional, Scientific & Technical Services | 0.29 | 2025-06-30 | ASIC_ABS |
| Public Administration & Safety | 0.90 | 2025-06-30 | ASIC_ABS |
| Rental, Hiring & Real Estate | 0.18 | 2025-06-30 | ASIC_ABS |
| Retail Trade | 0.56 | 2025-06-30 | ASIC_ABS |
| Transport, Postal & Warehousing | 0.29 | 2025-06-30 | ASIC_ABS |
| Electricity, Gas, Water & Waste | 1.93 | 2025-06-30 | ASIC_ABS |
| Wholesale Trade | 0.32 | 2025-06-30 | ASIC_ABS |

**What this means for credit:**

- Highest failure rates concentrated in: **Accommodation & Food Services (2.19%), Electricity, Gas, Water & Waste (1.93%), Mining (1.66%)** — review portfolio exposure concentration to these ANZSIC divisions.
- Cross-industry median failure rate is **0.63%**; spread across industries is 2.09 pp, signalling material sector dispersion.
- Use this context to challenge internal SME and corporate PD models where sector concentration is high; consider overlay if your book skews to the top-3 failure-rate industries above.

_Used as directional context only; not incorporated into calibrated PDs per MRC policy._

## 4. Calibrated Benchmarks (final values)

| segment | triangulated_pd | calibrated_pd | method | floor_triggered |
|---|---|---|---|---|
| Corporate SME | 2.6350% | 2.6350% | external_blending (internal_weight=0.9) | No |
| Residential Mortgage | 0.8329% | 0.8329% | external_blending (internal_weight=0.9) | No |

**What this means for credit:**

- **Corporate SME**: calibrated PD of **2.63%** is **elevated** — typical for SME / sub-IG corporate. Feeds directly into the internal PD model's long-run anchor.
- **Residential Mortgage**: calibrated PD of **0.83%** is **moderate** — typical for performing retail / mortgage. Feeds directly into the internal PD model's long-run anchor.
- Calibrated values are the period's **external anchor** for the PD calibration module; they will be blended with internal default experience (see §5.5 of README) before flowing to RWA and ECL.
- No regulatory floors triggered this period — all calibrated PDs sit above the 3 bps APRA minimum.

## 5. Key Observations & Recommendations

1. **Bank vs private-credit PD uplift.** Commercial real estate benchmark shows a 2.15× PD uplift once private-credit adjustments (selection bias, loan-to-value, trading history) are applied — raw 2.50% → 5.38%. **Recommendation:** reaffirm concentration limits on non-bank CRE exposure, and review whether risk pricing on PC lines reflects this gap.
2. **Peer outlier — Financial Institution.** WBC's PD of 4.78% is 5.8× the peer median of 0.83%. **Recommendation:** investigate whether the gap is driven by a genuine portfolio-mix difference, a one-off default event, or a potential data-quality issue in the source disclosure; footnote in the Technical Appendix if material.
3. **System-wide credit stress: deteriorating, within normal range.** APRA ADI 90+ DPD / impaired ratio stands at 1.03% (+0.16 pp vs 3 years ago, +18.4% relative). **Recommendation:** add to the forward ECL staging-assumption review for Q4; consider a management overlay if direction persists next quarter.
4. **Industry watchlist — highest business failure rates:** Accommodation & Food Services (2.19%), Electricity, Gas, Water & Waste (1.93%), Mining (1.66%). **Recommendation:** Credit team to confirm our portfolio exposure to these ANZSIC divisions is within appetite; if concentration > 10% of book in any one, consider a sector-level PD overlay.
5. **Elevated calibrated PDs.** The following segments sit above 2.0% after calibration: **Corporate SME**. **Recommendation:** confirm these anchors align with internal default experience in the PD calibration module; flag any segment whose internal long-run average differs by > 50 bps.
6. **No regulatory floors triggered.** All calibrated PDs sit above the APS 113 3 bps minimum — no prudential floor override required. **Recommendation:** none — flagged for completeness.
7. **Data governance.** Zero stale sources; refresh cadence on track. Quality flags on ABS/ASIC rows are by design (annual publication cadence, policy is directional context only). **Recommendation:** no action — continue current cadence.
8. **Methodology review (standing item).** Private-credit adjustment multipliers (selection bias 1.75–2.25, LVR 1.10–1.25) and external-blending weight schedule (0.9 at 5+ years of internal data) have not been re-examined this period. **Recommendation:** schedule a full methodology review when the next Framework revision cycle opens; confirm whether current ranges still reflect observed PC default experience.

## 6. Governance & Sign-off

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

## 11. Committee Sign-Off

The following signatures are required to adopt this calibration for the Q3 2025 cycle. Names, dates, and signatures to be filled in at the formal sign-off meeting. A signed copy will be retained in the governance archive and a PDF-exported version will supersede this draft.

### 3 Lines of Defence

| Line | Role | Name | Date | Signature |
|------|------|------|------|-----------|
| 1LoD | Model Owner |  |  |  |
| 2LoD | Model Validation |  |  |  |
| 3LoD | Internal Audit |  |  |  |

---

_Full source register, adjustment audit trail, version history and governance findings are available in the accompanying **Technical Appendix**._

_Generated by External Benchmark Engine_