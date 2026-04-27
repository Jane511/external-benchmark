# External Benchmark — Environment & Industry Overlay Report — Q1 2026 — Board Summary

_Companion to Report 1 · Sourced from industry-analysis v1 contracts · Generated 2026-04-23 · Data as-of 2026-03-16_

## 1. Executive Summary

Benign macro backdrop — current-state signals support the base-scenario overlay. No automatic uplift applied to stressed pricing inputs this cycle.

- Macro regime flag is **base** (cash-rate regime: neutral_easing; arrears environment: Low, trending improving).
- 4 of 9 industries carry an 'Elevated' base risk level; top by structural score is **Agriculture, Forestry And Fishing** (3.50, Elevated).
- Property cycle shows 1 segment(s) in Downturn, 1 Slowing, 6 Neutral, 3 Growth.
- Downturn segments: Offices.
- Growth segments: Short term accommodation buildings, Commercial Buildings - Total, Health buildings.

> **Flagship read.** Macro regime is **base**. 4 of 9 industries sit at 'Elevated' base risk.

## 2. Industry Risk Outlook

All 9 ANZSIC divisions ranked by structural base risk score. Macro conditioner: cash rate 3.85% (-0.25 pp YoY).

| Rank | Industry | Base score | Level |
|---|---|---|---|
| 1 | Agriculture, Forestry And Fishing | 3.50 | Elevated |
| 2 | Manufacturing | 3.50 | Elevated |
| 3 | Wholesale Trade | 3.23 | Elevated |
| 4 | Retail Trade | 3.23 | Elevated |
| 5 | Accommodation And Food Services | 2.68 | Medium |
| 6 | Construction | 2.68 | Medium |
| 7 | Health Care and Social Assistance | 2.22 | Medium |
| 8 | Professional, Scientific And Technical Services | 2.18 | Medium |
| 9 | Transport, Postal And Warehousing | 2.14 | Medium |

**Top-3 drivers**

- **Agriculture, Forestry And Fishing** (score 3.50, Elevated): primarily structural (cyclicality / concentration). Classification component 3.75, macro component 3.20.
- **Manufacturing** (score 3.50, Elevated): primarily structural (cyclicality / concentration). Classification component 3.75, macro component 3.20.
- **Wholesale Trade** (score 3.23, Elevated): macro and structural factors roughly balanced. Classification component 3.25, macro component 3.20.

> **Methodology note.** Construction is the leading example: the structural score places it in the 'Medium' band, but market narrative over 2024–2026 (Porter Davis, Probuild, Clough collapses; subcontractor arrears at multi-year highs; fixed-price + materials-inflation squeeze) suggests 'Elevated' is the more honest current-state read. This divergence is a known methodology-review item in industry-analysis, not a data error.

## 3. Property Market Outlook

11 property segments grouped by cycle stage.

### Downturn (1 segments)

_Contracting segments — approvals trending down materially, market softness elevated. Highest portfolio risk; consider limit tightening and LVR haircuts._

| Segment | Softness | Band | Approvals Δ% |
|---|---|---|---|
| Offices | 4.30 | soft | -35.7 |

### Slowing (1 segments)

_Past-peak segments — approvals flat to soft, market softness rising. Watch list; no immediate tightening required but concentration should be monitored._

| Segment | Softness | Band | Approvals Δ% |
|---|---|---|---|
| Education buildings | 3.25 | softening | -21.4 |

### Neutral (6 segments)

_Mid-cycle segments — approvals mixed, softness neither supportive nor stressed. Standard credit appetite applies._

| Segment | Softness | Band | Approvals Δ% |
|---|---|---|---|
| Retail and wholesale trade buildings | 3.15 | normal | +68.5 |
| Aged care facilities | 2.70 | normal | +219.9 |
| Agricultural and aquacultural buildings | 2.65 | normal | +58.4 |
| Total Non-residential | 2.60 | normal | +71.5 |
| Industrial Buildings - Total | 2.40 | normal | +55.5 |
| Warehouses | 2.20 | normal | +69.3 |

### Growth (3 segments)

_Expanding segments — approvals trending up materially, market softness low. Opportunity-side; assess whether current limits capture the tailwind without overshoot._

| Segment | Softness | Band | Approvals Δ% |
|---|---|---|---|
| Short term accommodation buildings | 2.85 | supportive | +113.7 |
| Commercial Buildings - Total | 2.30 | supportive | +165.4 |
| Health buildings | 1.65 | supportive | +355.0 |

- **Most at risk (highest market softness):** Offices (softness 4.30, downturn); Education buildings (softness 3.25, slowing); Retail and wholesale trade buildings (softness 3.15, neutral).
- **Strongest tailwinds (highest approvals momentum):** Health buildings (approvals +355.0%, growth); Aged care facilities (approvals +219.9%, neutral); Commercial Buildings - Total (approvals +165.4%, growth).

## 4. Downturn Scenario Overlays

| Scenario | PD × | LGD × | CCF × | Property haircut |
|---|---|---|---|---|
| Base | 1.00 | 1.00 | 1.00 | 0.00 |
| Mild | 1.20 | 1.10 | 1.05 | 0.05 |
| Moderate | 1.50 | 1.20 | 1.10 | 0.10 |
| Severe | 2.00 | 1.30 | 1.20 | 0.20 |

**Interpretation**

- **Base** — Current staged environment. Multipliers = 1.00 by construction; represents the starting point before any downturn overlay is applied.
- **Mild** — A short, shallow downturn — typical of a mid-cycle pullback. Use for conservative portfolio calibration when the book is running on optimistic through-the-cycle assumptions.
- **Moderate** — A deeper, more prolonged stress consistent with a material macro slowdown. Use as the primary stressed-pricing input and for ECL staging sensitivities.
- **Severe** — Tail-risk scenario. Not calibrated to any specific regulatory stress (e.g. APRA CPG 220). Use for concentration-risk stress testing and what-if analysis, not for capital.

## 5. Methodology Notes

Report 2 is a read-only overlay view of the `industry-analysis` project's canonical parquet exports. Benchmark-engine code does not modify or recompute those contracts; changes to the underlying scoring methodology must be raised in that upstream repo.

**Structural vs current-state risk**
Industry base risk scores reflect structural classification factors (cyclicality, market concentration) blended with a single macro conditioner (cash-rate regime and one-year change). They do not incorporate real-time sector-stress signals such as ASIC insolvency flows, subcontractor arrears, or sector-specific collapse events.

> Construction is the leading example: the structural score places it in the 'Medium' band, but market narrative over 2024–2026 (Porter Davis, Probuild, Clough collapses; subcontractor arrears at multi-year highs; fixed-price + materials-inflation squeeze) suggests 'Elevated' is the more honest current-state read. This divergence is a known methodology-review item in industry-analysis, not a data error.

**Path taken.** This report takes **Option 3 — document the limitation**. It surfaces the Construction caveat to committee readers explicitly, without altering the upstream risk scores or applying a local override. A future methodology cycle may choose Option 1 or 2; until then, treat the industry risk table as a structural backdrop, and pair it with the ABS/ASIC failure-rate context in Report 1 §4 (Industry Context) for a fuller picture.

_See Technical variant for the full methodology write-up and the three options considered upstream._

_Generated by External Benchmark Engine — Report 2 (Board variant). Upstream: industry-analysis (v1 contracts)._