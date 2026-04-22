# External Benchmark Engine

Generates external PD/LGD benchmarks for Australian credit  (Big 4 banks + APRA + ASIC/ABS) and produces a **Board report** and **Technical Appendix** per quarter.

This README is the operational run-book for: environment setup, data refresh schedule, CLI workflow, and the exact numbers to review before the report goes to the Model Risk Committee / Board.

---

## Deliverables (Q3 2025 cycle)

- `outputs/reports/Report_Q3_2025_final.docx` — MRC-ready Word document
- `outputs/reports/Report_Q3_2025_final.html` — browser view
- `outputs/reports/Report_Q3_2025_final_Board.md` — board summary
- `outputs/reports/Report_Q3_2025_final_Technical.md` — technical appendix
- `outputs/spot_check_verification.md` — human-verified extraction QA

## Quarterly refresh (once your data is available)

1. Re-run each Big 4 bank adapter (one command per bank — the CLI has no bulk alias):

   ```bash
   python cli.py ingest pillar3 cba --reporting-date <YYYY-MM-DD>
   python cli.py ingest pillar3 nab --reporting-date <YYYY-MM-DD>
   python cli.py ingest pillar3 wbc --reporting-date <YYYY-MM-DD>
   python cli.py ingest pillar3 anz --reporting-date <YYYY-MM-DD>
   ```

2. `python cli.py ingest apra` (refresh APRA data)
3. `python cli.py ingest asic-abs` (refresh combined ASIC insolvency + ABS business counts)
4. `python cli.py report benchmark --format docx --output outputs/reports/Report_<PERIOD>_final.docx`
5. `python cli.py report benchmark --format markdown --output outputs/reports/Report_<PERIOD>_final.md` (emits both `*_Board.md` and `*_Technical.md`)
6. Manual spot-check of 5 random rows against source PDFs — use `python scripts/pick_spot_checks.py` to sample rows, then record findings in `outputs/spot_check_verification.md`
7. Email DOCX to MRC

---

## 1. What this tool does

The engine ingests public disclosures from four source types, normalises them into a single registry, runs adjustment and triangulation logic, and emits reports:

| Source type       | Publisher(s)              | Cadence       | What we pull                                       |
|-------------------|---------------------------|---------------|----------------------------------------------------|
| `pillar3`         | ANZ, CBA, NAB, WBC        | Half-yearly   | PD/LGD per asset class × PD band (APS 330 CR6/CR10) |
| `apra_adi`        | APRA                      | Quarterly     | ADI sector 90+ DPD / impaired exposure ratios      |
| `insolvency`      | ASIC + ABS                | Annual/quarterly | Business failure & insolvency appointment rates |
| `icc_trade`       | ICC Trade Register        | Annual (paid) | Trade-finance default & recovery — **manual only** |

Output: two markdown files per period (Board + Technical) plus optional DOCX/HTML.

---

## 2. First-time setup

```bash
# From repo root (the folder containing this README)
python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # macOS/Linux

pip install -e ".[ingestion,download,reports]"
```

Required Python: **3.9+**. The SQLite database `benchmarks.db` is created on first run.

Smoke-test the install:

```bash
python cli.py --help
python cli.py list --help
```

---

## 3. The quarterly workflow (end-to-end)

Run these four steps in order each quarter. Total wall-clock time ≈ 15–30 min (mostly the Pillar 3 PDF scrapes).

### Step 1 — Download raw source files

```bash
# Big 4 Pillar 3 disclosures (PDFs + CBA XLSX)
python scripts/download_sources/pillar3_downloader.py

# APRA ADI quarterly statistics (XLSX)
python scripts/download_sources/apra_downloader.py

# ASIC insolvency statistics (Series 1 & 2 XLSX)
python scripts/download_sources/asic_insolvency_downloader.py

# ABS business entries/exits (Cat. 8165 XLSX)
python scripts/download_sources/abs_business_counts_downloader.py
```

Files land under [data/raw/](data/raw/), [data/asic/](data/asic/), [data/abs/](data/abs/). If a scraper fails, see §6 *Common failures*.

### Step 2 — Ingest into the registry

```bash
# Big 4 — pass --reporting-date of the disclosure period end
python cli.py ingest pillar3 --reporting-date 2025-09-30

# APRA
python cli.py ingest apra

# ASIC + ABS (combined into failure rates)
python cli.py ingest asic-abs
```

Check what landed:

```bash
python cli.py ingest status
python cli.py list --limit 20
```

### Step 3 — Generate reports

**Quick reference — running reports for Bank vs Private Credit:**

- Use `--institution-type bank` for MRC / Bank Board reports; use `--institution-type private_credit` for Credit Committee reports.
- Each variant produces a different report (differences listed in the comparison table below): title, committee label, Stage 2 adjustment chain applied, sign-off block, and DOCX helper wrappers.
- **Do not confuse** the top-level `--institution` flag (controls *ingestion* — which adjustment profile binds to the DB) with the `--institution-type` flag on `report benchmark` (controls the *report template*). Always pass `--institution-type` on the `report benchmark` subcommand itself.

The engine emits a different report variant depending on who the audience is. Pick one with the **`--institution-type`** flag:

```bash
# Bank / MRC variant (default — omits flag if bank)
python cli.py report benchmark --format markdown \
    --institution-type bank \
    --period-label "Q3 2025" \
    --output outputs/reports/Report_Q3_2025.md

# Private-credit / Credit Committee variant
python cli.py report benchmark --format markdown \
    --institution-type private_credit \
    --period-label "Q3 2025" \
    --output outputs/reports/Report_Q3_2025_PC.md
```

Each invocation writes **two** files (Board + Technical):
- `*_Board.md` — board-ready (~2–3 pages)
- `*_Technical.md` — full source register, audit trail, governance

**What `--institution-type` changes in the output:**

| Element                  | `bank`                                                 | `private_credit`                                      |
|--------------------------|--------------------------------------------------------|-------------------------------------------------------|
| Report title             | "External Benchmark Calibration Report — Q3 2025"      | "External Benchmark Report — Q3 2025"                 |
| Committee label          | Model Risk Committee                                   | Credit Committee                                      |
| Flagship adjustment chain | Bank Stage 2: `peer_mix × geography_ig` (near-neutral) | PC Stage 2: `selection_bias × lvr × trading_history` (2.15x uplift) |
| Sign-off block           | 3 Lines of Defence (1LoD / 2LoD / 3LoD)                | Credit Committee Decision Log + Next Review Actions   |
| DOCX footer (if chosen)  | `add_3lod_signoff()`                                   | `add_decision_log()` + `add_next_review_actions()`    |

The underlying benchmark entries in the registry are the same — what differs is (a) the Stage 2 adjustment multipliers applied (see §6.3–6.4 below) and (b) the sign-off governance wrapper. Run both variants in the same quarter if you need separate packs for MRC and Credit Committee.

For DOCX / HTML (optional — same `--institution-type` flag applies):

```bash
python cli.py report benchmark --format docx --institution-type bank --period-label "Q3 2025"
python cli.py report benchmark --format html --institution-type private_credit --period-label "Q3 2025"
```

> **Note:** the top-level `--institution` flag (e.g. `python cli.py --institution private_credit report ...`) controls the **ingestion** side (which adjustment profile to bind to). For report generation, always use the `--institution-type` flag on the `report benchmark` subcommand itself — that's what drives the template.

### Step 4 — Review before sign-off

See §5 below — this is the most important step and **must not be skipped**.

---

## 4. What to run, when

| Frequency          | Commands                                              | Trigger                                    |
|--------------------|-------------------------------------------------------|--------------------------------------------|
| **Quarterly** (Feb, May, Aug, Nov) | `pillar3_downloader.py` → `ingest pillar3` → `ingest apra` → `report benchmark` | ~30 days after quarter-end, when APRA publishes |
| **Annually** (July) | `asic_insolvency_downloader.py` + `abs_business_counts_downloader.py` → `ingest asic-abs` | ABS Cat. 8165 releases each August        |
| **Annually** (when available) | Manually download ICC Trade Register PDF → `python cli.py ingest icc --path <file>` | Currently paywalled — budget needed       |
| **Ad hoc**         | `python cli.py report stale`                          | Before any report run, to catch missing refreshes |
| **Ad hoc**         | `python cli.py report quality`                        | When a new source type is added           |

Stale-detection thresholds live in [config/refresh_schedules.yaml](config/refresh_schedules.yaml) (pillar3 = 120 days, insolvency = 210 days, etc.).

---

## 5. Review checklist before the report goes to Board

These are the numbers in `Report_Q3_2025_Board.md` that **you must tie back to source** each period. Any discrepancy holds the report.

### 5.1 Peer comparison table (§2 of Board report)

For each asset class, compare the median PD / LGD per bank against the raw Pillar 3 disclosure PDF:

| Board figure               | Where to verify                                                           |
|----------------------------|---------------------------------------------------------------------------|
| ANZ PD / LGD medians       | ANZ Pillar 3 PDF, Table CR6 (cached in [data/raw/pillar3/](data/raw/pillar3/)) |
| CBA PD / LGD medians       | CBA Basel III Pillar 3 Capital Adequacy PDF, Table 12.1                   |
| NAB PD / LGD medians       | NAB Pillar 3 PDF, Table 21 (CR6)                                          |
| WBC PD / LGD medians       | Westpac Pillar 3 PDF, Table 21                                            |

**Known data-quality issue:** NAB's `corporate_general` LGD sometimes reads as 0.45% (should be ~45%) due to a decimal-scale inconsistency in the source. If you see a peer value two orders of magnitude off the others, inspect [ingestion/adapters/nab_pillar3_pdf_adapter.py](ingestion/adapters/nab_pillar3_pdf_adapter.py) before publishing.

### 5.2 Flagship CBA CRE PD (§1 Executive Summary, §7 Technical)

The "raw CBA CRE PD 2.50% → Bank 2.50% / PC 5.38% (2.15x)" figure is **hard-coded** in [reports/benchmark_report.py](reports/benchmark_report.py) (`_build_bank_vs_pc_comparison`, `raw_pd = 0.025`). Update when the CBA disclosure value changes.

### 5.3 APRA ADI impaired ratio (§3 Board report)

- Latest quarter value + 3-year-prior value are pulled from the registry automatically.
- Sanity-check against APRA's *Quarterly ADI Performance Statistics* Table 5.
- If the latest publication date in the report is older than ~30 days past quarter-end, APRA hasn't published yet — wait.

### 5.4 ASIC / ABS industry failure rates (§3 Board report)

- One row per ANZSIC division, using the latest available value per industry.
- Verify against ABS *Counts of Australian Businesses* (Cat. 8165.0) and ASIC *Insolvency Statistics Series 1 & 2*.
- These are **directional context only** — do not incorporate into calibrated PDs (policy set in §5.1 of the board report and enforced by `SourceType.INSOLVENCY.frequency = LOW` in [src/governance.py](src/governance.py)).

### 5.5 Calibrated benchmarks (§4 Board report)

This is the table that feeds directly into the PD Calibration module and ultimately into the internal model. Each row has two numbers you must understand and verify:

**Pipeline end-to-end:**

```
raw external entries (Pillar 3 PD per band)
       │
       ▼  AdjustmentEngine.adjust()   [Stage 1 + Stage 2 — see §6]
adjusted values per source
       │
       ▼  BenchmarkTriangulator.triangulate()
triangulated_pd  ───────────────────────→ (column 1 in §4 table)
       │
       ▼  CalibrationFeed.for_<method>()  +  regulatory floor
calibrated_pd    ───────────────────────→ (column 2 in §4 table)
```

#### 5.5.1 `triangulated_pd` — how it's computed

`triangulated_pd` is the weighted average of **adjusted** external PD values across all banks reporting in that segment.

- **Default method:** `weighted_by_years` — each source is weighted by the disclosure period length (clamped at `max(period_years, 1)`). Half-yearly ANZ/NAB/WBC PDFs therefore carry more weight than quarterly CBA XLSX. See [src/triangulation.py](src/triangulation.py).
- **Alternative methods** (not currently wired to the board report but available in [src/triangulation.py](src/triangulation.py)): `simple_average`, `quality_weighted` (HIGH = 3×, MEDIUM = 2×, LOW = 1× — weights hard-coded in `_QUALITY_WEIGHTS`), `trimmed_mean` (drops min and max — requires ≥ 4 sources).
- **What goes in:** only PD entries for the segment (LGD and supervisory-value rows are filtered out). Each entry is first run through the full Stage 1 + Stage 2 adjustment chain from §6 — so the multipliers you tune in `adjustment_profiles.yaml` flow through to this number.
- **Confidence (`confidence_n`)** is also computed and shown in the Technical Appendix §4; it's clamped at 500. Low confidence (N < 40) should be footnoted for the committee.

Verify `triangulated_pd` by: pulling the adjusted values per source from the Technical Appendix §3 (adjustment audit trail), computing the period-year-weighted average yourself, and matching. Off by > 0.5 bps → investigate the adjustment chain.

#### 5.5.2 `calibrated_pd` — how it's computed

`calibrated_pd` takes `triangulated_pd` and applies one of **five** PD calibration methods, then a regulatory floor. All five live in [src/calibration_feed.py](src/calibration_feed.py).

| Method                   | Output field        | What it does                                                                                                    | When to use                                           |
|--------------------------|---------------------|-----------------------------------------------------------------------------------------------------------------|-------------------------------------------------------|
| `central_tendency`       | `external_lra`      | Returns the triangulated PD as-is (after floor). Pure external anchor.                                          | No internal data yet; pure external calibration       |
| `logistic_recalibration` | `target_lra` + `confidence_n` | Same value as central_tendency, carrying confidence count for logistic-regression scaling downstream. | Rank-order preserved from internal model; just rescale target rate |
| `bayesian_blending`      | `external_pd` + `confidence_n` | Supplies the prior (external) and sample size for Bayesian blending with internal frequency data.   | When you want to blend internal and external with explicit prior strength |
| `external_blending`      | `external_lra` + `internal_weight` | Blends external LRA with internal LRA using a weight schedule: **< 3 yr = 0.30**, 3–4 yr = 0.50, 4–5 yr = 0.70, **5+ yr = 0.90**. | Default method in board report — use when internal data length drives the weight |
| `pluto_tasche`           | `external_pd` + `role="comparison_only"` | Low-default-portfolio method; external serves as comparison anchor only, not as a direct input.   | For LDPs like sovereigns and financial institutions  |

**Which method is used in the board report:** `external_blending` with `internal_years=5` (→ `internal_weight = 0.9`, so `calibrated_pd = 0.9 × internal + 0.1 × external`). This is why the Q3 2025 board report shows `calibrated_pd ≈ triangulated_pd` — the display currently reflects the external component only; the internal blend is applied downstream in the calibration module. See [reports/benchmark_report.py](reports/benchmark_report.py) `_call_feed_method`.

**Regulatory floor (APRA APS 113):** `DEFAULT_REGULATORY_FLOOR = 0.0003` (3 bps) — hard-coded in [src/calibration_feed.py](src/calibration_feed.py:39). Any calibrated PD below 3 bps is clamped up and `floor_triggered = Yes` flags it in the report. The floor applies to **PD only**; LGD never goes through this path (LGD uses `downturn.py`).

#### 5.5.3 Relationship to the adjustment multipliers from §6

Each adjustment multiplier you tune in [config/adjustment_profiles.yaml](config/adjustment_profiles.yaml) flows through to **both** columns of the §4 table:

- Raising `peer_mix` default from 1.00 → 1.05 raises every bank-source adjusted value by 5%, which raises `triangulated_pd` roughly 5%, which raises `calibrated_pd` by the same 5% (unless the floor activates).
- PC selection_bias changes do **not** affect the board-report table because the report runs the Bank adjustment chain. PC chain only runs when `--institution-type private_credit` is set (see §3).
- Stage 1 `apra_impaired_to_pd = 1.50` only affects segments fed by APRA impaired-ratio entries (currently none in `corporate_sme` / `residential_mortgage`, so it's a no-op for the current §4 rows).

#### 5.5.4 How to update the calibration assumptions

| What you want to change                                              | Where to change it                                                                                                                                   |
|----------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| Triangulation weighting scheme (e.g. switch to `quality_weighted`)   | `CalibrationFeed(... triangulation_method="quality_weighted")` constructor arg in [cli.py](cli.py) — currently hard-coded to `weighted_by_years`     |
| Quality weights (HIGH/MEDIUM/LOW = 3/2/1)                            | `_QUALITY_WEIGHTS` dict in [src/triangulation.py](src/triangulation.py:52-56). Requires MRC approval.                                                 |
| Regulatory floor (3 bps → e.g. 5 bps)                                | `DEFAULT_REGULATORY_FLOOR` in [src/calibration_feed.py](src/calibration_feed.py:39). Ties to APRA APS 113 — **do not change without MRC + regulatory sign-off**. |
| External-blending weight schedule (0.30 / 0.50 / 0.70 / 0.90)        | `_internal_weight_for_years()` function at the bottom of [src/calibration_feed.py](src/calibration_feed.py:163-171)                                  |
| Which segments get calibrated                                        | `DEFAULT_PD_SEGMENTS` tuple at the top of [reports/benchmark_report.py](reports/benchmark_report.py:46-52)                                           |
| Which method appears as `calibrated_pd` in the board report          | `_render_board_markdown` in [reports/benchmark_report.py](reports/benchmark_report.py) — currently picks `external_blending`. Change the lookup in the row-builder if you want a different method surfaced. |
| Internal data history length (drives blending weight)                | `internal_years=5` in `_call_feed_method` in [reports/benchmark_report.py](reports/benchmark_report.py) — update to reflect the actual model's data vintage |

**Governance for calibration changes:** same gate as §6.7 (adjustment config) — pytest must pass, board report must be regenerated and the §4 table re-reviewed, MRC decision register entry with rationale. Changes to the regulatory floor or quality weights additionally require documented referencing to the driving APRA guidance or framework revision.

#### 5.5.5 If `floor_triggered = Yes`

A triggered floor means `triangulated_pd < 3 bps`. Either:

1. The external benchmark is genuinely low (e.g. AAA sovereign, prime senior residential mortgage) — leave the floor in place and add a footnote acknowledging it.
2. A data-quality issue is dragging the triangulation down (check the Technical Appendix §3 for an outlier source with an unreasonably low adjusted value) — fix the ingestion before publishing.
3. An adjustment multiplier has been set too aggressively (e.g. `peer_mix` default below 1.00) — review §6 config.

### 5.6 Governance flag counts (§6 Board report)

- `stale` flag_count should be **0**. Anything >0 means a source missed its refresh window — re-run the relevant downloader before publishing.
- `quality` flag_count of ~76 is expected (all ASIC/ABS rows flag `low_quality:…:frequency` — this is by design, see §5.4 above).
- `pillar3_divergence` flag_count highlights where one Big 4 bank's PD is materially above peer median — investigate but not a blocker.

---

## 6. Bank vs Private Credit adjustments

Every external benchmark entering the calibration feed is put through a **2-stage adjustment chain** by [src/adjustments.py](src/adjustments.py). All multipliers live in [config/adjustment_profiles.yaml](config/adjustment_profiles.yaml) — no magic numbers in code.

### 6.1 How the chain runs

```
raw external value
      │
      ▼
Stage 1 — definition_alignment   (shared; keyed by source_type)
      │
      ▼
Stage 2 — institution-specific
      ├── BankAdjustment             : peer_mix  × geography_ig
      └── PrivateCreditAdjustment    : selection_bias × LVR × industry
                                       × trading_history × unsecured
                                       × invoice concentration overlay
      │
      ▼
adjusted value (→ triangulation → calibration)
```

Stage 1 and Stage 2 multipliers compose multiplicatively. The `final_multiplier` you see in the Board report §1 flagship line is the product of every step that fired.

### 6.2 Stage 1 — Definition alignment (shared)

Only fires when the `source_type` matches one of the rules below. If no rule matches (e.g. `pillar3`, `apra_adi` PD entries, `listed_peer`), Stage 1 is a no-op.

| Rule key                          | Fires for source_type    | Default multiplier | Purpose                                           |
|-----------------------------------|--------------------------|--------------------|---------------------------------------------------|
| `apra_impaired_to_pd`             | `apra_adi` (impaired-ratio rows) | **1.50**       | APRA 90+ DPD / impaired ratio → PD equivalent (APG 113) |
| `illion_bfri_to_default_rate`     | `bureau`                 | **1.40** (1.30–1.50) | illion BFRI index → default rate                 |
| `rating_agency_global_to_au_ig`   | `rating_agency` (IG)     | **0.85** (0.80–0.90) | Global IG rating → AU IG                         |
| `rating_agency_global_to_au_sub_ig` | `rating_agency` (sub-IG) | **1.00**          | Global sub-IG → AU sub-IG (no adjustment)        |

Source-reference rationale is stored alongside each rule and emitted in the Technical Appendix audit trail.

### 6.3 Stage 2 — Bank chain

Two multipliers. Both always available; `geography_ig` only fires when the source is a global rating agency.

| Step          | Range        | Default | When it fires                                           |
|---------------|--------------|---------|---------------------------------------------------------|
| `peer_mix`    | 0.95 – 1.05  | **1.00** | Always — adjusts for portfolio-mix differences vs the benchmark basket |
| `geography_ig`| 0.80 – 0.90  | **0.85** | Only when source is `rating_agency` (global → AU IG)    |

The default `peer_mix = 1.00` is why the board report shows the bank output roughly equal to the raw Pillar 3 value — bank-to-bank the mix is similar, so no uplift.

### 6.4 Stage 2 — Private Credit chain

Driven by the **product** passed in (not the asset_class). The product key is matched against [config/adjustment_profiles.yaml](config/adjustment_profiles.yaml) → `private_credit_stage2`. Current products and their default multipliers:

| Product                    | selection_bias | LVR     | industry   | other                |
|----------------------------|----------------|---------|------------|----------------------|
| `bridging_residential`     | 1.75 (1.5–2.0) | 1.15    | —          | —                    |
| `bridging_commercial`      | **2.00** (1.8–2.5) | **1.20** | —     | —                    |
| `development`              | 2.25 (2.0–2.5) | — (slotting) | —    | —                    |
| `residual_stock`           | 1.75           | 1.15    | —          | —                    |
| `trade_finance`            | 1.35           | —       | 1.10       | —                    |
| `invoice_finance`          | 1.40           | —       | —          | concentration overlay |
| `working_capital_secured`  | 2.00           | —       | 1.00 (ANZSIC-dependent) | —       |
| `working_capital_unsecured`| 2.00           | —       | 1.00       | `unsecured` = 1.30   |

Plus cross-product multipliers that fire when the caller passes the kwarg:

| Multiplier                      | Range        | Default | Trigger                                              |
|---------------------------------|--------------|---------|------------------------------------------------------|
| `trading_history_adj`           | 1.05 – 1.15  | 1.10    | Borrower with <3 years trading history               |
| `invoice_concentration_overlay` | 1.00 – 1.40  | 1.10    | Debtor concentration share — bucketed into 4 tiers   |
| LGD: `weaker_guarantor`         | 1.05 – 1.15  | 1.10    | PC counterparty weaker than bank equivalent          |
| LGD: `smaller_workout_team`     | 1.05 – 1.10  | 1.075   | Smaller recovery function                            |
| LGD: `subordinated`             | 1.30 – 1.80  | 1.55    | Position behind senior bank debt                     |
| LGD: `higher_lvr`               | 1.10 – 1.25  | 1.175   | Above-market LVR at origination                      |
| LGD: `sector_concentration`     | 1.05 – 1.15  | 1.10    | Concentration in a single ANZSIC sector              |

The **flagship Board-report figure** (2.15x PC/Bank ratio on CBA CRE PD) comes from `selection_bias × lvr × trading_history` = 1.70 × 1.15 × 1.10 ≈ 2.15 hard-coded in [reports/benchmark_report.py](reports/benchmark_report.py) `_build_bank_vs_pc_comparison`.

### 6.5 Where each multiplier is sourced from

All ranges trace back to the **Bank External Benchmarking Framework** (`project guidence.md` §4) and supporting regulatory references:

| Multiplier family          | Source document                                          |
|----------------------------|----------------------------------------------------------|
| `apra_impaired_to_pd`      | APRA APG 113 *Capital Adequacy: Credit Risk*              |
| `illion_bfri_*`            | illion BFRI methodology whitepaper                        |
| `rating_agency_*`          | S&P / Moody's sovereign and corporate methodology        |
| `peer_mix`, `geography_ig` | Framework §4 (bank stage-2 table)                         |
| `selection_bias`, `lvr`, `industry`, product table | Framework §4 (private-credit stage-2 table) |
| `trading_history_adj`, `invoice_concentration_overlay` | Framework §4 (additional PC adjustments)  |
| LGD-specific (`subordinated`, `higher_lvr`, etc.) | Framework §4 + MRC calibration committee minutes |

Each row in [config/adjustment_profiles.yaml](config/adjustment_profiles.yaml) carries a `source_reference` string that flows through to the audit trail — **do not remove this field** when editing.

### 6.6 How to update the config

All tuning is done in [config/adjustment_profiles.yaml](config/adjustment_profiles.yaml). No code changes required. Typical changes:

**a) Change a default multiplier for an existing step** (e.g. raise PC `bridging_commercial` selection_bias from 2.00 to 2.10):

```yaml
bridging_commercial:
  selection_bias:  {min: 1.8, max: 2.5, default: 2.10}   # was 2.00
```

Keep `default` between `min` and `max`. Stage 1 single-multiplier entries (e.g. `apra_impaired_to_pd`) use `multiplier:` instead of a min/max/default tuple.

**b) Widen or narrow a range** (e.g. peer_mix band):

```yaml
bank_stage2:
  peer_mix:
    min: 0.90           # was 0.95
    max: 1.10           # was 1.05
    default: 1.00
```

The engine only enforces that `what_if` overrides land within `[min, max]` in tests — the `default` is what runs in production.

**c) Add a new private-credit product**:

```yaml
private_credit_stage2:
  new_product_name:
    selection_bias:  {min: 1.5, max: 2.0, default: 1.70}
    lvr:             {min: 1.10, max: 1.20, default: 1.15}
    # industry key is optional
```

Then pass `product="new_product_name"` when calling `AdjustmentEngine.adjust()`.

**d) Adjust the invoice concentration tiers**:

```yaml
invoice_concentration_overlay:
  below_10pct: 1.00
  10_to_25pct: 1.10
  25_to_50pct: 1.25
  above_50pct: 1.40
  default_when_absent: 1.10
```

The bucket keys are hard-coded in [src/adjustments.py](src/adjustments.py) `_select_concentration` — if you rename them, update both files.

**e) Change the stale-source refresh thresholds** (separate file):

Edit [config/refresh_schedules.yaml](config/refresh_schedules.yaml) — values are days. E.g. to tighten Pillar 3 from quarterly + 30-day grace (120 days) to quarterly only (90 days):

```yaml
refresh_schedules:
  pillar3:       90     # was 120
```

### 6.7 Governance / sign-off for config changes

Any change to [config/adjustment_profiles.yaml](config/adjustment_profiles.yaml) **must**:

1. Be tested: `pytest tests/test_adjustments.py -v` (119 tests guard the multiplier math).
2. Regenerate the board report and re-check the flagship PC/Bank ratio — it's the most sensitive figure to adjustment changes.
3. Be logged in the MRC decision register with the old value, new value, rationale, and supporting document reference.
4. If a range (min/max) changes, update the `source_reference` field to cite the decision paper or framework revision.

Do not edit `source_reference` or `rationale` fields without a corresponding MRC decision — they are the audit-trail evidence shown in the Technical Appendix §3.

---

## 7. Common failures and fixes

| Symptom                                              | Likely cause / fix                                                  |
|------------------------------------------------------|---------------------------------------------------------------------|
| `Pillar3Downloader: no matching anchor` for a bank   | The bank moved the disclosure page. Update the `BANKS` dict in [scripts/download_sources/pillar3_downloader.py](scripts/download_sources/pillar3_downloader.py) (header comment keeps a change-log). |
| `APRA scraper found no Series anchor`                | APRA changed the publication naming. Inspect [scripts/download_sources/apra_downloader.py](scripts/download_sources/apra_downloader.py) keyword list. |
| `ICC Trade Register` flagged unavailable             | Expected — ICC is paywalled since 2025. Either skip or manually purchase and ingest via `python cli.py ingest icc --path <pdf>`. |
| NAB LGD ≈ 0.x% vs peers at 30–50%                    | Decimal scaling issue in NAB PDF adapter. See §5.1 above.           |
| `floor_triggered = True` on every segment            | `adjustment_profiles.yaml` floor values may be set too high. Review with MRC. |
| Board report has empty peer tables                   | `pillar3` ingestion didn't find PD/LGD rows — check `ingest status` and re-run with `--force-refresh`. |

Cache management:

```bash
python cli.py cache status        # Show cached files per source
python cli.py cache clear pillar3 # Force re-download on next run
```

---

## 7. Repository layout

```
external_benchmark_engine/
├── cli.py                        # Top-level Click CLI — start here
├── config/
│   ├── adjustment_profiles.yaml  # Bank vs private-credit multipliers
│   └── refresh_schedules.yaml    # Stale-source thresholds per source_type
├── data/                         # Raw downloaded files (cached, git-ignored)
│   ├── raw/ (pillar3/, apra/)
│   ├── asic/
│   └── abs/
├── scripts/download_sources/     # Scrapers — one per publisher
├── ingestion/                    # Parsers that turn raw files into registry rows
│   ├── adapters/                 # One adapter per PDF/XLSX format
│   └── pillar3/                  # Per-bank Pillar 3 adapters
├── src/                          # Core engine (registry, adjustments, triangulation, calibration, governance)
├── reports/
│   ├── benchmark_report.py       # Report 1 — Board + Technical markdown
│   └── docx_helpers.py
├── outputs/reports/              # Generated board & technical reports
├── tests/
└── benchmarks.db                 # SQLite registry (created on first ingest)
```

---

## 8. Escalation

- **Model Risk Committee** — quality flags, calibration method changes, floor overrides.
- **Data engineering** — broken scrapers, adapter failures, cache corruption.
- **Owner of [project guidence.md](project%20guidence.md)** — scope, roadmap, deferred reports (Reports 2 & 3).

---

_Last updated: see `git log README.md`._
