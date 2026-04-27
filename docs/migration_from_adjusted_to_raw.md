# Migration: Adjusted-PD Engine Output → Raw Observation Engine Output

**Audience:** downstream consumers of this engine — primarily the PD
workbook, plus the LGD project and stress-testing project once they
land. This is the companion doc to Brief 1 of 3 (engine refactor).

---

## TL;DR

- The engine **no longer applies adjustments**. No definition alignment,
  no institution adjustments, no LGD overlays, no triangulation. Every
  PD/LGD value the engine emits is the source-published raw figure.
- The new public read API is
  [`src.observations.PeerObservations.for_segment()`](../src/observations.py).
  It returns an `ObservationSet` with a list of `RawObservation` records
  plus a `ValidationFlags` summary (spread, outliers, vintage) — but
  **no consensus value**.
- The old `CalibrationFeed.for_central_tendency()` /
  `for_logistic_recalibration()` / `for_bayesian_blending()` /
  `for_external_blending()` / `for_pluto_tasche()` methods raise on
  import. So do `AdjustmentEngine` and `BenchmarkTriangulator`.
- Adjustments now live in the consuming project. For PD, that's the
  Thin Data Workbook's `compute_lra_per_product()` (Brief 2), which
  uses an EBA Margin-of-Conservatism formula
  `best_estimate + MoC_A + MoC_B + MoC_C` to absorb what was previously
  Stage 1 + Stage 2.

---

## Why this changed

The engine answers exactly one question:

> **What did each external source publish for this segment, in this
> period?**

It deliberately does NOT answer:

- "What's the consensus benchmark?" → triangulation. Removed.
- "What does this source mean once we align to Basel definitions?" → Stage 1
  definition alignment. Moved to the consuming project.
- "How do we adjust for our portfolio's selection bias?" → Stage 2
  institution adjustments. Moved to the consuming project.
- "What's the LGD overlay for downturn?" → moved to the LGD project.

Adjustments are use-case-specific (the PD workbook adjusts differently
from LGD or stress testing) and benefit from sitting in **one place per
use case** so each model can manage its own complete adjustment chain
coherently. Audit traceability also improves: a reviewer asking
*"where does the 5× total uplift on raw CBA come from?"* gets one
answer in the PD workbook, not a chain across two projects.

Regulatory backing:

- **EBA/GL/2017/16** specifies that MoC is quantified at the level of
  the institution's calibration segment — i.e. by the **consumer** of
  the data, not by the data source.
- **APRA APG 113 paragraph 78** says *"where only limited data is
  available… APRA expects the ADI to add a greater margin of
  conservatism to its PD estimates"* — conservatism is the ADI's
  responsibility, not the data source's.

---

## Migrating a consumer

### Before (deprecated)

```python
from src.adjustments import AdjustmentEngine
from src.calibration_feed import CalibrationFeed
from src.triangulation import BenchmarkTriangulator
from src.models import InstitutionType
from src.registry import BenchmarkRegistry

registry = BenchmarkRegistry(engine, actor="pd_workbook")
adjuster = AdjustmentEngine(InstitutionType.BANK, engine)
triangulator = BenchmarkTriangulator(InstitutionType.BANK)
feed = CalibrationFeed(registry, adjuster, triangulator)

ct = feed.for_central_tendency("commercial_property_investment")
external_lra = ct.external_lra        # already adjusted, already triangulated
```

### After (Brief 1)

```python
from src.observations import PeerObservations
from src.registry import BenchmarkRegistry

registry = BenchmarkRegistry(engine, actor="pd_workbook")
peer = PeerObservations(registry)

obs_set = peer.for_segment("commercial_property_investment")
raw_observations = obs_set.observations          # list[RawObservation]
flags = obs_set.validation_flags                 # ValidationFlags

# The PD workbook decides how to combine the raw observations and how
# to apply MoC (Brief 2). For example:
import statistics
best_estimate = statistics.median(o.value for o in raw_observations)
moc_a = ...     # definition-alignment MoC (was Stage 1)
moc_b = ...     # selection-bias / book-vs-peer MoC (was Stage 2)
moc_c = ...     # remaining estimation-error MoC
external_lra = best_estimate + moc_a + moc_b + moc_c
```

The engine surfaces flags so the consumer can decide whether the spread
across sources is wide enough to warrant a higher MoC Category C
allocation:

```python
if flags.spread_pct and flags.spread_pct > 0.50:
    moc_c += 0.005   # add 50 bps for high cross-source dispersion
```

---

## Flagship example — same final number, different attribution

Before Brief 1, the headline number was:

> CBA CRE adjusted PD = 5.38%
> = 2.50% raw × Stage 1 × Stage 2 chain

After Brief 1, the same final number is:

> CBA CRE raw PD = 2.50% (engine output)
> + PD workbook applies MoC Category B ≈ 2.88% additive
> = 5.38% total LRA contribution

The engine no longer multiplies by anything — it publishes the 2.50%
verbatim with `source_id="cba"`, `as_of_date="2025-...", `
`reporting_basis="Pillar 3 trailing 4-quarter average"`,
`methodology_note="CR6 EAD-weighted Average PD"`, and the
PD-band / page reference. The PD workbook applies its EBA MoC framework
and produces the 5.38% LRA target with full accountability for each
component.

---

## Where the removed adjustments now live

| Removed engine knob               | New home                                                                         |
|-----------------------------------|----------------------------------------------------------------------------------|
| `apra_impaired_to_pd` (1.50)      | PD workbook MoC Category A (definition alignment)                                |
| `illion_bfri_to_default_rate`     | PD workbook MoC Category A                                                       |
| `rating_agency_global_to_au_*`    | PD workbook MoC Category A                                                       |
| `bank_stage2.peer_mix`            | PD workbook MoC Category B (selection bias / book-vs-peer)                       |
| `bank_stage2.geography_ig`        | PD workbook MoC Category A                                                       |
| `private_credit_stage2.*`         | PD workbook (PC variant) MoC Category B                                          |
| `trading_history_adj`             | PD workbook MoC Category B                                                       |
| `invoice_concentration_overlay`   | PD workbook MoC Category B                                                       |
| `lgd_specific.*`                  | LGD project (when built)                                                         |
| `BenchmarkTriangulator`           | PD workbook chooses its own combination — typically median + MoC                 |

All of these are referenced in **Brief 2** (PD workbook side); see that
brief for the EBA MoC formulas and category boundaries.

---

## Backwards compatibility during transition

- The legacy `BenchmarkEntry` / `Benchmark` table persists in the SQLite
  DB. `BenchmarkRegistry.list()` / `get_by_segment()` / `export()` still
  read from it.
- The new `RawObservation` / `raw_observations` table is written by the
  new ingest path. `BenchmarkRegistry.add_observation()`,
  `add_observations()`, `query_observations()`, `list_segments()` are
  the Brief-1 read/write methods.
- Run `python scripts/migrate_to_raw_observations.py --db <path>` once
  to back-fill `raw_observations` from `benchmarks`.
- After Brief 2 ships and the PD workbook has migrated, the legacy
  `CentralTendencyOutput` / `BayesianBlendingOutput` / etc. classes can
  be removed in a follow-up cleanup. They are still exported from
  `src.models` for now so existing code that imports them keeps loading.

---

## Out of scope for this migration

- Reality-check methods against published non-bank loss data,
  historical default-rate floor, APRA system-wide benchmarks — these
  go into the PD workbook (**Brief 3**), not the engine.
- ABS labour-force / CPI / macro overlays — these go into the
  `industry-analysis` project, not the engine. The engine's
  `ingestion/industry_context.py` is unchanged.
