# External Benchmark Engine

Publishes a **library of raw, source-attributable PD / LGD observations**
for Australian credit — Big 4 Pillar 3, non-bank ASX-listed lenders
(Judo, Liberty, Pepper, Resimac, MoneyMe, Plenti, Wisr, Qualitas,
Metrics Credit), APRA, RBA Financial Stability Review, and
S&P SPIN RMBS arrears — and emits a per-period **raw-only
benchmark report** that downstream consumers (PD workbook, LGD project,
stress testing) read from.

> **The engine publishes raw, source-attributable observations only.**
> No definition alignment, institution adjustments, downturn LGD
> overlays, or cross-source triangulation. All adjustment logic moved
> to consuming projects so each use case can manage its own complete
> adjustment chain coherently. See
> [`docs/migration_from_adjusted_to_raw.md`](docs/migration_from_adjusted_to_raw.md)
> for the migration guide.

---

## 1. Deliverables (per period)

**Reports** (in `outputs/reports/`):

- `Report_<period>.md` — raw-only Markdown report (committee-friendly, git-reviewable)
- `Report_<period>.html` — single-file browser view (inline CSS, no JS)
- `Report_<period>.docx` — Word version (requires `[reports]` extras)

**Machine-readable CSV bundle** (in `outputs/csv/`, emitted by `python cli.py export-csvs`):

- `raw_observations.csv` — one row per observation with `source_id`, `source_type`, `is_big4`, `segment`, `parameter`, `data_definition_class`, `value`, `as_of_date`, vintage, methodology, URL. **This is the contract for downstream PD / LGD / ECL projects and BI dashboards.**
- `validation_flags.csv` — per-segment cross-source flags (spread, outliers, vintage, Big 4 vs non-bank ratio).
- `reality_check_bands.csv` — per-product upper / lower band table flattened from [`config/reality_check_bands.yaml`](config/reality_check_bands.yaml).
- `raw_data_inventory.csv` — manifest of every file currently staged in `data/raw/` (size, modified time, family, kind). Lets dashboards show "what raw publications are on disk?".

**Configuration** (committed):

- [`config/reality_check_bands.yaml`](config/reality_check_bands.yaml) — per-product upper / lower bands and justifying source IDs; loaded by downstream consumers via [`src.reality_check.load_reality_check_bands()`](src/reality_check.py).

**Quick runbook — regenerate all outputs:**

```bash
# 1. Refresh raw files. Use the cadence table in section 2 to decide
# which of these are due this cycle.
python scripts/download_sources/pillar3_downloader.py
python scripts/download_sources/apra_downloader.py
python scripts/download_sources/rba_downloader.py --target all
python scripts/download_sources/non_bank_downloader.py
python scripts/download_sources/external_indices_downloader.py --index sp_spin

# 2. Stage any manual files called out by _MANUAL.md or *_GATE.md notes,
# then ingest/migrate into raw_observations.
python cli.py --db benchmarks.db ingest pillar3 --reporting-date 2026-03-31
python cli.py --db benchmarks.db ingest apra
python scripts/migrate_to_raw_observations.py --db benchmarks.db

# 3. Produce the machine-readable CSV bundle.
python cli.py --db benchmarks.db export-csvs --out-dir outputs/csv --raw-dir data/raw

# 4. Produce all report formats.
python cli.py --db benchmarks.db report benchmark --format markdown --period-label "Q1 2026" --output outputs/reports/Report_Q1_2026.md
python cli.py --db benchmarks.db report benchmark --format html --period-label "Q1 2026" --output outputs/reports/Report_Q1_2026.html
python cli.py --db benchmarks.db report benchmark --format docx --period-label "Q1 2026" --output outputs/reports/Report_Q1_2026.docx
```

Run `python cli.py --db benchmarks.db seed` only when initialising a new
empty database; the migration command is idempotent and safe to re-run.

**Report sections (six):**

1. **Executive summary** — count of segments, observations, sources (Big 4 vs non-bank / aggregate split).
2. **Per-source raw observations by segment** — every published value with `source_id`, `source_type`, `parameter`, `data_definition_class`, vintage, methodology footnote, page/table reference.
3. **Cross-source validation summary** — spread, outliers, stale-vintage flags. No consensus / triangulated value.
4. **Big 4 vs non-bank disclosure spread** — medians per cohort plus the ratio. **Informational only**; the engine recommends no uplift.
5. **Provenance & methodology footnotes** — one line per source with URL + reporting basis.
6. **Raw data inventory** — every file in `data/raw/` grouped by source family, including `_MANUAL.md` notes for sources that need manual download.

Each `RawObservation` carries a `data_definition_class` (Basel PD,
arrears 30+/90+, impaired ratio, NPL ratio, loss expense, realised
loss, regulatory floor, qualitative commentary) so consumers can filter
by definition family without parsing methodology notes.

---

## 2. Source coverage — full inventory

Every external data source the engine knows about, the downloader that
fetches it, the publisher's natural release cadence, and which
`data_definition_class`(es) it produces in `raw_observations`.

| # | Source | Family | Downloader command | Cadence | Definition class(es) | Tier |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | CBA Pillar 3 (annual + quarterly) | `pillar3/` | `python scripts/download_sources/pillar3_downloader.py --bank cba` | Half-yearly + quarterly supplement | `basel_pd_one_year` | Automated |
| 2 | NAB Pillar 3 | `pillar3/` | `python scripts/download_sources/pillar3_downloader.py --bank nab` | Half-yearly | `basel_pd_one_year` | Automated |
| 3 | WBC Pillar 3 | `pillar3/` | `python scripts/download_sources/pillar3_downloader.py --bank wbc` | Half-yearly | `basel_pd_one_year` | Automated |
| 4 | ANZ Pillar 3 | `pillar3/` | `python scripts/download_sources/pillar3_downloader.py --bank anz` | Half-yearly | `basel_pd_one_year` | Automated |
| 5 | APRA Quarterly ADI Performance | `apra/` | `python scripts/download_sources/apra_downloader.py` | Quarterly (~30 days post quarter-end) | `npl_ratio` | Automated |
| 6 | APRA Quarterly Property Exposures (QPEX) | `apra/` | (same — `apra_downloader.py` fetches both) | Quarterly | `impaired_loans_ratio` | Automated |
| 7 | RBA Financial Stability Review | `rba/` | `python scripts/download_sources/rba_downloader.py --target fsr` | Semi-annual (March + September/October) | `arrears_30_plus_days`, `arrears_90_plus_days` | Automated |
| 8 | RBA Securitisation system | `rba/` | `python scripts/download_sources/rba_downloader.py --target securitisation` | Continuous (gated dataset); landing-page snapshot only | (none until user agreement signed) | Snapshot + gate note (signed RBA user agreement required for raw data) |
| 9 | Pepper Money (debt-investors) | `non_bank/pepper/` | `python scripts/download_sources/non_bank_downloader.py --lender pepper` | Half-yearly | `loss_expense_rate`, `arrears_90_plus_days`, `arrears_30_plus_days` | **Automated + parsed** |
| 10 | Judo Bank Pillar 3 | `non_bank/judo/` | `python scripts/download_sources/non_bank_downloader.py --lender judo` | Quarterly + half-yearly | `basel_pd_one_year` | **Automated + parsed** (`/regulatory-disclosures`) |
| 11 | Liberty Financial annual | `non_bank/liberty/` | `python scripts/download_sources/non_bank_downloader.py --lender liberty` | Annual + half-yearly | `impaired_loans_ratio` | **Automated + parsed** (`lfgroup.com.au`) |
| 12 | Resimac half-yearly | `non_bank/resimac/` | `python scripts/download_sources/non_bank_downloader.py --lender resimac` | Half-yearly + annual | `arrears_90_plus_days` (parser TODO) | URL fixed; manual workflow until parser lands |
| 13 | MoneyMe investor centre | `non_bank/moneyme/` | `python scripts/download_sources/non_bank_downloader.py --lender moneyme` | Half-yearly | (TBD when parsed) | URL fixed (`investors.moneyme.com.au`); manual workflow until parser lands |
| 14 | Plenti quarterly trading update | `non_bank/plenti/` | `python scripts/download_sources/non_bank_downloader.py --lender plenti` | Quarterly + half-yearly + annual | `loss_expense_rate`, `arrears_90_plus_days` | **Automated + parsed** (`/shareholders`) |
| 15 | Wisr investor centre | `non_bank/wisr/` | `python scripts/download_sources/non_bank_downloader.py --lender wisr` | Quarterly | (TBD when parsed) | URL fixed (`investorhub.wisr.com.au`); manual workflow until parser lands |
| 16 | Qualitas (ASX:QAL) | `non_bank/qualitas/` | `python scripts/download_sources/non_bank_downloader.py --lender qualitas` | Half-yearly + monthly QRI | `qualitative_commentary` | Automated (`investors.qualitas.com.au`); commentary-only by design |
| 17 | Metrics Credit Partners (MREIF) | `non_bank/metrics_credit/` | `python scripts/download_sources/non_bank_downloader.py --lender metrics_credit` | Monthly + half-yearly | `qualitative_commentary` | Automated (`/listed-funds/`); commentary-only by design |
| 18 | S&P SPIN (AU RMBS) | `external_indices/sp_spin/` | `python scripts/download_sources/external_indices_downloader.py --index sp_spin` | Monthly; staged quarterly | `arrears_30_plus_days` | Manual download, **parsed when staged** |

**Cadence guidance — when to re-download:**

- **Quarterly (Feb / May / Aug / Nov)** — Big 4 Pillar 3 (CBA quarterly), APRA Performance, APRA QPEX. Run `pillar3_downloader.py` and `apra_downloader.py` ~30 days after each quarter-end (when APRA publishes).
- **Semi-annual (March + September/October)** — Big 4 Pillar 3 H1 / FY PDFs (NAB, WBC, ANZ, CBA H1/FY); RBA FSR. Re-run `pillar3_downloader.py` and `rba_downloader.py --target fsr`.
- **Monthly / quarterly staging** — Pepper Money, Metrics Credit Partners, and S&P SPIN. Run `non_bank_downloader.py` monthly; stage one SPIN PDF per quarter and re-run `scripts/migrate_to_raw_observations.py`.
- **One-off after publisher URL drift** — non-bank ASX `_MANUAL.md` lenders. Check the IR pages every quarter; when the IR layout stabilises, lift the URL into the per-lender config in [`non_bank_downloader.py`](scripts/download_sources/non_bank_downloader.py).

**Refresh-staleness thresholds** are enforced by [`src/governance.py`](src/governance.py) reading [`config/refresh_schedules.yaml`](config/refresh_schedules.yaml). Run `python cli.py report stale` before any committee report to flag overdue sources.

The engine emits raw observations only; downstream consumers decide how
to combine, align, or weight them.

---

## 3. Quarterly workflow (end-to-end)

Total wall-clock time ≈ 15–30 min, depending on scraper hits.

### Step 1 — Download raw source files

```bash
# Big 4 Pillar 3 disclosures (PDFs + CBA XLSX)            -> data/raw/pillar3/
python scripts/download_sources/pillar3_downloader.py

# APRA ADI quarterly statistics (XLSX)                    -> data/raw/apra/
python scripts/download_sources/apra_downloader.py

# RBA Financial Stability Review (latest PDF)             -> data/raw/rba/
# + Securitisation system landing snapshot + gate note
python scripts/download_sources/rba_downloader.py

# Non-bank ASX-listed lender disclosures (best-effort)    -> data/raw/non_bank/<lender>/
# Pepper Money typically OK; the rest are bot-protected
# or DNS-gated and emit a per-lender _MANUAL.md note.
python scripts/download_sources/non_bank_downloader.py

# External rating-agency RMBS indices (best-effort)       -> data/raw/external_indices/
# S&P SPIN requires manual PDF staging; downloader maintains _MANUAL.md.
python scripts/download_sources/external_indices_downloader.py
```

#### Source-by-source automation tier

| Source family | Downloader | Tier |
| --- | --- | --- |
| Big 4 Pillar 3 (CBA, NAB, WBC, ANZ) | `pillar3_downloader.py` | **Automated** |
| APRA ADI Performance + QPEX | `apra_downloader.py` | **Automated** |
| RBA FSR (latest PDF) | `rba_downloader.py --target fsr` | **Automated** |
| RBA Securitisation system | `rba_downloader.py --target securitisation` | Snapshot + gate note (signed user agreement required for raw data) |
| Pepper Money | `non_bank_downloader.py --lender pepper` | **Automated + parsed** (debt-investors page) |
| Judo / Liberty / Plenti | `non_bank_downloader.py` | **Automated + parsed** after the URL-fix brief landed |
| Qualitas / Metrics Credit | `non_bank_downloader.py` | Automated; commentary-only by design (no published numbers) |
| Resimac / MoneyMe / Wisr | `non_bank_downloader.py` | URL fixed; per-source `_MANUAL.md` describes the staging workflow until the per-adapter parser lands in a follow-up brief |
| S&P SPIN | `external_indices_downloader.py` | Manual PDF staging; parser emits prime + non-conforming 30+ DPD observations |

When a downloader can't fetch a source it writes a `_MANUAL.md` note
into the per-source folder pointing the analyst at the right URL. The
adapters accept "no input" as a valid outcome (per
[`ingestion/adapters/base.py`](ingestion/adapters/base.py)) and emit an
empty frame — no fabricated observations.

### Step 2 — Seed and ingest into the registry

```bash
# Initialise (or reset) the SQLite DB with the canonical seed entries
python cli.py --db benchmarks.db seed

# Ingest live publisher files (re-run per source family)
python cli.py --db benchmarks.db ingest pillar3 --reporting-date 2026-03-31
python cli.py --db benchmarks.db ingest apra

# Migrate legacy BenchmarkEntry rows -> raw_observations
# (idempotent; safe to re-run)
python scripts/migrate_to_raw_observations.py --db benchmarks.db

# Sanity-check what landed
python cli.py --db benchmarks.db ingest status
python cli.py --db benchmarks.db list --limit 20
python cli.py --db benchmarks.db observations --segment commercial_property
```

### Step 3 — Export the CSV bundle (input for PD project + dashboards)

```bash
# Writes 4 files into outputs/csv/:
#   raw_observations.csv      raw_data_inventory.csv
#   validation_flags.csv      reality_check_bands.csv
python cli.py --db benchmarks.db export-csvs
```

Override the destinations if needed:

```bash
python cli.py --db benchmarks.db export-csvs \
    --out-dir /path/to/pd_project/inputs/external_benchmark/ \
    --raw-dir data/raw
```

These CSVs are the **stable contract** between this engine and downstream
consumers. They are deterministic — same DB content → byte-identical
CSVs. Schema:

- **`raw_observations.csv`** — `source_id, source_type, is_big4, segment, product, parameter, data_definition_class, value, as_of_date, reporting_basis, methodology_note, sample_size_n, period_start, period_end, source_url, page_or_table_ref`. One row per observation. `value` is a decimal in `[0, 1]` (commentary rows = `0.0`).
- **`validation_flags.csv`** — `segment, n_sources, spread_pct, big4_spread_pct, bank_vs_nonbank_ratio, outlier_sources, stale_sources`. `outlier_sources` and `stale_sources` are pipe-separated source-IDs.
- **`reality_check_bands.csv`** — `product, lower_band_pd, upper_band_pd, lower_sources, upper_sources, rationale, last_review_date, next_review_due`. Pipe-separated source lists; `rationale` is multi-line markdown flattened with `\n`.
- **`raw_data_inventory.csv`** — `source_family, subfamily, filename, relative_path, size_bytes, modified_utc, kind`. Walks `data/raw/` and lists every staged file (PDF / XLSX / CSV / `_MANUAL.md` / `*_GATE.md`).

Recommended downstream wiring:

```python
import pandas as pd

obs = pd.read_csv("outputs/csv/raw_observations.csv", parse_dates=["as_of_date"])
basel_pd_only = obs[obs["data_definition_class"] == "basel_pd_one_year"]

bands = pd.read_csv("outputs/csv/reality_check_bands.csv")
upper = bands.set_index("product")["upper_band_pd"].to_dict()
```

### Step 4 — Generate the committee report

```bash
# Markdown — committee-friendly, git-reviewable
python cli.py --db benchmarks.db report benchmark \
    --format markdown --period-label "Q1 2026"

# HTML — single self-contained file (inline CSS, no JS)
python cli.py --db benchmarks.db report benchmark \
    --format html --period-label "Q1 2026"

# DOCX — Word format (requires [reports] extras)
python cli.py --db benchmarks.db report benchmark \
    --format docx --period-label "Q1 2026"
```

Default output paths: `outputs/reports/Report_<period>.{md,html,docx}`.
Pass `--output` to override. `--source-type` narrows the report to a
single source family (e.g. `--source-type bank_pillar3` or
`--source-type rating_agency_index`).

The report has six sections (see §1). Section 6 ("Raw data inventory")
walks `data/raw/` and lists every staged file by source family — so the
committee can see at a glance which non-bank disclosures were fetched
versus which still need a manual download.

### Step 5 — Reality-check the calibrated values (downstream)

The engine itself does not enforce reality-check bounds. The PD / LGD /
ECL projects load
[`config/reality_check_bands.yaml`](config/reality_check_bands.yaml) via
[`src/reality_check.py`](src/reality_check.py) and decide what to do
with values that fall outside an upper or lower band:

```python
from src.reality_check import load_reality_check_bands

library = load_reality_check_bands()
band = library.for_product("commercial_property")
# band.upper_band_pd, band.lower_band_pd, band.upper_sources,
# band.lower_sources, band.rationale
```

The library carries `last_review_date` and `next_review_due` so
consumers can warn when bands are stale.

### Step 6 — Spot-check before sign-off

Pick 5 random observations and verify against source PDFs:

```bash
python scripts/pick_spot_checks.py
# Record findings in outputs/spot_check_verification.md
```

---

## 4. First-time setup

```bash
# From repo root
python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # macOS / Linux

pip install -e ".[ingestion,download,reports]"
```

Required Python: **3.9+**. The SQLite database `benchmarks.db` is
created on first `seed` / `ingest`.

Smoke-test the install:

```bash
python cli.py --help
python cli.py list --help
python -m pytest --no-header -q
```

---

## 5. CLI surface

```text
# Core
python cli.py [--db PATH] seed | list | history | export | observations

# Downloaders (one per source family — see §2 for the full table)
python scripts/download_sources/pillar3_downloader.py            [--bank cba|nab|wbc|anz|all]
python scripts/download_sources/apra_downloader.py
python scripts/download_sources/rba_downloader.py                [--target fsr|securitisation|all]
python scripts/download_sources/non_bank_downloader.py           [--lender pepper|judo|...|all]
python scripts/download_sources/external_indices_downloader.py   [--index sp_spin|all] [--dry-run]

# Ingest
python cli.py ingest pillar3 [cba|nab|wbc|anz] [--reporting-date YYYY-MM-DD]
python cli.py ingest apra
python cli.py ingest status

# Cache
python cli.py cache status | clear [...]

# CSV exports (machine-readable contract for downstream projects)
python cli.py [--db PATH] export-csvs [--out-dir DIR] [--raw-dir DIR]

# Reports
python cli.py report stale | quality | coverage | peer | annual    (governance)
python cli.py report benchmark --format docx|html|markdown \
    [--output PATH] [--period-label "Q1 2026"] [--source-type X]

# Migration (one-shot — populates raw_observations from legacy benchmarks)
python scripts/migrate_to_raw_observations.py [--db PATH]
```

There is no `report environment` or `report combined` subcommand — the
engine no longer pulls from the `industry-analysis` sibling project,
and reports surface only what the engine itself ingests.

---

## 6. Repository layout

```text
external_benchmark_engine/
├── cli.py                              # Top-level Click CLI — start here
├── config/
│   ├── reality_check_bands.yaml        # Per-product upper / lower PD bands
│   └── refresh_schedules.yaml          # Stale-source thresholds per source_type
├── data/                               # Raw downloaded files (cached, git-ignored)
│   └── raw/pillar3/, raw/apra/, raw/rba/, raw/non_bank/, raw/external_indices/
├── scripts/download_sources/           # Downloaders — one per publisher / family
├── ingestion/
│   ├── adapters/                       # Per-publisher PDF/XLSX adapters
│   ├── pillar3/                        # Per-bank Pillar 3 entry points
│   ├── external_indices/               # S&P SPIN and RBA aggregate adapters
│   └── segment_mapping.yaml            # Canonical segment IDs + per-source aliases
├── src/                                # Core engine — all Python lives here
│   ├── models.py                       # RawObservation, DataDefinitionClass, etc.
│   ├── db.py                           # SQLAlchemy schema; ALTER for new column
│   ├── registry.py                     # add_observation(), query_observations()
│   ├── observations.py                 # PeerObservations facade
│   ├── validation.py                   # Cross-source spread / outlier / vintage flags
│   ├── reality_check.py                # Reality-check band loader
│   ├── seed_data.py                    # Canonical reality-check seed entries
│   ├── governance.py                   # Stale / quality / coverage / peer / annual reports
│   ├── benchmark_report.py             # Raw-only report — Markdown + HTML + DOCX
│   ├── csv_exporter.py                 # CSV bundle for downstream / dashboards
│   └── docx_helpers.py                 # Shared python-docx primitives
├── outputs/
│   ├── reports/                        # Report_<period>.{md,html,docx}
│   └── csv/                            # raw_observations / validation_flags /
│                                       # reality_check_bands / raw_data_inventory
├── tests/                              # Full coverage of the raw-only path
└── benchmarks.db                       # SQLite registry (created on first ingest)
```

---

## 7. Common failures and fixes

| Symptom | Likely cause / fix |
| --- | --- |
| `Pillar3Downloader: no matching anchor` for a bank | The bank moved the disclosure page. Update the `BANKS` dict in [`pillar3_downloader.py`](scripts/download_sources/pillar3_downloader.py) (header comment keeps a change-log). |
| `APRA scraper found no Series anchor` | APRA changed the publication naming. Inspect [`apra_downloader.py`](scripts/download_sources/apra_downloader.py) keyword list. |
| Non-bank lender `_MANUAL.md` written | IR page is bot-protected or DNS-gated. Follow the hint in the per-lender `_MANUAL.md` and drop the file by hand into `data/raw/non_bank/<lender>/`. |
| S&P SPIN `_MANUAL.md` written | The public release URL is generated per article. Drop the PDF into `data/raw/external_indices/sp_spin/` and re-run `scripts/migrate_to_raw_observations.py`. |
| Migration script reports rows skipped | Skipped rows are non-PD / non-LGD legacy entries that don't map to a `data_definition_class`. Inspect the row's `source_id` and add a pattern to `_infer_definition_class` if it should be migrated. |
| Spread / outlier flags fire across every segment | Vintage drift. Refresh staler sources before publishing. |
| Tests fail with `ModuleNotFoundError: No module named 'src'` when running a script | Use `python cli.py …` or run the script with `python scripts/<file>.py` (project root is auto-bootstrapped). Don't run `python -c "from scripts…"` outside the repo root. |

Cache management:

```bash
python cli.py cache status
python cli.py cache clear pillar3
```

---

## 8. Architectural principle

The engine answers exactly one question:

> **What did each external source publish for this segment, in this
> period, under what definition?**

It deliberately does NOT answer:

- *"What's the consensus benchmark across sources?"* → triangulation; lives in the consuming project.
- *"What does this source mean once aligned to Basel definitions?"* → definition alignment; lives in the consuming project.
- *"Should our LRA be capped at this level?"* → calibration decision; lives in the consuming project.
- *"What does the macro / industry environment look like?"* → industry overlay; **removed entirely** from this engine.

Heterogeneity is a **feature, not a bug**. Different sources publish
different things — Pepper publishes asset-finance arrears, Liberty
publishes impaired-loan ratios, Judo publishes proper Basel PDs. The
engine surfaces these definitions explicitly via
`data_definition_class`. Consumers (PD, LGD, ECL projects) decide how
to align them.

---

## 9. Escalation

- **Model Risk Committee** — quality flags, governance reports, calibration changes downstream.
- **Data engineering** — broken scrapers, adapter failures, cache corruption.
- **Owner of the brief documents** — scope, roadmap.

---

_Last updated: see `git log README.md`._
