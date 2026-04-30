# External Benchmark Engine

## What it does

This project collects credit-risk numbers that Australian lenders and
regulators publish — Big 4 bank Pillar 3 reports, APRA quarterly stats,
RBA Financial Stability Review, S&P RMBS arrears, and the disclosures of
ASX-listed non-bank lenders — and packages them into a single tidy
report and a small set of CSV files that the rest of your modelling
projects can read.

It does **not** blend, average, or adjust the numbers. Each row is a
single source's published figure, tagged with where it came from, when,
and what the source says it means. Adjustments (definition alignment,
downturn overlays, peer triangulation) are deliberately left to the
projects that consume this output — that way each consumer manages its
own complete adjustment chain.

> The engine answers one question: *"What did each external source
> publish for this segment, in this period, under what definition?"*
> Anything beyond that — what the consensus is, what the right LRA cap
> should be — is the consuming project's call.

## What you get each cycle

Three rendered reports, in `outputs/reports/`:

- `Report_<period>.md` — Markdown, easy to read in GitHub or email.
- `Report_<period>.html` — single self-contained file, opens in any browser.
- `Report_<period>.docx` — Word version, the one a committee usually reads.

Six CSV files for downstream projects, in `outputs/csv/`:

| File | What's in it |
| --- | --- |
| `raw_observations.csv` | One row per published figure. The contract for downstream PD / LGD / ECL projects. |
| `validation_flags.csv` | Per-segment summary: spread, outliers, stale sources, peer-Big4-vs-non-bank ratio. |
| `validation_flag_sources.csv` | Same flags in long form — one row per (segment, flag type, source) so spreadsheets handle it cleanly. |
| `segment_trend.csv` | Latest vs prior vintage per source — only fires when at least two vintages exist. |
| `reality_check_bands.csv` | Per-product upper/lower bands and the rationale, flattened from `config/reality_check_bands.yaml`. |
| `raw_data_inventory.csv` | Every file currently staged on disk under `data/raw/`. |

The report itself has eight sections:

0. **Segment definitions** — one-line glossary of every segment used in the report.
1. **Executive summary** — short prose paragraph plus headline counts.
2. **Per-source observations by segment** — the actual numbers, with reporting basis and methodology footnote on every row.
3. **Cross-source validation summary** — spread, outliers, stale-vintage flags. No consensus value.
4. **Big 4 vs non-bank disclosure spread** — peer-only medians and the ratio. Informational; no recommendation.
4a. **Reference anchors** — regulator aggregates (APRA, RBA), rating-agency indices (S&P SPIN), regulatory floors (APS 113), Macquarie. Listed separately so they don't pollute peer arithmetic.
5. **Provenance & methodology footnotes** — one line per source with URL.
6. **Raw data inventory** — every file staged under `data/raw/`, including manual-download notes.
7. **Trend vs prior cycle** — current-vs-prior delta per source, where two vintages exist.

## How to run it

End-to-end, from a fresh clone:

```bash
# 1. Install
python -m venv .venv
.venv\Scripts\activate          # Windows; on macOS/Linux use: source .venv/bin/activate
pip install -e ".[ingestion,download,reports]"

# 2. Build the database from seed data and migrations
python cli.py --db benchmarks.db seed
python scripts/migrate_to_raw_observations.py --db benchmarks.db

# 3. Generate the CSV bundle
python cli.py --db benchmarks.db export-csvs

# 4. Generate the three report formats
python cli.py --db benchmarks.db report benchmark --format markdown --period-label "Q1 2026"
python cli.py --db benchmarks.db report benchmark --format html     --period-label "Q1 2026"
python cli.py --db benchmarks.db report benchmark --format docx     --period-label "Q1 2026"
```

Each cycle, before step 2, refresh the source files. Use the cadence
column in the source table below to decide which downloaders need to
run — most people run the whole batch once a quarter:

```bash
python scripts/download_sources/pillar3_downloader.py
python scripts/download_sources/apra_downloader.py
python scripts/download_sources/rba_downloader.py --target all
python scripts/download_sources/non_bank_downloader.py
python scripts/download_sources/external_indices_downloader.py --index sp_spin
python scripts/download_sources/governance_publications_downloader.py
```

The migration script is idempotent — running it twice doesn't duplicate
rows. The `seed` command is only needed on a fresh empty database.

## Sources tracked

| Source | Folder | Downloader | Cadence | Tier |
| --- | --- | --- | --- | --- |
| CBA Pillar 3 (annual + quarterly) | `pillar3/` | `pillar3_downloader.py --bank cba` | Half-yearly + quarterly | Automatic |
| NAB Pillar 3 | `pillar3/` | `pillar3_downloader.py --bank nab` | Half-yearly | Automatic |
| WBC Pillar 3 | `pillar3/` | `pillar3_downloader.py --bank wbc` | Half-yearly | Automatic |
| ANZ Pillar 3 | `pillar3/` | `pillar3_downloader.py --bank anz` | Half-yearly | Automatic |
| Macquarie Pillar 3 | `pillar3/` | `pillar3_downloader.py --bank mqg` | Half-yearly | Automatic |
| APRA Quarterly ADI Performance | `apra/` | `apra_downloader.py` | Quarterly | Automatic |
| APRA Quarterly Property Exposures (QPEX) | `apra/` | `apra_downloader.py` (same) | Quarterly | Automatic |
| APRA Insight | `apra/insight/` | `governance_publications_downloader.py` | Irregular | Automatic, newest-first manifest |
| Council of Financial Regulators publications | `cfr/` | `governance_publications_downloader.py` | Irregular | Automatic, newest-first manifest |
| RBA Financial Stability Review | `rba/` | `rba_downloader.py --target rba_fsr` | Semi-annual | Automatic |
| RBA Statement on Monetary Policy | `rba/` | `rba_downloader.py --target rba_smp` | Quarterly | Automatic |
| RBA Chart Pack | `rba/` | `rba_downloader.py --target rba_chart_pack` | Quarterly | Automatic |
| RBA Securitisation system | `rba/` | `rba_downloader.py --target securitisation` | Continuous | Snapshot + gate note (signed user agreement required for raw data) |
| Pepper Money | `non_bank/pepper/` | `non_bank_downloader.py --lender pepper` | Half-yearly | Automatic + parsed |
| Judo Bank | `non_bank/judo/` | `non_bank_downloader.py --lender judo` | Quarterly + half-yearly | Automatic + parsed |
| Liberty Financial | `non_bank/liberty/` | `non_bank_downloader.py --lender liberty` | Annual + half-yearly | Automatic + parsed |
| Plenti | `non_bank/plenti/` | `non_bank_downloader.py --lender plenti` | Quarterly + half-yearly | Automatic + parsed |
| Resimac | `non_bank/resimac/` | `non_bank_downloader.py --lender resimac` | Half-yearly | URL fixed; manual until parser lands |
| MoneyMe | `non_bank/moneyme/` | `non_bank_downloader.py --lender moneyme` | Half-yearly | URL fixed; manual until parser lands |
| Wisr | `non_bank/wisr/` | `non_bank_downloader.py --lender wisr` | Quarterly | URL fixed; manual until parser lands |
| Qualitas (ASX:QAL) | `non_bank/qualitas/` | `non_bank_downloader.py --lender qualitas` | Half-yearly + monthly QRI | Automatic; commentary-only by design |
| Metrics Credit Partners (MREIF) | `non_bank/metrics_credit/` | `non_bank_downloader.py --lender metrics_credit` | Monthly + half-yearly | Automatic; commentary-only by design |
| S&P SPIN (Australian RMBS arrears) | `external_indices/sp_spin/` | `external_indices_downloader.py --index sp_spin` | Monthly; staged quarterly | Manual download, parsed when staged |

When a downloader can't reach a source it writes a `_MANUAL.md` note in
the per-source folder with the URL and a manual fetch instruction. The
ingest pipeline treats "no input" as a valid outcome — no fabricated
observations, ever.

Per-source-type cadence thresholds live in
`config/refresh_schedules.yaml`. Run
`python cli.py report stale` before any committee report to flag
overdue sources.

## How observations are tagged

Every row in `raw_observations.csv` carries three labels that consumers
filter on:

**`parameter`** — what kind of metric. One of: `pd`, `lgd`, `arrears`,
`impaired`, `npl`, `loss_rate`, `commentary`. Commentary rows have no
numeric value (the published narrative goes in `methodology_note`).

**`data_definition_class`** — the precise definition the source uses,
because different publishers measure different things:

- `basel_pd_one_year` — Basel-aligned 12-month PD (Big 4 Pillar 3, Judo).
- `arrears_30_plus_days`, `arrears_90_plus_days` — loans past due (S&P SPIN, APRA QPEX, Pepper, RBA FSR).
- `impaired_loans_ratio` — loans flagged as impaired (Liberty, APRA QPEX).
- `npl_ratio` — non-performing loans (APRA quarterly performance).
- `loss_expense_rate`, `realised_loss_rate` — P&L-driven vs charge-offs (Pepper asset finance, La Trobe).
- `regulatory_floor_pd`, `regulatory_floor_lgd` — APRA APS 113 slotting grades and minimum floors.
- `qualitative_commentary` — narrative-only sources (Qualitas, Metrics).

**`cohort`** — peer grouping, used by the validation arithmetic:

- `peer_big4` — CBA, NAB, WBC, ANZ.
- `peer_other_major_bank` — Macquarie. APRA classifies them as a major bank but they aren't Big 4, so they sit in their own bucket and don't distort either Big-4 or non-bank medians.
- `peer_non_bank` — ASX-listed non-bank lenders (Judo, Liberty, Pepper, Plenti, Wisr, MoneyMe, Resimac, Qualitas, Metrics, La Trobe).
- `regulator_aggregate` — APRA, RBA aggregates.
- `rating_agency` — S&P, Moody's indices.
- `regulatory_floor` — APS 113 PD/LGD slotting + floors.
- `industry_body` — AFIA, illion BFRI.

Outlier detection and the `peer_big4_vs_non_bank_ratio` look only at
`peer_big4` and `peer_non_bank`. Macquarie and the four reference cohorts
appear separately under "Reference anchors" in the report so a reader
can see them without them poisoning the peer numbers.

## CSV schemas

The CSVs are the stable contract between this engine and downstream
consumers. Same database content gives byte-identical CSVs.

`raw_observations.csv`:
> `source_id, source_type, cohort, is_big4, segment, product, parameter,
> data_definition_class, value, as_of_date, reporting_basis,
> methodology_note, sample_size_n, period_start, period_end, source_url,
> page_or_table_ref`

`value` is a decimal in `[0, 1]`, or empty for commentary rows.

`validation_flags.csv`:
> `segment, n_sources, spread_decimal, big4_spread_decimal,
> peer_big4_vs_non_bank_ratio, outlier_sources, stale_sources,
> frozen_dataset_banner`

The first line is a `# units:` comment row that documents the
decimal-vs-percent convention and the precise definition of the peer
ratio. Spreadsheet importers should be told to skip lines starting with
`#`. Source lists are deduped and pipe-separated.

`validation_flag_sources.csv`:
> `segment, flag_type, source_id` — long-form companion. Easier to pivot
> in Excel than parsing pipe-delimited cells.

`reality_check_bands.csv`:
> `product, lower_band_pd, upper_band_pd, lower_sources, upper_sources,
> rationale, last_review_date, next_review_due` — pipe-separated source
> lists; multi-line rationales flattened with `\n`.

`raw_data_inventory.csv`:
> `source_family, subfamily, filename, relative_path, size_bytes,
> modified_utc, kind` — walks `data/raw/` and lists every file (PDF,
> XLSX, CSV, `_MANUAL.md`, `*_GATE.md`).

`segment_trend.csv`:
> `segment, parameter, source_id, current_value, current_as_of,
> prior_value, prior_as_of, delta, pct_change` — only emits rows where
> the same source has at least two vintages for the same (segment,
> parameter).

Recommended pandas wiring downstream:

```python
import pandas as pd

obs = pd.read_csv("outputs/csv/raw_observations.csv", parse_dates=["as_of_date"])
basel_pds = obs[obs["data_definition_class"] == "basel_pd_one_year"]

# Skip the units comment row when reading validation_flags.csv
flags = pd.read_csv("outputs/csv/validation_flags.csv", comment="#")

bands = pd.read_csv("outputs/csv/reality_check_bands.csv")
upper = bands.set_index("product")["upper_band_pd"].to_dict()
```

## Command reference

```text
# Database setup
python cli.py [--db PATH] seed
python scripts/migrate_to_raw_observations.py [--db PATH]

# One-off data migrations (run on existing DBs after upgrade)
python scripts/migrate_collapse_cre_segments.py --db benchmarks.db [--apply]
python scripts/migrate_commentary_values_to_null.py --db benchmarks.db [--apply]
python scripts/migrate_definition_class_consistency.py --db benchmarks.db [--apply]

# Downloaders
python scripts/download_sources/pillar3_downloader.py            [--bank cba|nab|wbc|anz|mqg|all]
python scripts/download_sources/apra_downloader.py
python scripts/download_sources/rba_downloader.py                [--target rba_fsr|rba_smp|rba_chart_pack|all]
python scripts/download_sources/non_bank_downloader.py           [--lender pepper|judo|...|all]
python scripts/download_sources/external_indices_downloader.py   [--index sp_spin|all]
python scripts/download_sources/governance_publications_downloader.py [--target apra_insight|cfr_publications|all]

# Ingest
python cli.py ingest pillar3 [cba|nab|wbc|anz|mqg] [--reporting-date YYYY-MM-DD]
python cli.py ingest apra
python cli.py ingest status

# Cache
python cli.py cache status
python cli.py cache clear [--source FAMILY] [--yes]

# CSV exports
python cli.py [--db PATH] export-csvs [--out-dir DIR] [--raw-dir DIR]

# Reports
python cli.py report stale | quality | coverage | annual    (governance subreports)
python cli.py report benchmark --format docx|html|markdown \
    [--output PATH] [--period-label "Q1 2026"] [--source-type X]

# Read-only queries
python cli.py list [--source-type X] [--limit N]
python cli.py history SOURCE_ID
python cli.py observations [--segment X] [--big4-only|--nonbank-only]
python cli.py export [--format json|csv] [--output PATH]
```

## Repository layout

```text
external_benchmark_engine/
├── cli.py                              # Top-level Click CLI — start here
├── config/
│   ├── reality_check_bands.yaml        # Per-product upper / lower PD bands
│   └── refresh_schedules.yaml          # Stale-source thresholds per source_type
├── data/                               # Raw downloaded files (cached, git-ignored)
│   └── raw/                            # apra/, cfr/, external_indices/, non_bank/, pillar3/, rba/
├── scripts/download_sources/           # One downloader per publisher / family
├── ingestion/
│   ├── adapters/                       # Per-publisher PDF/XLSX/HTML adapters
│   ├── pillar3/                        # Per-bank Pillar 3 entry points
│   ├── external_indices/               # S&P SPIN and RBA aggregate adapters
│   └── source_registry.py              # Catalog of every source URL + cache layout
├── src/                                # Core engine — all Python lives here
│   ├── models.py                       # RawObservation, Cohort, DataDefinitionClass
│   ├── db.py                           # SQLAlchemy schema
│   ├── registry.py                     # add / supersede / query (with audit trail)
│   ├── observations.py                 # PeerObservations facade
│   ├── validation.py                   # Spread / outlier / vintage / peer ratio
│   ├── trend.py                        # Current-vs-prior trend rows
│   ├── reality_check.py                # Reality-check band loader
│   ├── seed_data.py                    # Canonical Australian seed entries
│   ├── segment_glossary.py             # One-line definition per canonical segment
│   ├── governance.py                   # Stale / quality / coverage / annual reports
│   ├── benchmark_report.py             # Markdown + HTML + DOCX renderer
│   ├── csv_exporter.py                 # CSV bundle for downstream consumers
│   └── docx_helpers.py                 # python-docx primitives
├── outputs/
│   ├── reports/                        # Report_<period>.{md,html,docx}
│   └── csv/                            # The six CSVs above
├── tests/                              # 451 tests covering the raw-only path
└── benchmarks.db                       # SQLite registry (created on first ingest)
```

## Troubleshooting

| Symptom | What to do |
| --- | --- |
| `Pillar3Downloader: no matching anchor` for a bank | The bank moved their disclosure page. Update the `BANKS` dict in `scripts/download_sources/pillar3_downloader.py` (the file's header keeps a change-log of past URL moves). |
| `APRA scraper found no Series anchor` | APRA renamed their publication. Inspect the keyword list in `scripts/download_sources/apra_downloader.py`. |
| Non-bank lender `_MANUAL.md` written to disk | The IR page is bot-protected or DNS-gated. Open the URL in `_MANUAL.md`, download by hand, drop the file in `data/raw/non_bank/<lender>/`. |
| S&P SPIN `_MANUAL.md` written | The S&P release URL is generated per article. Download manually, drop into `data/raw/external_indices/sp_spin/`, re-run `migrate_to_raw_observations.py`. |
| Migration script reports rows skipped | Skipped rows are legacy entries that don't map to a `data_definition_class`. Inspect the row's `source_id` and add a pattern to `_infer_definition_class` if it should be migrated. |
| Tests fail with `ModuleNotFoundError: No module named 'src'` | Run from the repo root via `python cli.py ...` or `python scripts/<file>.py`. Don't run `python -c "from scripts..."` outside the repo root. |
| Old DB still shows commentary `value=0.0` | Run `python scripts/migrate_commentary_values_to_null.py --db benchmarks.db --apply`. |
| Old DB shows `regulatory_floor_pd` on LGD rows | Run `python scripts/migrate_definition_class_consistency.py --db benchmarks.db --apply`. |
| Old DB has a duplicate `commercial_property_investment` segment | Run `python scripts/migrate_collapse_cre_segments.py --db benchmarks.db --apply`. |

Cache management:

```bash
python cli.py cache status
python cli.py cache clear --source pillar3 --yes
```

## The one rule

The engine answers exactly one question: *"What did each external
source publish for this segment, in this period, under what
definition?"*

It deliberately does **not** answer:

- *What's the consensus benchmark across sources?* → triangulation; consuming project's call.
- *What does this source mean once aligned to Basel definitions?* → definition alignment; consuming project's call.
- *Should our LRA be capped at this level?* → calibration decision; consuming project's call.

Heterogeneity is a feature, not a bug. Different sources publish
different things — Pepper publishes asset-finance arrears, Liberty
publishes impaired-loan ratios, Judo publishes Basel PDs. The engine
labels these differences explicitly via `data_definition_class` and
`cohort`. Consumers (PD, LGD, ECL projects) decide how to align them.

## Contact

- **Model Risk Committee** — quality flags, governance reports, calibration changes downstream.
- **Data engineering** — broken scrapers, adapter failures, cache corruption.
- **Owner of the brief documents** — scope, roadmap.

---

_Last updated: see `git log README.md`._
