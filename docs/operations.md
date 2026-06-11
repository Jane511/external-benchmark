# Operations guide

Operational detail for running and maintaining the engine. The
[README](../README.md) covers what the project is and how to produce a
report; this file covers the day-to-day plumbing.

## Refreshing source files each cycle

Before rebuilding the database, refresh the raw source files. Use the
cadence column in the [source table](../README.md#data-sources) to decide
which downloaders to run — most users run the whole batch once a quarter.

```bash
python src/download_sources/pillar3_downloader.py
python src/download_sources/apra_downloader.py
python src/download_sources/rba_downloader.py --target all
python src/download_sources/non_bank_downloader.py
python src/download_sources/external_indices_downloader.py --index sp_spin
python src/download_sources/governance_publications_downloader.py
```

When a downloader cannot reach a source it writes a `_MANUAL.md` note in
the per-source folder with the URL and a manual-fetch instruction. The
ingest pipeline treats "no input" as a valid outcome — it never fabricates
observations.

Run `python cli.py report stale` before any committee report to flag
sources that are overdue against the thresholds in
`config/refresh_schedules.yaml`.

## Manual downloads

Some lender investor-relations portals are JS-rendered or bot-protected,
so the downloader cannot fetch them automatically. Drop the PDFs into the
folder shown below (the adapter matches on folder + glob, not filename).

| Lender | Download from | Drop into |
| --- | --- | --- |
| Pepper Money | `peppermoney.com.au/about/shareholders` (or search "Pepper Money annual report") | `data/raw/non_bank/pepper/` |
| La Trobe Financial | `latrobefinancial.com.au/investments/forms-library/` (Credit Fund Annual Report + Investment Snapshot & Metrics) | `data/raw/non_bank/latrobe/` |
| Judo Bank | Search "Judo Bank annual report" — IR page is Cloudflare-protected. Judo is an ADI, so it lives under `other_bank/`. | `data/raw/other_bank/judo/` |
| Qualitas | Search "Qualitas annual report" — IR portal is JS-rendered | `data/raw/non_bank/qualitas/` |
| Liberty Financial | `lfgroup.com.au/reports/annual-reports` | `data/raw/non_bank/liberty/` |
| Resimac | `resimac.com.au/investors/annual-reports` | `data/raw/non_bank/resimac/` |
| Latitude Financial | `investors.latitudefinancial.com.au` (latest results presentation) | `data/raw/non_bank/latitude/` |
| humm Group | `shareholders.hummgroup.com.au/Investors/` (latest results presentation) | `data/raw/non_bank/humm/` |
| Zip Co | `zip.co/au/investors` (latest results presentation) | `data/raw/non_bank/zip/` |

Once a real PDF lands in the folder, the `_MANUAL.md` note clears on the
next successful run.

## Migrations

The raw-observation migration is part of the standard setup flow (it is
idempotent — running it twice does not duplicate rows):

```bash
python src/migrate_to_raw_observations.py --db benchmarks.db
```

## Cache management

```bash
python cli.py cache status
python cli.py cache clear --source pillar3 --yes
```

## Command reference

```text
# Database setup
python cli.py [--db PATH] seed
python src/migrate_to_raw_observations.py [--db PATH]

# Ingest
python cli.py ingest pillar3 [cba|nab|wbc|anz|mqg] [--reporting-date YYYY-MM-DD]
python cli.py ingest apra
python cli.py ingest status

# CSV exports
python cli.py [--db PATH] export-csvs [--out-dir DIR] [--raw-dir DIR]

# Reports
python cli.py report stale | quality | coverage | annual        (governance subreports)
python cli.py report benchmark --format docx|html|markdown \
    [--output PATH] [--period-label "Q1 2026"] [--source-type X]

# Read-only queries
python cli.py list [--source-type X]
python cli.py history SOURCE_ID
python cli.py observations [--segment X] [--big4-only|--nonbank-only]
python cli.py export [--format json|csv] [--output PATH]
```

## Troubleshooting

| Symptom | What to do |
| --- | --- |
| `Pillar3Downloader: no matching anchor` | The bank moved their disclosure page. Update the `BANKS` dict in `src/download_sources/pillar3_downloader.py`. |
| `APRA scraper found no Series anchor` | APRA renamed a publication. Inspect the keyword list in `src/download_sources/apra_downloader.py`. |
| `_MANUAL.md` written for a lender | The IR page is bot-protected. Open the URL in the note, download by hand, drop the file in the folder shown above. |
| Migration reports rows skipped | Skipped rows are legacy entries with no `data_definition_class`. Inspect `source_id` and extend `_infer_definition_class` if it should migrate. |
| `ModuleNotFoundError: No module named 'src'` | Run from the repo root via `python cli.py ...`. |
