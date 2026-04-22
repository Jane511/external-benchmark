"""Purge all Pillar 3 rows, then re-ingest from cached PDFs using the fixed adapter.

Run only after any adapter fix has been applied to
ingestion/adapters/cba_pillar3_pdf_adapter.py and all tests pass.

IMPORTANT: must be run from the project root so that the relative paths
(``benchmarks.db`` and ``data/raw/pillar3/*``) resolve correctly:

    cd <project-root>
    python scripts/cleanup_and_reingest.py
"""

import sqlite3
import subprocess
import sys

DB = 'benchmarks.db'

# ---------------------------------------------------------------
# Step 1 — count what we're about to delete
# ---------------------------------------------------------------
c = sqlite3.connect(DB)

print("Current Pillar 3 entries by publisher (before purge):")
print("-" * 60)
q = """
SELECT publisher, COUNT(*) as cnt
FROM benchmarks
WHERE source_type IN ('pillar3', 'pillar3_annual', 'pillar3_quarterly')
   OR url LIKE '%pillar%'
   OR notes LIKE '%pillar3%'
GROUP BY publisher
ORDER BY publisher
"""
total = 0
for row in c.execute(q):
    print(f"  {row[0]:<8} {row[1]:>6}")
    total += row[1]
print(f"  {'TOTAL':<8} {total:>6}")

confirm = input("\nProceed to purge and re-ingest? [y/N]: ")
if confirm.lower() != 'y':
    print("Aborted.")
    sys.exit(0)

# ---------------------------------------------------------------
# Step 2 — purge Pillar 3 rows
# ---------------------------------------------------------------
print("\nPurging Pillar 3 rows...")
c.execute("""
    DELETE FROM benchmarks
    WHERE source_type IN ('pillar3', 'pillar3_annual', 'pillar3_quarterly')
       OR notes LIKE '%pillar3%'
""")
c.commit()
print(f"Deleted {c.total_changes} rows.")
c.close()

# ---------------------------------------------------------------
# Step 3 — re-ingest all four banks
# ---------------------------------------------------------------
# Re-ingest from cached PDFs. The web URLs in the adapters have drifted
# (NAB/ANZ 404; CBA download page returns a non-XLSX wrapper), so we use
# --source-path to point at the PDFs in data/raw/pillar3/.
BANKS = [
    ('cba', 'data/raw/pillar3/CBA_FY2025_Pillar3_Annual.pdf',       '2025-06-30'),
    ('nab', 'data/raw/pillar3/NAB_FY2025_Pillar3_Annual.pdf',       '2025-09-30'),
    ('wbc', 'data/raw/pillar3/WBC_FY2025_Pillar3_Annual.pdf',       '2025-09-30'),
    ('anz', 'data/raw/pillar3/ANZ_FY2025_Pillar3_Annual.pdf',       '2025-09-30'),
]
for bank, path, rdate in BANKS:
    print(f"\n--- Re-ingesting {bank.upper()} from {path} ---")
    result = subprocess.run(
        ['python', 'cli.py', 'ingest', 'pillar3', bank,
         '--source-path', path, '--reporting-date', rdate],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(f"WARNING: {bank} ingestion returned code {result.returncode}")
        print(result.stderr)

# ---------------------------------------------------------------
# Step 4 — verify
# ---------------------------------------------------------------
c = sqlite3.connect(DB)
print("\n" + "=" * 60)
print("After re-ingest — LGD entries with value < 5%")
print("(expect empty or sovereign-only; NO corporate_general/retail_sme):")
print("=" * 60)
q = """
SELECT publisher, asset_class, COUNT(*) as cnt
FROM benchmarks
WHERE data_type='lgd' AND value < 0.05
GROUP BY publisher, asset_class
ORDER BY publisher, asset_class
"""
rows = list(c.execute(q))
if not rows:
    print("  None — all LGD values >= 5%. Clean.")
else:
    for row in rows:
        print(f"  {row[0]:<6} {row[1]:<24} {row[2]:>4}  (review each case)")

print("\nTotal entries by publisher (after re-ingest):")
print("-" * 40)
for row in c.execute("SELECT publisher, COUNT(*) FROM benchmarks GROUP BY publisher ORDER BY publisher"):
    print(f"  {row[0]:<10} {row[1]:>6}")

print("\nDuplicate source_id check (should be empty):")
print("-" * 40)
q = """
SELECT source_id, COUNT(*) as cnt
FROM benchmarks
WHERE notes LIKE '%pillar3%'
GROUP BY source_id
HAVING COUNT(*) > 1
"""
dupes = list(c.execute(q))
if not dupes:
    print("  None — no duplicate source_ids across versions.")
else:
    for row in dupes:
        print(f"  {row[0]} — {row[1]} versions")
