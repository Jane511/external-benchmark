"""Pick random Pillar 3 rows to spot-check against source PDFs.

Prints 5 randomly-selected extracted values with enough context
that you can find them in the source PDF.
"""

import sqlite3

c = sqlite3.connect('benchmarks.db')

rows = list(c.execute("""
    SELECT publisher, source_id, asset_class, data_type, value, notes
    FROM benchmarks
    WHERE superseded_by IS NULL
      AND source_type LIKE 'pillar3%'
    ORDER BY RANDOM()
    LIMIT 5
"""))

print()
print("=" * 75)
print("SPOT-CHECK SAMPLE — 5 random Pillar 3 values to verify against PDFs")
print("=" * 75)
print()

for i, r in enumerate(rows, 1):
    publisher, source_id, asset_class, data_type, value, notes = r

    # Extract source page from notes
    page = 'n/a'
    pd_band = 'n/a'
    for token in (notes or '').split(';'):
        token = token.strip()
        if token.startswith('source_page='):
            page = token.split('=', 1)[1].strip()
        elif token.startswith('pd_band='):
            pd_band = token.split('=', 1)[1].strip()

    # Format value as percentage if < 1
    if value < 1:
        pct = f"{value * 100:.2f}%"
    else:
        pct = f"{value:.4f}"

    print(f"SAMPLE {i}")
    print(f"  Bank:        {publisher}")
    print(f"  PDF to open: data/raw/pillar3/{publisher}_*_Pillar3_Annual.pdf")
    print(f"  Go to page:  {page}")
    print(f"  Find table:  CR6 — IRB credit risk exposures by portfolio and PD range")
    print(f"  Asset class: {asset_class}")
    print(f"  PD band:     {pd_band}")
    print(f"  Metric:      {data_type.upper()}")
    print(f"  ENGINE SAYS: {pct}")
    print(f"  source_id:   {source_id}")
    print()

print("=" * 75)
print("HOW TO VERIFY")
print("=" * 75)
print("""
For each sample above:

1. Open the PDF listed (File Explorer → data/raw/pillar3/ → double-click)
2. Jump to the page number shown (Ctrl+Shift+N in most PDF viewers)
3. Find the CR6 table on that page
4. Find the row matching the asset class + PD band
5. Read the value in the metric column (PD or LGD)
6. Compare to what ENGINE SAYS

If they match within 0.01%: write down "match"
If they differ by more than that: write down both values and which PDF

Open a text file in outputs/ called 'spot_check_verification.md' and record:

  Sample 1: Bank=ANZ  asset=X  band=Y  metric=LGD  engine=32.00%  pdf=32.00%  MATCH
  Sample 2: ...
""")