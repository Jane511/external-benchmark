"""
run_all.py — Single entry point to generate all three report formats.

Usage:
    python run_all.py

Output:
    ./output/Report_Q3_2025_v2.docx   (MRC-ready Word document)
    ./output/Report_Q3_2025_v2.md     (Markdown for git/wiki)
    ./output/Report_Q3_2025_v2.html   (browser-viewable)

Requirements:
    pip install python-docx

That's it. Markdown and HTML use only Python standard library.
"""

import os
import sys

# Make sure we can import the sibling modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def main():
    # Create output directory relative to this script's location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, "output")
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 60)
    print("External Benchmark Engine — Report 1 Generation")
    print("=" * 60)
    print()

    # -------------------- DOCX --------------------
    print("[1/3] Generating DOCX...")
    try:
        from build_report import build_report
        docx_path = os.path.join(output_dir, "Report_Q3_2025_v2.docx")
        build_report(
            output_path=docx_path,
            reporting_period="Q3 2025",
            institution_type="bank",
        )
        size_kb = os.path.getsize(docx_path) / 1024
        print(f"      ✓ {docx_path}  ({size_kb:.1f} KB)")
    except ImportError as e:
        print(f"      ✗ Skipped — missing dependency: {e}")
        print(f"        Run: pip install python-docx")
    except Exception as e:
        print(f"      ✗ Error: {e}")

    # -------------------- Markdown --------------------
    print("[2/3] Generating Markdown...")
    try:
        from build_markdown import build_markdown
        md_path = os.path.join(output_dir, "Report_Q3_2025_v2.md")
        content = build_markdown()
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(content)
        size_kb = os.path.getsize(md_path) / 1024
        print(f"      ✓ {md_path}  ({size_kb:.1f} KB)")
    except Exception as e:
        print(f"      ✗ Error: {e}")

    # -------------------- HTML --------------------
    print("[3/3] Generating HTML...")
    try:
        from build_html import build_html
        html_path = os.path.join(output_dir, "Report_Q3_2025_v2.html")
        content = build_html()
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(content)
        size_kb = os.path.getsize(html_path) / 1024
        print(f"      ✓ {html_path}  ({size_kb:.1f} KB)")
    except Exception as e:
        print(f"      ✗ Error: {e}")

    print()
    print("=" * 60)
    print(f"Done. Open {output_dir} to view the reports.")
    print("=" * 60)


if __name__ == "__main__":
    main()
