"""Catalog of upstream source URLs + cache conventions.

Consumed by:
  - Scrapers (for `_fetch_via_cache` — discovers the URL and filename pattern)
  - CacheManager (for `cache status` — one row per distinct subdir)
  - CLI (for validating `--source-key` values)

Each entry declares:
  cache_dir         full subdirectory path under the repo root
  description       human-readable label for CLI output
  manual_download   True if the file can't be fetched automatically (paid/gated);
                    user drops the file into cache_dir manually
  files             list of {name, url, filename_pattern} tuples describing each
                    physical file that belongs to this source

`filename_pattern` uses str.format() placeholders — scrapers fill them in from
reporting-date context (quarter, year, half).
"""
from __future__ import annotations


SOURCE_URLS: dict[str, dict] = {
    # -------------------------------------------------------------------
    "apra_adi": {
        "cache_dir": "data/raw/apra/",
        "description": "APRA Quarterly ADI Statistics (Performance + Property Exposures)",
        "manual_download": False,
        "files": [
            {
                "name": "ADI Performance",
                "url": (
                    "https://www.apra.gov.au/"
                    "quarterly-authorised-deposit-taking-institution-statistics"
                ),
                "filename_pattern": "ADI_Performance_{quarter}_{year}.xlsx",
            },
            {
                "name": "Property Exposures",
                "url": (
                    "https://www.apra.gov.au/"
                    "quarterly-authorised-deposit-taking-institution-statistics"
                ),
                "filename_pattern": "ADI_Property_Exposures_{quarter}_{year}.xlsx",
            },
        ],
    },

    # -------------------------------------------------------------------
    "cba_pillar3": {
        "cache_dir": "data/raw/pillar3/",
        "description": "CBA Pillar 3 quantitative supplement (Excel companion)",
        "manual_download": False,
        "files": [
            {
                "name": "Quantitative Supplement",
                "url": (
                    "https://www.commbank.com.au/about-us/investors/"
                    "regulatory-disclosure/pillar-3-capital-disclosures.html"
                ),
                "filename_pattern": "CBA_{half}_{year}_Pillar3_Quantitative.xlsx",
            },
        ],
    },

    # -------------------------------------------------------------------
    "nab_pillar3": {
        "cache_dir": "data/raw/pillar3/",
        "description": "NAB Pillar 3 disclosure (PDF)",
        "manual_download": False,
        "files": [
            {
                "name": "Pillar 3 Report",
                "url": (
                    "https://www.nab.com.au/about-us/shareholder-centre/"
                    "financial-disclosures/pillar-3-disclosures"
                ),
                "filename_pattern": "NAB_{half}_{year}_Pillar3.pdf",
            },
        ],
    },

    # -------------------------------------------------------------------
    "wbc_pillar3": {
        "cache_dir": "data/raw/pillar3/",
        "description": "Westpac Pillar 3 disclosure (PDF)",
        "manual_download": False,
        "files": [
            {
                "name": "Pillar 3 Report",
                "url": (
                    "https://www.westpac.com.au/about-westpac/investor-centre/"
                    "financial-information/pillar-3-disclosures/"
                ),
                "filename_pattern": "WBC_{half}_{year}_Pillar3.pdf",
            },
        ],
    },

    # -------------------------------------------------------------------
    "anz_pillar3": {
        "cache_dir": "data/raw/pillar3/",
        "description": "ANZ Pillar 3 disclosure (PDF)",
        "manual_download": False,
        "files": [
            {
                "name": "Pillar 3 Report",
                "url": (
                    "https://www.anz.com.au/shareholder/centre/reporting/"
                    "pillar-3-disclosure/"
                ),
                "filename_pattern": "ANZ_{half}_{year}_Pillar3.pdf",
            },
        ],
    },

    # -------------------------------------------------------------------
    "mqg_pillar3": {
        "cache_dir": "data/raw/pillar3/",
        "description": "Macquarie Bank Pillar 3 disclosure (PDF)",
        "manual_download": False,
        "files": [
            {
                "name": "Pillar 3 Report",
                "url": (
                    "https://www.macquarie.com/investors/"
                    "regulatory-disclosures.html"
                ),
                "filename_pattern": "MQG_{half}_{year}_Pillar3.pdf",
            },
        ],
    },

    # -------------------------------------------------------------------
    "rba_fsr": {
        "cache_dir": "data/raw/rba/",
        "description": "RBA Financial Stability Review",
        "manual_download": False,
        "files": [
            {
                "name": "Financial Stability Review",
                "url": "https://www.rba.gov.au/publications/fsr/",
                "filename_pattern": "RBA_FSR_{period}.pdf",
            },
        ],
    },

    # -------------------------------------------------------------------
    "rba_smp": {
        "cache_dir": "data/raw/rba/",
        "description": "RBA Statement on Monetary Policy",
        "manual_download": False,
        "files": [
            {
                "name": "Statement on Monetary Policy",
                "url": "https://www.rba.gov.au/publications/smp/",
                "filename_pattern": "RBA_SMP_{quarter}_{year}.pdf",
            },
        ],
    },

    # -------------------------------------------------------------------
    "rba_chart_pack": {
        "cache_dir": "data/raw/rba/",
        "description": "RBA Chart Pack",
        "manual_download": False,
        "files": [
            {
                "name": "Chart Pack",
                "url": "https://www.rba.gov.au/chart-pack/",
                "filename_pattern": "RBA_ChartPack_{quarter}_{year}.pdf",
            },
        ],
    },

    # -------------------------------------------------------------------
    # Forward-looking regulator commentary (newest-first scrape, per-issue
    # manifest). These do NOT feed BenchmarkRegistry; they surface in the
    # Board report's supporting-documentation section.
    # -------------------------------------------------------------------
    "apra_insight": {
        "cache_dir": "data/raw/apra/insight/",
        "description": "APRA Insight publications",
        "manual_download": False,
        "files": [
            {
                "name": "APRA Insight",
                "url": "https://www.apra.gov.au/news-and-publications/apra-insight",
                "filename_pattern": "APRA_Insight_{slug}.pdf",
            },
        ],
    },

    # -------------------------------------------------------------------
    "cfr_publications": {
        "cache_dir": "data/raw/cfr/",
        "description": "Council of Financial Regulators publications",
        "manual_download": False,
        "files": [
            {
                "name": "CFR Publications",
                "url": "https://www.cfr.gov.au/publications/",
                "filename_pattern": "CFR_{slug}.pdf",
            },
        ],
    },

}
