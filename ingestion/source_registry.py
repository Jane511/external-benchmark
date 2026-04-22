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

The seventh "reserved" slot covers sources that are planned but not yet
scraped (CoreLogic, ASIC, ABS). Documented so the schema keeps room for them
without needing another refactor.
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
    "icc_trade": {
        "cache_dir": "data/raw/icc/",
        "description": "ICC Trade Register (annual; paid publication; manual download)",
        "manual_download": True,
        "files": [
            {
                "name": "ICC Trade Register Report",
                "url": (
                    "https://iccwbo.org/news-publications/policies-reports/"
                    "icc-trade-register-report/"
                ),
                "filename_pattern": "ICC_Trade_Register_{year}.pdf",
            },
        ],
    },

    # -------------------------------------------------------------------
    # Reserved slot for future sources (ASIC/ABS, RBA financial stability
    # review, other manual-download providers). No download URLs yet;
    # included here so CacheManager can detect files the user drops manually.
    "reserved_future": {
        "cache_dir": "data/raw/other/",
        "description": "Reserved — ASIC / ABS / RBA (future phases)",
        "manual_download": True,
        "files": [],
    },
}
