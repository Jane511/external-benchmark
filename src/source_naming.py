"""Plain-English names for source IDs and the acronyms used in the report.

The raw ``source_id`` values are machine-friendly (e.g.
``APRA_QPEX_MAJOR_BANKS_COMMERCIAL_PROPERTY_NPL_RATIO``) and are useful
when you want to filter a CSV. They are *not* useful when a committee is
reading the rendered report. This module maps a source_id to a short,
human-readable label such as "APRA QPEX (major banks)".

It also provides:
- ``parameter_label`` — friendly name for a parameter (``"pd"`` -> ``"PD"``).
- ``cohort_label`` — friendly name for a cohort.
- ``ACRONYM_GLOSSARY`` — terms used in the report.

Everything is pure functions / constants, no I/O. Stable across runs so
test snapshots don't churn.
"""
from __future__ import annotations

import re
from typing import Mapping


# ---------------------------------------------------------------------------
# Acronym glossary — rendered as a small table at the top of the report
# ---------------------------------------------------------------------------

ACRONYM_GLOSSARY: Mapping[str, str] = {
    "PD": "Probability of default — likelihood the borrower defaults within 12 months.",
    "LGD": "Loss given default — fraction of exposure not recovered after default.",
    "NPL": "Non-performing loan — typically 90+ days past due plus impaired.",
    "DPD": "Days past due.",
    "ADI": "Authorised deposit-taking institution (a bank, credit union, or building society).",
    "QPEX": "APRA Quarterly Property Exposures statistics.",
    "FSR": "Reserve Bank of Australia Financial Stability Review (semi-annual).",
    "SMP": "Reserve Bank of Australia Statement on Monetary Policy (quarterly).",
    "SPIN": "S&P Performance Index for Australian RMBS arrears.",
    "RMBS": "Residential mortgage-backed securities.",
    "APS 113": "APRA prudential standard for credit-risk capital — defines slotting grades and supervisory floors.",
    "Pillar 3": "Basel public disclosure of regulatory capital, RWA, EAD, PD, and LGD.",
    "CRE": "Commercial real estate.",
    "SME": "Small and medium enterprise.",
    "IRB": "Internal-ratings-based — Basel approach used by the Big 4 + Macquarie.",
    "MoC": "Margin of Conservatism — Basel/EBA add-on to mitigate model risk.",
    "FY24 / H1 / H2": "Financial year 2024; first / second half.",
}


# ---------------------------------------------------------------------------
# Cohort labels (must match src.models.Cohort enum values)
# ---------------------------------------------------------------------------

_COHORT_LABELS: Mapping[str, str] = {
    "peer_big4": "Big 4 banks",
    "peer_other_major_bank": "Other major bank (Macquarie)",
    "peer_non_bank": "Non-bank peers",
    "regulator_aggregate": "Regulator aggregate",
    "rating_agency": "Rating agency",
    "regulatory_floor": "Regulatory floor",
    "industry_body": "Industry body",
}


def cohort_label(cohort_value: str) -> str:
    """Friendly label for a Cohort enum value."""
    return _COHORT_LABELS.get(cohort_value, cohort_value.replace("_", " ").title())


# ---------------------------------------------------------------------------
# Parameter labels (must match src.models._ALLOWED_PARAMETERS)
# ---------------------------------------------------------------------------

_PARAMETER_LABELS: Mapping[str, str] = {
    "pd": "Probability of default (PD)",
    "lgd": "Loss given default (LGD)",
    "arrears": "Arrears",
    "impaired": "Impaired loans",
    "npl": "Non-performing loans (NPL)",
    "loss_rate": "Loss rate",
    "commentary": "Qualitative commentary",
}


def parameter_label(parameter: str) -> str:
    """Friendly label for a parameter."""
    return _PARAMETER_LABELS.get(parameter, parameter.replace("_", " ").title())


# ---------------------------------------------------------------------------
# Source-id -> friendly publisher name
# ---------------------------------------------------------------------------

# Big 4 + Macquarie name map (operates on the source_id prefix).
_BANK_NAMES: Mapping[str, str] = {
    "cba": "Commonwealth Bank",
    "nab": "NAB",
    "wbc": "Westpac",
    "anz": "ANZ",
    "mqg": "Macquarie",
    "macquarie": "Macquarie",
}

# Non-bank lender heads (lower-case head of source_id).
_NON_BANK_NAMES: Mapping[str, str] = {
    "judo": "Judo Bank",
    "liberty": "Liberty Financial",
    "pepper": "Pepper Money",
    "resimac": "Resimac",
    "moneyme": "MoneyMe",
    "plenti": "Plenti",
    "wisr": "Wisr",
    "qualitas": "Qualitas",
    "metrics": "Metrics Credit Partners",
    "latrobe": "La Trobe Financial",
}

# APS 113 slotting grade map (regulatory floor cohort).
_SLOTTING_LABELS: Mapping[str, str] = {
    "STRONG": "Strong",
    "GOOD": "Good",
    "SATIS": "Satisfactory",
    "WEAK": "Weak",
}

# APS 113 LGD floor map.
_APS113_FLOOR_LABELS: Mapping[str, str] = {
    "RES_LGD_FLOOR": "APS 113 LGD floor (residential)",
    "CRE_LGD_FLOOR": "APS 113 LGD floor (commercial)",
    "INVOICE_LGD_FLOOR": "APS 113 LGD floor (invoice finance)",
    "UNSECURED_LGD": "APS 113 LGD floor (senior unsecured)",
}

# APRA population labels (parsed from source_id).
_APRA_POPULATION_LABELS: Mapping[str, str] = {
    "ALL_ADIS": "all ADIs",
    "BANKS": "banks",
    "MAJOR_BANKS": "major banks",
}

# APRA metric suffix labels.
_APRA_METRIC_LABELS: Mapping[str, str] = {
    "NPL_RATIO": "NPL",
    "NINETY_DPD_RATE": "90+ DPD",
    "IMPAIRED": "impaired loan ratio",
    "COMMERCIAL_NPL": "commercial NPL",
}


def friendly_name(source_id: str) -> str:
    """Map a source_id to a human-readable label.

    The fallback for an unrecognised source_id is the raw string. Adding
    a new source family means adding a small branch here.
    """
    if not source_id:
        return ""
    sid = source_id

    # Lower-case heads (e.g. ``sp_spin_prime`` and the SPIN time-series rows).
    lower = sid.lower()
    if lower == "sp_spin_prime":
        return "S&P SPIN (prime RMBS, 30+ DPD)"
    if lower == "sp_spin_non_conforming":
        return "S&P SPIN (non-conforming RMBS, 30+ DPD)"

    head = lower.replace("-", "_").split("_", 1)[0]

    # Big 4 / Macquarie Pillar 3 — strip the PILLAR3_… suffix and add a
    # short product hint when present in the source_id.
    if "PILLAR3" in sid.upper() and head in _BANK_NAMES:
        bank = _BANK_NAMES[head]
        upper = sid.upper()
        product_hint = ""
        if "RES_LGD" in upper:
            product_hint = " (residential LGD)"
        elif "RES" in upper.split("PILLAR3", 1)[1]:
            product_hint = " (residential PD)"
        elif "CRE" in upper.split("PILLAR3", 1)[1]:
            product_hint = " (commercial property PD)"
        elif "CORP_SME" in upper:
            product_hint = " (corporate SME PD)"
        elif "SME_UNSECURED" in upper:
            product_hint = " (SME unsecured LGD)"
        return f"{bank}{product_hint}"

    # Big 4 / Macquarie short forms ("cba", "mqg") used by tests + adapters.
    if lower in _BANK_NAMES:
        return _BANK_NAMES[lower]

    # APS 113 slotting grade rows (PD or LGD).
    upper = sid.upper()
    if upper.startswith("APS113_SLOTTING_"):
        for token, label in _SLOTTING_LABELS.items():
            if f"_{token}_" in f"_{upper}_" or upper.endswith(f"_{token}_PD") or upper.endswith(f"_{token}_LGD"):
                kind = "PD" if upper.endswith("_PD") else "LGD"
                return f"APS 113 slotting (Strong / Good / Satisfactory / Weak): {label} {kind}"
        return "APS 113 slotting"

    # APS 113 LGD floors.
    for suffix, label in _APS113_FLOOR_LABELS.items():
        if upper.endswith(suffix) or upper.endswith(f"APS113_{suffix}"):
            return label

    # APRA quarterly performance / property-exposures time series.
    apra_match = re.match(
        r"APRA_(?P<table>QPEX|PERF)_"
        r"(?P<population>ALL_ADIS|BANKS|MAJOR_BANKS)_"
        r"(?P<remainder>.+)$",
        upper,
    )
    if apra_match:
        table = "QPEX" if apra_match.group("table") == "QPEX" else "performance"
        population = _APRA_POPULATION_LABELS.get(apra_match.group("population"), "")
        remainder = apra_match.group("remainder")
        metric = ""
        if remainder.endswith("NPL_RATIO"):
            metric = ", NPL"
        elif remainder.endswith("NINETY_DPD_RATE"):
            metric = ", 90+ DPD"
        return f"APRA {table} ({population}{metric})"

    # APRA legacy single-snapshot rows used in seed_data.
    if upper.startswith("APRA_QPEX_CRE_IMPAIRED"):
        return "APRA QPEX (commercial property impaired ratio)"
    if upper.startswith("APRA_PERF_COMMERCIAL_NPL"):
        return "APRA performance (commercial NPL)"

    # RBA FSR aggregates.
    if upper.startswith("RBA_FSR_HH_ARREARS"):
        return "RBA FSR (household 90+ arrears)"
    if upper.startswith("RBA_FSR_BUSINESS_ARREARS"):
        return "RBA FSR (business arrears)"
    if upper.startswith("RBA_FSR"):
        return "RBA FSR"

    # S&P SPIN single-snapshot rows.
    if upper.startswith("SP_SPIN_PRIME_RMBS"):
        return "S&P SPIN (prime RMBS, 30+ DPD)"
    if upper.startswith("SP_SPIN_NON_CONFORMING"):
        return "S&P SPIN (non-conforming RMBS, 30+ DPD)"
    if upper.startswith("SP_CORP_DEFAULT"):
        return "S&P (corporate default, global IG)"

    # Industry-body / bureau aggregates.
    if upper.startswith("AFIA_INVOICE"):
        return "AFIA (invoice finance loss rate)"
    if upper.startswith("ILLION_BFRI"):
        return "illion BFRI"

    # Non-bank named sources (commentary + La Trobe).
    if "QUALITAS" in upper:
        return "Qualitas (commentary)"
    if "METRICS" in upper:
        return "Metrics Credit (commentary)"
    if "LATROBE" in upper:
        return "La Trobe Financial (bridging realised loss)"
    if head in _NON_BANK_NAMES:
        return _NON_BANK_NAMES[head]

    # Fallback: original string. Better than guessing wrong.
    return source_id


def segment_label(segment: str) -> str:
    """Friendly label for a canonical segment name."""
    titled = segment.replace("_", " ").title()
    # Acronyms titled() mangles.
    titled = titled.replace("Sme", "SME").replace("Adi", "ADI").replace("Lgd", "LGD")
    return titled
