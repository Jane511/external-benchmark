"""Canonical segment glossary for committee-facing benchmark reports."""
from __future__ import annotations


SEGMENT_GLOSSARY: dict[str, str] = {
    "adi_sector_total": (
        "APRA sector-level aggregate across ADI loan portfolios, used for "
        "system-wide NPL and arrears trend context."
    ),
    "bridging_residential": (
        "Short-tenor bridging and residual-stock residential lending, typically "
        "secured by residential property and repaid from sale or refinance."
    ),
    "commercial_property": (
        "Income-producing and investment commercial real estate exposures, "
        "including office, retail, industrial and mixed-use CRE lending."
    ),
    "corporate_sme": (
        "Small and medium enterprise corporate lending, including term debt and "
        "working-capital style facilities to trading businesses."
    ),
    "development": (
        "Construction and land-development lending, including specialised "
        "lending slotting-style project finance and staged draw structures."
    ),
    "invoice_finance": (
        "Receivables-backed lending such as invoice discounting and factoring, "
        "where repayment is supported by short-dated trade debtors."
    ),
    "residential_mortgage": (
        "Prime residential home lending secured by owner-occupied or standard "
        "investment housing collateral."
    ),
    "residential_mortgage_specialist": (
        "Specialist or non-conforming residential mortgages, including near-prime "
        "and credit-impaired borrower cohorts."
    ),
    "residential_mortgage_arrears": (
        "Residential mortgage arrears reference segment used for governance and "
        "reality checks rather than direct peer-PD comparison."
    ),
    "residential_mortgage_specialist_arrears": (
        "Specialist residential arrears reference segment for non-conforming "
        "mortgage pools and adjacent stress-testing comparisons."
    ),
    "sme_corporate": (
        "System-wide SME and commercial lending aggregate used as an external "
        "reference point for bank and non-bank portfolio benchmarks."
    ),
    "working_capital_unsecured": (
        "Unsecured working-capital and general corporate facilities without "
        "specific collateral support, typically revolving or short-tenor."
    ),
}
