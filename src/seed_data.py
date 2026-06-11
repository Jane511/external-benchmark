"""Seed data for the External Benchmark Engine — Australian segments.

Values are authentic to Australian external-benchmark sources per plan §3:
  - Big 4 Pillar 3 disclosures (CBA, NAB, WBC, ANZ) H2 2024 snapshot
  - APRA APS 113 regulatory floors and specialised-lending slotting grades
  - illion BFRI aggregate failure rates
  - AFIA invoice finance aggregates
  - S&P corporate default study
  - La Trobe bridging realised-loss disclosure

LGD **rates** (portfolio-level, no `component` field) are seeded here. LGD
**components** (haircut / time_to_recovery / workout_costs / discount_rate
entries with `component=` populated) were extracted to `future_lgd/` and
belong in a downstream LGD model project, not the benchmark engine.

Call `load_seed_data(registry)` after initialising a fresh registry to
populate every seed entry in one pass.
"""
from __future__ import annotations

from datetime import date
from typing import Any

from src.models import (
    BenchmarkEntry,
    DataType,
    QualityScore,
    SourceType,
)
from src.registry import BenchmarkRegistry


def _entry(**overrides: Any) -> BenchmarkEntry:
    """Shorthand with AU / 5-year / HIGH-quality / 2024H2 defaults."""
    base: dict[str, Any] = {
        "geography": "AU",
        "period_years": 5,
        "value_date": date(2024, 12, 31),
        "retrieval_date": date(2025, 3, 1),
        "quality_score": QualityScore.HIGH,
        "notes": "",
    }
    base.update(overrides)
    if not str(base.get("notes") or "").strip():
        base["notes"] = _default_notes(base)
    return BenchmarkEntry(**base)


def _default_notes(payload: dict[str, Any]) -> str:
    """Populate a non-placeholder methodology note for seed rows missing one."""
    publisher = str(payload.get("publisher", "Source"))
    asset_class = str(payload.get("asset_class", "portfolio")).replace("_", " ")
    source_type = payload.get("source_type")
    data_type = payload.get("data_type")
    url = str(payload.get("url", ""))
    source_id = str(payload.get("source_id", ""))
    value_date = payload.get("value_date")
    period = value_date.isoformat() if hasattr(value_date, "isoformat") else "current cycle"
    metric = data_type.value.upper() if hasattr(data_type, "value") else "metric"

    if source_type == SourceType.PILLAR3:
        return f"{publisher} Pillar 3 {asset_class} {metric} disclosure ({period}); {url}"
    if source_type == SourceType.REGULATORY:
        return f"APRA APS 113 {asset_class} {metric} reference ({period}); {url}"
    if source_type == SourceType.RBA:
        return f"RBA aggregate {asset_class} {metric} reference ({period}); {url}"
    if source_type == SourceType.RATING_AGENCY:
        return f"{publisher} rating-agency {asset_class} {metric} reference ({period}); {url}"
    if source_type == SourceType.INDUSTRY_BODY:
        return f"{publisher} industry aggregate for {asset_class} ({period}); {url}"
    if source_type == SourceType.BUREAU:
        return f"{publisher} bureau reference for {asset_class} ({period}); {url}"
    if source_type == SourceType.LISTED_PEER:
        return f"{publisher} listed-peer disclosure for {asset_class} ({period}); {url}"
    return f"{publisher} source note for {source_id or asset_class} ({period}); {url}"


# ---------------------------------------------------------------------------
# 1. Residential mortgage (Bank)
# ---------------------------------------------------------------------------
_RESIDENTIAL = [
    _entry(
        source_id="CBA_PILLAR3_RES_2024H2", publisher="Commonwealth Bank of Australia",
        source_type=SourceType.PILLAR3, data_type=DataType.PD,
        asset_class="residential_mortgage",
        url="https://www.commbank.com.au/pillar3", value=0.0072,
        notes="CBA residential PD disclosure H2 2024",
    ),
    _entry(
        source_id="NAB_PILLAR3_RES_2024H2", publisher="National Australia Bank",
        source_type=SourceType.PILLAR3, data_type=DataType.PD,
        asset_class="residential_mortgage",
        url="https://www.nab.com.au/pillar3", value=0.0090,
    ),
    _entry(
        source_id="WBC_PILLAR3_RES_2024H2", publisher="Westpac Banking Corporation",
        source_type=SourceType.PILLAR3, data_type=DataType.PD,
        asset_class="residential_mortgage",
        url="https://www.westpac.com.au/pillar3", value=0.0088,
    ),
    _entry(
        source_id="ANZ_PILLAR3_RES_2024H2", publisher="Australia and New Zealand Banking Group",
        source_type=SourceType.PILLAR3, data_type=DataType.PD,
        asset_class="residential_mortgage",
        url="https://www.anz.com/pillar3", value=0.0080,
    ),
    _entry(
        source_id="CBA_PILLAR3_RES_LGD_2024H2", publisher="Commonwealth Bank of Australia",
        source_type=SourceType.PILLAR3, data_type=DataType.LGD,
        asset_class="residential_mortgage",
        url="https://www.commbank.com.au/pillar3", value=0.22,
        notes="CBA residential LGD (downturn-adjusted) H2 2024",
    ),
    _entry(
        source_id="NAB_PILLAR3_RES_LGD_2024H2", publisher="National Australia Bank",
        source_type=SourceType.PILLAR3, data_type=DataType.LGD,
        asset_class="residential_mortgage",
        url="https://www.nab.com.au/pillar3", value=0.24,
    ),
    _entry(
        source_id="APS113_RES_LGD_FLOOR", publisher="APRA",
        source_type=SourceType.REGULATORY, data_type=DataType.SUPERVISORY_VALUE,
        asset_class="residential_mortgage",
        url="https://www.apra.gov.au/aps-113",
        period_years=10, value=0.075,
        notes="APS 113 residential LGD floor (5-10%, midpoint)",
    ),
]

# ---------------------------------------------------------------------------
# 2. Corporate SME (Bank + PC)
# ---------------------------------------------------------------------------
_CORPORATE_SME = [
    _entry(
        source_id="CBA_PILLAR3_CORP_SME_2024H2", publisher="Commonwealth Bank of Australia",
        source_type=SourceType.PILLAR3, data_type=DataType.PD,
        asset_class="corporate_sme",
        url="https://www.commbank.com.au/pillar3", value=0.0280,
    ),
    _entry(
        source_id="NAB_PILLAR3_CORP_SME_2024H2", publisher="National Australia Bank",
        source_type=SourceType.PILLAR3, data_type=DataType.PD,
        asset_class="corporate_sme",
        url="https://www.nab.com.au/pillar3", value=0.0320,
    ),
    _entry(
        source_id="WBC_PILLAR3_CORP_SME_2024H2", publisher="Westpac Banking Corporation",
        source_type=SourceType.PILLAR3, data_type=DataType.PD,
        asset_class="corporate_sme",
        url="https://www.westpac.com.au/pillar3", value=0.0270,
    ),
    _entry(
        source_id="ANZ_PILLAR3_CORP_SME_2024H2", publisher="Australia and New Zealand Banking Group",
        source_type=SourceType.PILLAR3, data_type=DataType.PD,
        asset_class="corporate_sme",
        url="https://www.anz.com/pillar3", value=0.0340,
    ),
    _entry(
        source_id="SP_CORP_DEFAULT_IG_GLOBAL", publisher="S&P Global Ratings",
        source_type=SourceType.RATING_AGENCY, data_type=DataType.DEFAULT_RATE,
        asset_class="corporate_sme",
        url="https://www.spglobal.com/ratings/default-study",
        period_years=10, value=0.017,
        notes="S&P global IG corporate default — requires AU adjustment",
    ),
    _entry(
        source_id="ILLION_BFRI_CORP_2024", publisher="illion",
        source_type=SourceType.BUREAU, data_type=DataType.FAILURE_RATE,
        asset_class="corporate_sme",
        url="https://www.illion.com.au/bfri", value=0.035,
        notes="illion Business Failure Risk Index — requires definition alignment",
        quality_score=QualityScore.MEDIUM,
    ),
]

# ---------------------------------------------------------------------------
# 3. Bridging residential (PC)
# ---------------------------------------------------------------------------
_BRIDGING_RESIDENTIAL = [
    _entry(
        source_id="LATROBE_BRIDGING_REALISED_LOSS", publisher="La Trobe Financial",
        source_type=SourceType.LISTED_PEER, data_type=DataType.LGD,
        asset_class="bridging_residential",
        url="https://www.latrobefinancial.com.au/disclosures",
        value=0.08, quality_score=QualityScore.MEDIUM,
        notes="La Trobe realised loss on bridging book",
    ),
]

# ---------------------------------------------------------------------------
# 4. Development (PC) — APS 113 slotting grades
# ---------------------------------------------------------------------------
_DEVELOPMENT = [
    _entry(
        source_id="APS113_SLOTTING_STRONG_PD", publisher="APRA",
        source_type=SourceType.REGULATORY, data_type=DataType.PD,
        asset_class="development",
        url="https://www.apra.gov.au/aps-113",
        period_years=10, value=0.004,
        notes="APS 113 specialised lending: Strong grade PD",
    ),
    _entry(
        source_id="APS113_SLOTTING_GOOD_PD", publisher="APRA",
        source_type=SourceType.REGULATORY, data_type=DataType.PD,
        asset_class="development",
        url="https://www.apra.gov.au/aps-113",
        period_years=10, value=0.008,
    ),
    _entry(
        source_id="APS113_SLOTTING_SATIS_PD", publisher="APRA",
        source_type=SourceType.REGULATORY, data_type=DataType.PD,
        asset_class="development",
        url="https://www.apra.gov.au/aps-113",
        period_years=10, value=0.028,
    ),
    _entry(
        source_id="APS113_SLOTTING_WEAK_PD", publisher="APRA",
        source_type=SourceType.REGULATORY, data_type=DataType.PD,
        asset_class="development",
        url="https://www.apra.gov.au/aps-113",
        period_years=10, value=0.080,
    ),
    _entry(
        source_id="APS113_SLOTTING_STRONG_LGD", publisher="APRA",
        source_type=SourceType.REGULATORY, data_type=DataType.LGD,
        asset_class="development",
        url="https://www.apra.gov.au/aps-113",
        period_years=10, value=0.275,
        notes="APS 113 slotting Strong LGD (25-30% midpoint)",
    ),
    _entry(
        source_id="APS113_SLOTTING_GOOD_LGD", publisher="APRA",
        source_type=SourceType.REGULATORY, data_type=DataType.LGD,
        asset_class="development",
        url="https://www.apra.gov.au/aps-113",
        period_years=10, value=0.325,
    ),
    _entry(
        source_id="APS113_SLOTTING_SATIS_LGD", publisher="APRA",
        source_type=SourceType.REGULATORY, data_type=DataType.LGD,
        asset_class="development",
        url="https://www.apra.gov.au/aps-113",
        period_years=10, value=0.375,
    ),
    _entry(
        source_id="APS113_SLOTTING_WEAK_LGD", publisher="APRA",
        source_type=SourceType.REGULATORY, data_type=DataType.LGD,
        asset_class="development",
        url="https://www.apra.gov.au/aps-113",
        period_years=10, value=0.450,
    ),
]

# ---------------------------------------------------------------------------
# 5. Invoice finance (PC)
# ---------------------------------------------------------------------------
_INVOICE_FINANCE = [
    _entry(
        source_id="APS113_INVOICE_LGD_FLOOR", publisher="APRA",
        source_type=SourceType.REGULATORY, data_type=DataType.SUPERVISORY_VALUE,
        asset_class="invoice_finance",
        url="https://www.apra.gov.au/aps-113",
        period_years=10, value=0.35,
        notes="APS 113 eligible receivables LGD",
    ),
    _entry(
        source_id="AFIA_INVOICE_LOSS_RATE", publisher="Australian Finance Industry Association",
        source_type=SourceType.INDUSTRY_BODY, data_type=DataType.DEFAULT_RATE,
        asset_class="invoice_finance",
        url="https://afia.asn.au/research", value=0.012,
        quality_score=QualityScore.MEDIUM,
    ),
]

# ---------------------------------------------------------------------------
# 6. Working capital unsecured (PC)
# ---------------------------------------------------------------------------
_WORKING_CAPITAL = [
    _entry(
        source_id="APS113_UNSECURED_LGD", publisher="APRA",
        source_type=SourceType.REGULATORY, data_type=DataType.SUPERVISORY_VALUE,
        asset_class="working_capital_unsecured",
        url="https://www.apra.gov.au/aps-113",
        period_years=10, value=0.45,
        notes="APS 113 senior unsecured LGD",
    ),
    _entry(
        source_id="CBA_PILLAR3_SME_UNSECURED_LGD", publisher="Commonwealth Bank of Australia",
        source_type=SourceType.PILLAR3, data_type=DataType.LGD,
        asset_class="working_capital_unsecured",
        url="https://www.commbank.com.au/pillar3", value=0.48,
    ),
    _entry(
        source_id="ILLION_BFRI_UNSECURED_WC", publisher="illion",
        source_type=SourceType.BUREAU, data_type=DataType.FAILURE_RATE,
        asset_class="working_capital_unsecured",
        url="https://www.illion.com.au/bfri", value=0.041,
        quality_score=QualityScore.MEDIUM,
        notes="illion BFRI aggregate — requires ANZSIC / definition alignment",
    ),
]

# ---------------------------------------------------------------------------
# Reality-check sources — published with explicit data_definition_class
#
# These rows seed the BenchmarkEntry table with the canonical reality-check
# observations identified in the verification analysis. After running
# `src/migrate_to_raw_observations.py`, each row appears in
# `raw_observations` with parameter and data_definition_class inferred
# from the source_id pattern. The downstream PD/LGD/ECL projects read
# these via `PeerObservations.for_segment(...)` to apply per-product
# upper/lower band sanity checks (see `config/reality_check_bands.yaml`).
# ---------------------------------------------------------------------------

# APRA QPEX commercial property — system-wide impaired ratio.
_APRA_QPEX_CRE_NPL = [
    _entry(
        source_id="APRA_QPEX_CRE_IMPAIRED_2024Q4",
        publisher="APRA",
        source_type=SourceType.APRA_ADI,
        data_type=DataType.IMPAIRED_RATIO,
        asset_class="commercial_property",
        url="https://www.apra.gov.au/quarterly-property-exposures-statistics",
        period_years=1,
        value=0.012,
        quality_score=QualityScore.HIGH,
        notes=(
            "System-wide ADI commercial property impaired loan ratio "
            "(QPEX). Reality-check upper-band reference for non-bank CRE LRA."
        ),
    ),
]

# APRA quarterly ADI performance — system-wide commercial NPL.
_APRA_QUARTERLY_NPL = [
    _entry(
        source_id="APRA_PERF_COMMERCIAL_NPL_2024Q4",
        publisher="APRA",
        source_type=SourceType.APRA_ADI,
        data_type=DataType.IMPAIRED_RATIO,
        asset_class="sme_corporate",
        url=(
            "https://www.apra.gov.au/quarterly-authorised-deposit-taking-"
            "institution-performance-statistics"
        ),
        period_years=1,
        value=0.013,
        quality_score=QualityScore.HIGH,
        notes=(
            "System-wide ADI commercial NPL ratio. Reality-check lower-band "
            "reference: any LRA below ~0.5x system NPL is suspect for "
            "non-bank lenders."
        ),
    ),
]

# RBA FSR — system-wide household + business arrears.
_RBA_FSR_AGGREGATES = [
    _entry(
        source_id="RBA_FSR_HH_ARREARS_90PLUS_2024H2",
        publisher="Reserve Bank of Australia",
        source_type=SourceType.RBA,
        data_type=DataType.IMPAIRED_RATIO,
        asset_class="residential_mortgage",
        url="https://www.rba.gov.au/publications/fsr/",
        period_years=1,
        value=0.009,
        quality_score=QualityScore.HIGH,
        notes=(
            "System-wide owner-occupier housing 90+ DPD per RBA "
            "Securitisation Dataset, FSR Sep 2024."
        ),
    ),
    _entry(
        source_id="RBA_FSR_BUSINESS_ARREARS_2024H2",
        publisher="Reserve Bank of Australia",
        source_type=SourceType.RBA,
        data_type=DataType.IMPAIRED_RATIO,
        asset_class="sme_corporate",
        url="https://www.rba.gov.au/publications/fsr/",
        period_years=1,
        value=0.014,
        quality_score=QualityScore.HIGH,
        notes="System-wide business loan arrears per RBA FSR Sep 2024.",
    ),
]

# S&P SPIN — Australian RMBS arrears index.
_SP_SPIN = [
    _entry(
        source_id="sp_spin_prime",
        publisher="S&P Global Ratings",
        source_type=SourceType.RATING_AGENCY,
        data_type=DataType.DEFAULT_RATE,
        asset_class="residential_mortgage",
        url="https://www.spglobal.com/ratings/en/regulatory/topic/spin",
        period_years=1,
        value_date=date(2026, 2, 28),
        retrieval_date=date(2026, 4, 28),
        value=0.0079,
        quality_score=QualityScore.HIGH,
        notes=(
            "Latest staged S&P SPIN release (February 2026): "
            "weighted-average 30+ DPD arrears rate, prime RMBS universe. "
            "Refresh quarterly."
        ),
    ),
    _entry(
        source_id="sp_spin_non_conforming",
        publisher="S&P Global Ratings",
        source_type=SourceType.RATING_AGENCY,
        data_type=DataType.DEFAULT_RATE,
        asset_class="residential_mortgage_specialist",
        url="https://www.spglobal.com/ratings/en/regulatory/topic/spin",
        period_years=1,
        value_date=date(2026, 2, 28),
        retrieval_date=date(2026, 4, 28),
        value=0.0390,
        quality_score=QualityScore.HIGH,
        notes=(
            "Latest staged S&P SPIN release (February 2026): "
            "weighted-average 30+ DPD arrears rate, non-conforming RMBS "
            "universe. Refresh quarterly."
        ),
    ),
    _entry(
        source_id="SP_SPIN_PRIME_RMBS_30PLUS_2024Q4",
        publisher="S&P Global Ratings",
        source_type=SourceType.RATING_AGENCY,
        data_type=DataType.DEFAULT_RATE,
        asset_class="residential_mortgage",
        url="https://www.spglobal.com/ratings/en/regulatory/topic/spin",
        period_years=1,
        value=0.0093,
        quality_score=QualityScore.HIGH,
        notes=(
            "S&P SPIN aggregate Australian prime + non-conforming RMBS 30+ "
            "DPD arrears (Feb 2024 print). Free public index. Reality-check "
            "for residential lending products."
        ),
    ),
    _entry(
        source_id="SP_SPIN_NON_CONFORMING_30PLUS_2024Q4",
        publisher="S&P Global Ratings",
        source_type=SourceType.RATING_AGENCY,
        data_type=DataType.DEFAULT_RATE,
        asset_class="residential_mortgage",
        url="https://www.spglobal.com/ratings/en/regulatory/topic/spin",
        period_years=1,
        value=0.039,
        quality_score=QualityScore.HIGH,
        notes=(
            "Non-conforming Australian RMBS 30+ DPD. Closer to non-bank "
            "specialist book composition."
        ),
    ),
]

# Big 4 Pillar 3 commercial property PD — flagship reality-check anchor
# under the canonical `commercial_property` segment.
_BIG4_PILLAR3_CRE = [
    _entry(
        source_id="CBA_PILLAR3_CRE_PD_2024H2",
        publisher="Commonwealth Bank of Australia",
        source_type=SourceType.PILLAR3,
        data_type=DataType.PD,
        asset_class="commercial_property",
        url="https://www.commbank.com.au/about-us/investors/annual-reports.html",
        period_years=1,
        value=0.0250,
        quality_score=QualityScore.HIGH,
        notes="CBA Pillar 3 Table CR6 commercial property PD, H2 2024.",
    ),
    _entry(
        source_id="NAB_PILLAR3_CRE_PD_2024H2",
        publisher="National Australia Bank",
        source_type=SourceType.PILLAR3,
        data_type=DataType.PD,
        asset_class="commercial_property",
        url="https://www.nab.com.au/about-us/shareholder-centre/regulatory-disclosures",
        period_years=1,
        value=0.0220,
        quality_score=QualityScore.HIGH,
        notes="NAB Pillar 3 commercial property PD, H2 2024.",
    ),
    _entry(
        source_id="WBC_PILLAR3_CRE_PD_2024H2",
        publisher="Westpac Banking Corporation",
        source_type=SourceType.PILLAR3,
        data_type=DataType.PD,
        asset_class="commercial_property",
        url="https://www.westpac.com.au/financial-information/regulatory-disclosures/",
        period_years=1,
        value=0.0260,
        quality_score=QualityScore.HIGH,
        notes="WBC Pillar 3 commercial property PD, H2 2024.",
    ),
    _entry(
        source_id="ANZ_PILLAR3_CRE_PD_2024H2",
        publisher="Australia and New Zealand Banking Group",
        source_type=SourceType.PILLAR3,
        data_type=DataType.PD,
        asset_class="commercial_property",
        url="https://www.anz.com/shareholder/centre/reporting/regulatory-disclosure/",
        period_years=1,
        value=0.0210,
        quality_score=QualityScore.HIGH,
        notes="ANZ Pillar 3 commercial property PD, H2 2024.",
    ),
    _entry(
        source_id="APS113_CRE_LGD_FLOOR",
        publisher="APRA",
        source_type=SourceType.REGULATORY,
        data_type=DataType.SUPERVISORY_VALUE,
        asset_class="commercial_property",
        url="https://www.apra.gov.au/aps-113",
        period_years=10,
        value=0.175,
        quality_score=QualityScore.HIGH,
        notes="APS 113 commercial property LGD floor (10-25%, midpoint).",
    ),
]

# Qualitas — qualitative commentary placeholder. value=0.0 is a tag, not a
# rate; the methodology_note carries the published narrative.
_QUALITAS_COMMENTARY = [
    _entry(
        source_id="QUALITAS_CRE_COMMENTARY_2024H2",
        publisher="Qualitas Limited",
        source_type=SourceType.LISTED_PEER,
        data_type=DataType.PD,
        asset_class="commercial_property",
        url="https://www.qualitas.com.au/",
        period_years=1,
        value=0.0,
        quality_score=QualityScore.LOW,
        notes=(
            "QUALITATIVE: 'Office sector showing pressure in Q4 2024 with "
            "extended workout times. Industrial and BTR remain robust.' "
            "Source: Qualitas FY24 results presentation."
        ),
    ),
]

# Metrics Credit Partners — qualitative commentary placeholder.
_METRICS_CREDIT_COMMENTARY = [
    _entry(
        source_id="METRICS_CRE_COMMENTARY_2024H2",
        publisher="Metrics Credit Partners",
        source_type=SourceType.LISTED_PEER,
        data_type=DataType.PD,
        asset_class="commercial_property",
        url="https://metrics.com.au/",
        period_years=1,
        value=0.0,
        quality_score=QualityScore.LOW,
        notes=(
            "QUALITATIVE: 'CRE credit environment stable with selective "
            "stress in lower-tier office markets.' Source: Metrics Real "
            "Estate Income Fund H2 2024 update."
        ),
    ),
]


# Non-bank disclosures — values transcribed directly from the cached
# investor presentations / annual reports under data/raw/non_bank/. Each
# row carries the published figure plus a methodology note pointing at the
# source page so a reviewer can spot-check.
_NON_BANK_DISCLOSURES = [
    # Pepper Money — CY2025 Investor Presentation (calendar year ended
    # 31 Dec 2025), p14 "Credit Performance | Loan Loss Expense".
    _entry(
        source_id="PEPPER_MORTGAGES_90DPD_CY2025",
        publisher="Pepper Money",
        source_type=SourceType.LISTED_PEER,
        data_type=DataType.IMPAIRED_RATIO,
        asset_class="residential_mortgage_specialist",
        url="https://www.peppermoney.com.au/about/shareholders",
        value=0.0189,
        value_date=date(2025, 12, 31),
        retrieval_date=date(2026, 5, 1),
        period_years=1,
        notes=(
            "Pepper Money mortgages 90+ Day Arrears as % of AUM (CY2025, "
            "incl. NZ + CRE; excl. HSBC NZ acquired portfolio). Source: "
            "PPM CY2025 Investor Presentation, p14."
        ),
    ),
    _entry(
        source_id="PEPPER_ASSET_FINANCE_90DPD_CY2025",
        publisher="Pepper Money",
        source_type=SourceType.LISTED_PEER,
        data_type=DataType.IMPAIRED_RATIO,
        asset_class="consumer_secured",
        url="https://www.peppermoney.com.au/about/shareholders",
        value=0.0032,
        value_date=date(2025, 12, 31),
        retrieval_date=date(2026, 5, 1),
        period_years=1,
        notes=(
            "Pepper Money asset finance 90+ Day Arrears as % of AUM "
            "(CY2025; excludes loans in 120+ DPD charge-off). Source: "
            "PPM CY2025 Investor Presentation, p14."
        ),
    ),
    # Liberty Financial — FY25 Annual Report (year ended 30 Jun 2025),
    # Note 6 Financial Risk Management, p72: portfolio expected loss rate.
    _entry(
        source_id="LIBERTY_PORTFOLIO_ECL_FY25",
        publisher="Liberty Financial",
        source_type=SourceType.LISTED_PEER,
        data_type=DataType.LGD,
        asset_class="residential_mortgage",
        url="https://www.lfgroup.com.au/reports/annual-reports",
        value=0.0039,
        value_date=date(2025, 6, 30),
        retrieval_date=date(2026, 5, 1),
        period_years=1,
        notes=(
            "Liberty Group portfolio expected credit loss rate across "
            "$14.67bn gross book (Stage 1 0.31% / Stage 2 2.31% / Stage 3 "
            "1.62%; total 0.39%). Reported as a realised-loss-style "
            "indicator — not a Basel LGD. Source: LFG FY25 Annual Report "
            "Note 6, p72."
        ),
    ),
    _entry(
        source_id="LIBERTY_STAGE3_ECL_FY25",
        publisher="Liberty Financial",
        source_type=SourceType.LISTED_PEER,
        data_type=DataType.LGD,
        asset_class="residential_mortgage_specialist",
        url="https://www.lfgroup.com.au/reports/annual-reports",
        value=0.0162,
        value_date=date(2025, 6, 30),
        retrieval_date=date(2026, 5, 1),
        period_years=1,
        notes=(
            "Liberty Stage 3 (credit-impaired) expected loss rate; the "
            "closest disclosed proxy for LGD on a non-prime book. Source: "
            "LFG FY25 Annual Report Note 6, p72."
        ),
    ),
    # Resimac — FY25 Annual Report (year ended 30 Jun 2025), Note 2.6
    # "Loan impairment".
    _entry(
        source_id="RESIMAC_IMPAIRMENT_RATIO_FY25",
        publisher="Resimac",
        source_type=SourceType.LISTED_PEER,
        data_type=DataType.LGD,
        asset_class="residential_mortgage_specialist",
        url="https://www.resimac.com.au/investors/annual-reports",
        value=0.0023,
        value_date=date(2025, 6, 30),
        retrieval_date=date(2026, 5, 1),
        period_years=1,
        notes=(
            "Resimac loan impairment expense $22.56m on $9.82bn loans and "
            "advances (~0.23% loss rate); ECL allowance $56.5m on AUM. "
            "Source: Resimac FY25 Annual Report Note 2.6 (p23) + Note "
            "lending receivables (p35)."
        ),
    ),
    # La Trobe Financial — Credit Fund Half-Year Report FY2026 (period
    # ended 31 Dec 2025) plus Investment Snapshot & Metrics PDF.
    _entry(
        source_id="LATROBE_CREDIT_FUND_ARREARS_HY26",
        publisher="La Trobe Financial",
        source_type=SourceType.LISTED_PEER,
        data_type=DataType.IMPAIRED_RATIO,
        asset_class="residential_mortgage_specialist",
        url="https://www.latrobefinancial.com.au/investments/forms-library/",
        value=0.0047,
        value_date=date(2025, 12, 31),
        retrieval_date=date(2026, 5, 1),
        period_years=1,
        notes=(
            "La Trobe Australian Credit Fund total amount in arrears as % "
            "of total loan balance (0.47% at 31 Dec 2025; 0.45% prior). "
            "Source: La Trobe FY2026 Half-Year Report (LACF), p14."
        ),
    ),
    _entry(
        source_id="LATROBE_DEFAULT_RATE_2025",
        publisher="La Trobe Financial",
        source_type=SourceType.LISTED_PEER,
        data_type=DataType.DEFAULT_RATE,
        asset_class="residential_mortgage_specialist",
        url="https://www.latrobefinancial.com.au/investments/forms-library/",
        value=0.027,
        value_date=date(2025, 12, 31),
        retrieval_date=date(2026, 5, 1),
        period_years=1,
        notes=(
            "La Trobe 'Default loans > 30 days' as % of investment "
            "account balance (12-month Term Account 2.7%). Source: La "
            "Trobe Investment Snapshot & Metrics, p1."
        ),
    ),
    # Judo Bank — APRA-authorised ADI (ASX:JDO), Standardised approach.
    # Source: Judo 1H26 Half-Year Report (period ended 31 Dec 2025), p11
    # "Asset quality" + p8 result overview.
    _entry(
        source_id="JUDO_NPL_RATIO_HY26",
        publisher="Judo Bank",
        source_type=SourceType.PILLAR3,
        data_type=DataType.IMPAIRED_RATIO,
        asset_class="corporate_sme",
        url="https://www.judo.bank/regulatory-disclosures",
        value=0.0344,
        value_date=date(2025, 12, 31),
        retrieval_date=date(2026, 5, 1),
        period_years=1,
        notes=(
            "Judo Bank non-performing loans / GLA = 3.44% (Dec-25; "
            "Jun-25: 3.18%). Standardised-approach ADI; not a Basel "
            "PD. Source: Judo 1H26 Half-Year Report, p11."
        ),
    ),
    _entry(
        source_id="JUDO_90DPD_IMPAIRED_HY26",
        publisher="Judo Bank",
        source_type=SourceType.PILLAR3,
        data_type=DataType.IMPAIRED_RATIO,
        asset_class="corporate_sme",
        url="https://www.judo.bank/regulatory-disclosures",
        value=0.0266,
        value_date=date(2025, 12, 31),
        retrieval_date=date(2026, 5, 1),
        period_years=1,
        notes=(
            "Judo Bank 90+ DPD and impaired assets / GLA = 2.66% "
            "(Dec-25; Jun-25: 2.43%). Source: Judo 1H26 Half-Year "
            "Report, p8 + p11."
        ),
    ),
    _entry(
        source_id="JUDO_LOSS_RATE_HY26",
        publisher="Judo Bank",
        source_type=SourceType.PILLAR3,
        data_type=DataType.LGD,
        asset_class="corporate_sme",
        url="https://www.judo.bank/regulatory-disclosures",
        value=0.0052,
        value_date=date(2025, 12, 31),
        retrieval_date=date(2026, 5, 1),
        period_years=1,
        notes=(
            "Judo Bank annualised losses ratio = 0.52% of GLA (Dec-25; "
            "Jun-25: 0.35%). Realised-loss style indicator, not a "
            "Basel LGD. Source: Judo 1H26 Half-Year Report, p11."
        ),
    ),
    _entry(
        source_id="LATROBE_ANNUAL_IMPAIRMENT_2025",
        publisher="La Trobe Financial",
        source_type=SourceType.LISTED_PEER,
        data_type=DataType.LGD,
        asset_class="residential_mortgage_specialist",
        url="https://www.latrobefinancial.com.au/investments/forms-library/",
        value=0.001,
        value_date=date(2025, 12, 31),
        retrieval_date=date(2026, 5, 1),
        period_years=1,
        notes=(
            "La Trobe annualised asset impairment as % of AUM (12-month "
            "Term Account 0.10%). Realised-loss style indicator. Source: "
            "La Trobe Investment Snapshot & Metrics, p1."
        ),
    ),
]


_REALITY_CHECK_ENTRIES: list[BenchmarkEntry] = (
    _APRA_QPEX_CRE_NPL
    + _APRA_QUARTERLY_NPL
    + _RBA_FSR_AGGREGATES
    + _SP_SPIN
    + _BIG4_PILLAR3_CRE
    + _QUALITAS_COMMENTARY
    + _METRICS_CREDIT_COMMENTARY
    + _NON_BANK_DISCLOSURES
)


SEED_ENTRIES: list[BenchmarkEntry] = (
    _RESIDENTIAL
    + _CORPORATE_SME
    + _BRIDGING_RESIDENTIAL
    + _DEVELOPMENT
    + _INVOICE_FINANCE
    + _WORKING_CAPITAL
    + _REALITY_CHECK_ENTRIES
)


def load_seed_data(registry: BenchmarkRegistry) -> int:
    """Insert every SEED_ENTRIES row into the registry. Returns count."""
    for entry in SEED_ENTRIES:
        registry.add(entry)
    return len(SEED_ENTRIES)
