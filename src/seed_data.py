"""Seed data for the External Benchmark Engine — 8 Australian segments.

Values are authentic to Australian external-benchmark sources per plan §3:
  - Big 4 Pillar 3 disclosures (CBA, NAB, WBC, ANZ) H2 2024 snapshot
  - APRA APS 113 regulatory floors and specialised-lending slotting grades
  - ICC Trade Register (import/export LC, trade loans)
  - illion BFRI aggregate failure rates
  - AFIA invoice finance aggregates
  - S&P corporate default study
  - La Trobe bridging realised-loss disclosure

LGD **rates** (portfolio-level, no `component` field) are seeded here. LGD
**components** (haircut / time_to_recovery / workout_costs / discount_rate
entries with `component=` populated) were extracted to `future_lgd/` and
belong in a downstream LGD model project, not the benchmark engine.

Call `load_seed_data(registry)` after initialising a fresh registry to
populate all 36 entries in one pass.
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
    return BenchmarkEntry(**base)


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
# 2. Commercial property investment (Bank + PC)
# ---------------------------------------------------------------------------
_CRE_INVESTMENT = [
    _entry(
        source_id="CBA_PILLAR3_CRE_2024H2", publisher="Commonwealth Bank of Australia",
        source_type=SourceType.PILLAR3, data_type=DataType.PD,
        asset_class="commercial_property_investment",
        url="https://www.commbank.com.au/pillar3", value=0.0250,
    ),
    _entry(
        source_id="NAB_PILLAR3_CRE_2024H2", publisher="National Australia Bank",
        source_type=SourceType.PILLAR3, data_type=DataType.PD,
        asset_class="commercial_property_investment",
        url="https://www.nab.com.au/pillar3", value=0.0220,
    ),
    _entry(
        source_id="WBC_PILLAR3_CRE_2024H2", publisher="Westpac Banking Corporation",
        source_type=SourceType.PILLAR3, data_type=DataType.PD,
        asset_class="commercial_property_investment",
        url="https://www.westpac.com.au/pillar3", value=0.0260,
    ),
    _entry(
        source_id="ANZ_PILLAR3_CRE_2024H2", publisher="Australia and New Zealand Banking Group",
        source_type=SourceType.PILLAR3, data_type=DataType.PD,
        asset_class="commercial_property_investment",
        url="https://www.anz.com/pillar3", value=0.0210,
    ),
    _entry(
        source_id="APS113_CRE_LGD_FLOOR", publisher="APRA",
        source_type=SourceType.REGULATORY, data_type=DataType.SUPERVISORY_VALUE,
        asset_class="commercial_property_investment",
        url="https://www.apra.gov.au/aps-113",
        period_years=10, value=0.175,
        notes="APS 113 commercial LGD floor (10-25%, midpoint)",
    ),
]

# ---------------------------------------------------------------------------
# 3. Corporate SME (Bank + PC)
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
# 4. Bridging residential (PC)
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
# 5. Development (PC) — APS 113 slotting grades
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
# 6. Invoice finance (PC)
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
    _entry(
        source_id="ICC_TRADE_RECEIVABLES", publisher="ICC Trade Register",
        source_type=SourceType.ICC_TRADE, data_type=DataType.DEFAULT_RATE,
        asset_class="invoice_finance",
        url="https://iccwbo.org/trade-register",
        period_years=10, value=0.008,
    ),
]

# ---------------------------------------------------------------------------
# 7. Working capital unsecured (PC)
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
# 8. Trade finance (Bank + PC)
# ---------------------------------------------------------------------------
_TRADE_FINANCE = [
    _entry(
        source_id="ICC_TRADE_IMPORT_LC", publisher="ICC Trade Register",
        source_type=SourceType.ICC_TRADE, data_type=DataType.DEFAULT_RATE,
        asset_class="trade_finance",
        url="https://iccwbo.org/trade-register",
        period_years=10, value=0.0003,
        notes="ICC import LC default rate (0.01-0.05% range)",
    ),
    _entry(
        source_id="ICC_TRADE_EXPORT_LC", publisher="ICC Trade Register",
        source_type=SourceType.ICC_TRADE, data_type=DataType.DEFAULT_RATE,
        asset_class="trade_finance",
        url="https://iccwbo.org/trade-register",
        period_years=10, value=0.00015,
    ),
    _entry(
        source_id="ICC_TRADE_LOANS", publisher="ICC Trade Register",
        source_type=SourceType.ICC_TRADE, data_type=DataType.DEFAULT_RATE,
        asset_class="trade_finance",
        url="https://iccwbo.org/trade-register",
        period_years=10, value=0.004,
    ),
]


SEED_ENTRIES: list[BenchmarkEntry] = (
    _RESIDENTIAL
    + _CRE_INVESTMENT
    + _CORPORATE_SME
    + _BRIDGING_RESIDENTIAL
    + _DEVELOPMENT
    + _INVOICE_FINANCE
    + _WORKING_CAPITAL
    + _TRADE_FINANCE
)


def load_seed_data(registry: BenchmarkRegistry) -> int:
    """Insert every SEED_ENTRIES row into the registry. Returns count."""
    for entry in SEED_ENTRIES:
        registry.add(entry)
    return len(SEED_ENTRIES)
