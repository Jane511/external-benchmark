"""ScrapedDataPoint -> BenchmarkEntry conversion.

Single source of truth for the mapping rules between scraper output and
core registry format. Each rule (value normalisation, asset-class label,
source-type enum, data-type inference, source_id generation, quality
score) lives here so scrapers stay thin.
"""
from __future__ import annotations

from typing import Any, Optional

from ingestion.base import ScrapedDataPoint
from src.models import (
    BenchmarkEntry,
    Component,
    Condition,
    DataType,
    QualityScore,
    SourceType,
)


# ---------------------------------------------------------------------------
# Value unit normalisation
# ---------------------------------------------------------------------------

def normalize_value(raw_value: float, raw_unit: str) -> float:
    """Return the value as a plain number in the unit the core engine expects.

    Engine conventions (per plan):
      - PD / LGD / default_rate / impaired_ratio / recovery_rate: ratio in [0, 1]
      - time_to_recovery: months (positive number)
      - discount_rate: annual decimal
    """
    unit = raw_unit.lower().strip()
    if unit == "ratio" or unit == "decimal":
        return float(raw_value)
    if unit == "percent" or unit == "percentage":
        return float(raw_value) / 100.0
    if unit in ("basis_points", "bps"):
        return float(raw_value) / 10_000.0
    if unit == "months":
        return float(raw_value)
    if unit == "dollars":
        return float(raw_value)
    raise ValueError(f"Unknown raw_unit: {raw_unit!r}")


# ---------------------------------------------------------------------------
# Publisher / source-type mapping
# ---------------------------------------------------------------------------

_PUBLISHER_TO_SOURCE_TYPE: dict[str, SourceType] = {
    "APRA": SourceType.APRA_ADI,
    "CBA": SourceType.PILLAR3,
    "Commonwealth Bank of Australia": SourceType.PILLAR3,
    "NAB": SourceType.PILLAR3,
    "National Australia Bank": SourceType.PILLAR3,
    "WBC": SourceType.PILLAR3,
    "Westpac Banking Corporation": SourceType.PILLAR3,
    "ANZ": SourceType.PILLAR3,
    "Australia and New Zealand Banking Group": SourceType.PILLAR3,
    "illion": SourceType.BUREAU,
    "S&P": SourceType.RATING_AGENCY,
    "S&P Global Ratings": SourceType.RATING_AGENCY,
    "Moody's": SourceType.RATING_AGENCY,
    "CoreLogic": SourceType.INDUSTRY_BODY,
    "CoreLogic Australia": SourceType.INDUSTRY_BODY,
    "JLL": SourceType.INDUSTRY_BODY,
    "JLL Australia": SourceType.INDUSTRY_BODY,
    "AFIA": SourceType.INDUSTRY_BODY,
    "RBA": SourceType.RBA,
    "La Trobe Financial": SourceType.LISTED_PEER,
    "Qualitas": SourceType.LISTED_PEER,
}


def map_source_type(publisher: str) -> SourceType:
    """Exact or prefix match; raises ValueError on unknown publisher."""
    if publisher in _PUBLISHER_TO_SOURCE_TYPE:
        return _PUBLISHER_TO_SOURCE_TYPE[publisher]
    for key, st in _PUBLISHER_TO_SOURCE_TYPE.items():
        if publisher.startswith(key):
            return st
    raise ValueError(
        f"Unknown publisher {publisher!r}. Add it to transform._PUBLISHER_TO_SOURCE_TYPE."
    )


# ---------------------------------------------------------------------------
# Asset class label normalisation
# ---------------------------------------------------------------------------

# Baseline mapping — can be overridden per source via `sources.yaml`
# `asset_class_mapping` block, passed into scraped_to_entry() as override_map.
_ASSET_CLASS_BASELINE: dict[str, str] = {
    "residential": "residential_mortgage",
    "residential mortgage": "residential_mortgage",
    "residential mortgages": "residential_mortgage",
    "residential mortgages - owner occupied": "residential_mortgage",
    "residential mortgages - investor": "residential_mortgage",
    "commercial": "commercial_property_investment",
    "commercial property": "commercial_property_investment",
    "commercial property investment": "commercial_property_investment",
    "cre": "commercial_property_investment",
    "cre investment": "commercial_property_investment",
    "corporate": "corporate_sme",
    "corporate sme": "corporate_sme",
    "corporate (incl. sme corporate)": "corporate_sme",
    "sme": "corporate_sme",
    "bridging residential": "bridging_residential",
    "bridging commercial": "bridging_commercial",
    "development": "development",
    "invoice finance": "invoice_finance",
    "trade finance": "trade_finance",
    "working capital unsecured": "working_capital_unsecured",
    "working capital secured": "working_capital_secured",
}


def map_asset_class(
    raw_label: str,
    override_map: Optional[dict[str, str]] = None,
) -> str:
    """Normalise a raw asset-class label to the engine's canonical form.

    Lookup order: override_map (case-insensitive) → baseline → raw.lower().
    Falls through to the lowercased raw label if no mapping matches so
    unknown segments surface as distinct values rather than silently coalescing.
    """
    key = raw_label.strip().lower()
    if override_map:
        lowered_override = {k.lower(): v for k, v in override_map.items()}
        if key in lowered_override:
            return lowered_override[key]
    if key in _ASSET_CLASS_BASELINE:
        return _ASSET_CLASS_BASELINE[key]
    return key.replace(" ", "_")


# ---------------------------------------------------------------------------
# Data-type inference
# ---------------------------------------------------------------------------

_METRIC_TO_DATA_TYPE: dict[str, DataType] = {
    "pd": DataType.PD,
    "probability_of_default": DataType.PD,
    "lgd": DataType.LGD,
    "loss_given_default": DataType.LGD,
    "default_rate": DataType.DEFAULT_RATE,
    "90dpd": DataType.IMPAIRED_RATIO,
    "90_dpd": DataType.IMPAIRED_RATIO,
    "impaired": DataType.IMPAIRED_RATIO,
    "impaired_ratio": DataType.IMPAIRED_RATIO,
    "npl": DataType.IMPAIRED_RATIO,
    "non_performing": DataType.IMPAIRED_RATIO,
    "recovery_rate": DataType.RECOVERY_RATE,
    "recovery": DataType.RECOVERY_RATE,
    "failure_rate": DataType.FAILURE_RATE,
    "failure": DataType.FAILURE_RATE,
    "supervisory": DataType.SUPERVISORY_VALUE,
    "supervisory_value": DataType.SUPERVISORY_VALUE,
    "regulatory_floor": DataType.SUPERVISORY_VALUE,
}


def infer_data_type(metric_hint: str) -> DataType:
    key = metric_hint.strip().lower()
    if key in _METRIC_TO_DATA_TYPE:
        return _METRIC_TO_DATA_TYPE[key]
    raise ValueError(
        f"Cannot infer data_type from metric hint {metric_hint!r}. "
        f"Known hints: {sorted(_METRIC_TO_DATA_TYPE)}."
    )


# ---------------------------------------------------------------------------
# Deterministic source_id generation
# ---------------------------------------------------------------------------

def generate_source_id(
    publisher: str,
    asset_class: str,
    data_type: DataType,
    value_date,
    *,
    metric_code: str = "",
    period_code: Optional[str] = None,
) -> str:
    """Return a stable source_id so subsequent refreshes target the same row.

    `metric_code` disambiguates when multiple distinct metrics share the same
    (publisher, asset_class, data_type) — e.g. APRA 90DPD vs NPL both feed
    `IMPAIRED_RATIO` and would otherwise collide.

    `period_code` overrides the default quarter slug. Pillar 3 uses this to
    produce fiscal-year codes like "FY2025" or "H1FY2025" instead of "2025Q2".
    """
    publisher_slug = publisher.upper().replace(" ", "_").replace("&", "AND")
    period_slug = period_code if period_code else _period_slug(value_date)
    if metric_code:
        metric_slug = metric_code.upper().replace(" ", "_")
        return (
            f"{publisher_slug}_{asset_class.upper()}_{metric_slug}_"
            f"{data_type.value.upper()}_{period_slug}"
        )
    return (
        f"{publisher_slug}_{asset_class.upper()}_"
        f"{data_type.value.upper()}_{period_slug}"
    )


def _period_slug(value_date) -> str:
    month = value_date.month
    if month <= 3:
        quarter = 1
    elif month <= 6:
        quarter = 2
    elif month <= 9:
        quarter = 3
    else:
        quarter = 4
    return f"{value_date.year}Q{quarter}"


# ---------------------------------------------------------------------------
# Main conversion
# ---------------------------------------------------------------------------

def scraped_to_entry(
    point: ScrapedDataPoint,
    *,
    override_asset_class_map: Optional[dict[str, str]] = None,
    default_quality: QualityScore = QualityScore.HIGH,
) -> BenchmarkEntry:
    """Transform a raw ScrapedDataPoint into a registry-ready BenchmarkEntry.

    Deterministic: same input always produces the same source_id, so running
    a scraper twice with the same underlying data yields the same BenchmarkEntry.
    """
    normalized_value = normalize_value(point.raw_value, point.raw_unit)
    source_type = map_source_type(point.publisher)
    asset_class = map_asset_class(point.asset_class_raw, override_asset_class_map)

    metric_hint = point.metadata.get("data_type_hint") or point.source_name
    data_type = infer_data_type(metric_hint)

    # Use `metric_column` (if the scraper attached one) as the disambiguator.
    metric_code = str(point.metadata.get("metric_column", ""))
    # Optional fiscal/period override (e.g. "FY2025") — Pillar 3 uses this.
    period_code = point.metadata.get("period_code") or None

    # source_id_override wins when importers need a non-standard layout
    # (CoreLogic uses region + property + component + condition, not
    # publisher + asset_class + data_type).
    source_id = point.metadata.get("source_id_override")
    if not source_id:
        source_id = generate_source_id(
            point.publisher, asset_class, data_type, point.value_date,
            metric_code=metric_code,
            period_code=period_code,
        )

    # Condition / component hints from metadata (optional). Invalid strings
    # fall through to None rather than raising — importers are trusted to
    # emit engine-compatible values.
    condition = _resolve_condition(point.metadata.get("condition"))
    component = _resolve_component(point.metadata.get("component"))

    return BenchmarkEntry(
        source_id=source_id,
        publisher=point.publisher,
        source_type=source_type,
        data_type=data_type,
        asset_class=asset_class,
        value=normalized_value,
        value_date=point.value_date,
        period_years=point.period_years,
        geography=point.geography,
        url=point.url,
        retrieval_date=point.retrieval_date,
        quality_score=default_quality,
        notes=_build_notes(point),
        condition=condition,
        component=component,
    )


def _resolve_condition(hint: Any) -> Optional[Condition]:
    if hint is None:
        return None
    try:
        return Condition(str(hint).strip().lower())
    except ValueError:
        return None


def _resolve_component(hint: Any) -> Optional[Component]:
    if hint is None:
        return None
    try:
        return Component(str(hint).strip().lower())
    except ValueError:
        return None


def _build_notes(point: ScrapedDataPoint) -> str:
    """Include quality indicators (coverage, sample size) in the notes field."""
    parts: list[str] = []
    if point.quality_indicators:
        parts.append(
            "; ".join(f"{k}={v}" for k, v in point.quality_indicators.items())
        )
    return " | ".join(parts)
