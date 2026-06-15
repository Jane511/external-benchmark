"""Lean model-input tables for private-credit underwriting and risk.

The output contract in this module is intentionally narrow. It turns the
raw observation store into five downstream tables:

* PD inputs
* LGD inputs
* expected-loss rate inputs
* stress-testing rate inputs
* portfolio-monitor metrics

The engine still stores source attribution internally, but these tables
avoid methodology prose, inventories, caveat logs, and other report-only
material. Expected loss is published as a rate (PD x LGD); deal EAD is
owned by the underwriting or portfolio model that consumes the CSV.

The rendered report can also include a bank-by-industry monitor table
from local Big 4 Pillar 3 PDFs. That table is report-only; the default
CSV contract remains the five model-input files above.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path
from typing import Any, Iterable

from src.models import DataDefinitionClass, RawObservation
from src.reality_check import RealityCheckBandLibrary, load_reality_check_bands
from src.registry import BenchmarkRegistry
from src.source_naming import segment_label
from src.stress_scenarios import StressScenarioLibrary, load_stress_scenarios


SEGMENT_TO_PRODUCT: dict[str, str] = {
    "bridging_residential": "bridging",
    "commercial_property": "commercial_property",
    "corporate_sme": "term_loan",
    "development": "development",
    "invoice_finance": "invoice_finance",
    "residential_mortgage": "residential_mortgage",
    "residential_mortgage_specialist": "residential_mortgage_specialist",
    "sme_corporate": "term_loan",
    "working_capital_unsecured": "line_of_credit",
}

MONITOR_PARAMETERS = ("arrears", "npl", "impaired", "loss_rate")

BANK_INDUSTRY_METRICS = {
    "exposure_aud_m": "exposure_aud_m",
    "npe_aud_m": "npe_aud_m",
    "individually_assessed_provision_aud_m": "provision_aud_m",
    "write_offs_aud_m": "write_offs_aud_m",
}

BANK_INDUSTRY_SOURCES: tuple[dict[str, str], ...] = (
    {
        "bank_code": "cba",
        "bank": "CBA",
        "filename": "CBA_FY2025_Pillar3_Annual.pdf",
        "module": "ingestion.adapters.cba_pillar3_industry",
        "function": "extract_cba_industry_rows",
    },
    {
        "bank_code": "nab",
        "bank": "NAB",
        "filename": "NAB_FY2025_Pillar3_Annual.pdf",
        "module": "ingestion.adapters.nab_pillar3_industry",
        "function": "extract_nab_industry_rows",
    },
    {
        "bank_code": "wbc",
        "bank": "WBC",
        "filename": "WBC_FY2025_Pillar3_Annual.pdf",
        "module": "ingestion.adapters.wbc_pillar3_industry",
        "function": "extract_wbc_industry_rows",
    },
    {
        "bank_code": "anz",
        "bank": "ANZ",
        "filename": "ANZ_FY2025_Pillar3_Annual.pdf",
        "module": "ingestion.adapters.anz_pillar3_industry",
        "function": "extract_anz_industry_rows",
    },
)

BANK_ORDER = {
    spec["bank_code"]: index
    for index, spec in enumerate(BANK_INDUSTRY_SOURCES)
}


def build_model_input_bundle(
    registry: BenchmarkRegistry,
    *,
    library: RealityCheckBandLibrary | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Build all lean output tables from raw observations."""

    lib = library or load_reality_check_bands()
    pd_rows = build_parameter_input_rows(registry, parameter="pd")
    lgd_rows = build_parameter_input_rows(registry, parameter="lgd")
    expected_loss_rows = build_expected_loss_rows(registry)
    return {
        "pd_inputs": pd_rows,
        "lgd_inputs": lgd_rows,
        "expected_loss_inputs": expected_loss_rows,
        "stress_testing_inputs": build_stress_testing_rows(
            expected_loss_rows, library=lib,
        ),
        "portfolio_monitor_inputs": build_portfolio_monitor_rows(registry),
    }


def build_parameter_input_rows(
    registry: BenchmarkRegistry,
    *,
    parameter: str,
) -> list[dict[str, Any]]:
    """Latest numeric observation per source for one model parameter."""

    rows: list[dict[str, Any]] = []
    observations = _observations_for_model_parameter(registry, parameter)
    for obs in _latest_numeric_observations(observations):
        value = float(obs.value) if obs.value is not None else None
        if value is None:
            continue
        rows.append({
            "segment": obs.segment,
            "segment_label": segment_label(obs.segment),
            "product": _product_for(obs),
            "source_id": obs.source_id,
            "source_type": obs.source_type.value,
            f"{parameter}_decimal": _round_rate(value),
            "as_of_date": obs.as_of_date.isoformat(),
        })
    rows.sort(key=lambda r: (r["segment"], r["source_id"], r["as_of_date"]))
    return rows


def build_expected_loss_rows(registry: BenchmarkRegistry) -> list[dict[str, Any]]:
    """Build segment-level expected-loss rate rows from median PD and LGD."""

    pd_summary = _rate_summary_by_segment(registry, "pd")
    lgd_summary = _rate_summary_by_segment(registry, "lgd")
    out: list[dict[str, Any]] = []
    for key in sorted(pd_summary.keys() & lgd_summary.keys()):
        pd_row = pd_summary[key]
        lgd_row = lgd_summary[key]
        pd_value = pd_row["median_decimal"]
        lgd_value = lgd_row["median_decimal"]
        expected_loss = pd_value * lgd_value
        segment, product = key
        out.append({
            "segment": segment,
            "segment_label": segment_label(segment),
            "product": product,
            "pd_decimal": _round_rate(pd_value),
            "lgd_decimal": _round_rate(lgd_value),
            "expected_loss_rate_decimal": _round_rate(expected_loss),
            "expected_loss_rate_bps": round(expected_loss * 10000),
            "pd_source_count": pd_row["source_count"],
            "lgd_source_count": lgd_row["source_count"],
            "as_of_date": max(pd_row["as_of_date"], lgd_row["as_of_date"]),
        })
    return out


def build_stress_testing_rows(
    expected_loss_rows: Iterable[dict[str, Any]],
    *,
    library: RealityCheckBandLibrary | None = None,
    scenarios: StressScenarioLibrary | None = None,
) -> list[dict[str, Any]]:
    """Apply a base/mild/severe scenario set to model-ready PD/LGD/EAD rates.

    One row is emitted per segment x scenario. For each scenario, PD and LGD
    are multiplied by the scenario factors; a scenario flagged
    ``apply_reality_check_floor`` floors stressed PD at the product's
    reality-check upper band (config/reality_check_bands.yaml) so a stressed
    PD is never less conservative than the APS 113 / QPEX-anchored band.

    The CCF/EAD multiplier scales exposure-into-default: the EL rate is
    PD x LGD, and ``stressed_el_rate_incl_ead_decimal`` additionally applies
    the CCF multiplier so the loss reflects drawdown of undrawn limits in the
    stress (Basel CRE36.51 requires PD, LGD and EAD to be assessed).

    The mild scenario is the Basel CRE36.51 mandatory minimum (two
    consecutive quarters of zero growth); severe is a GFC-like path.
    """

    lib = library or load_reality_check_bands()
    scen_lib = scenarios or load_stress_scenarios()
    out: list[dict[str, Any]] = []
    for row in expected_loss_rows:
        base_pd = float(row["pd_decimal"])
        base_lgd = float(row["lgd_decimal"])
        product = str(row["product"])
        band = lib.for_product(product)
        pd_upper = band.upper_band_pd if band is not None else None
        for scen in scen_lib.ordered():
            stressed_pd = base_pd * scen.pd_multiplier
            if scen.apply_reality_check_floor and pd_upper is not None:
                stressed_pd = max(stressed_pd, pd_upper)
            stressed_lgd = min(1.0, base_lgd * scen.lgd_multiplier)
            stressed_el = stressed_pd * stressed_lgd
            out.append({
                "segment": row["segment"],
                "segment_label": row["segment_label"],
                "product": product,
                "scenario": scen.name,
                "scenario_label": scen.label,
                "base_pd_decimal": _round_rate(base_pd),
                "base_lgd_decimal": _round_rate(base_lgd),
                "base_expected_loss_rate_decimal": _round_rate(
                    base_pd * base_lgd,
                ),
                "pd_stress_multiplier": scen.pd_multiplier,
                "lgd_stress_multiplier": scen.lgd_multiplier,
                "ccf_stress_multiplier": scen.ccf_multiplier,
                "pd_upper_band_decimal": "" if pd_upper is None else _round_rate(pd_upper),
                "stressed_pd_decimal": _round_rate(stressed_pd),
                "stressed_lgd_decimal": _round_rate(stressed_lgd),
                "stressed_expected_loss_rate_decimal": _round_rate(stressed_el),
                "stressed_el_rate_incl_ead_decimal": _round_rate(
                    stressed_el * scen.ccf_multiplier,
                ),
                "macro_path": scen.macro_path,
                "as_of_date": row["as_of_date"],
            })
    return out


def build_reverse_stress_rows(
    expected_loss_rows: Iterable[dict[str, Any]],
    *,
    library: RealityCheckBandLibrary | None = None,
) -> list[dict[str, Any]]:
    """Reverse stress: the PD multiplier that breaches each reality-check band.

    For every segment with a configured reality-check upper PD band, report
    the multiplier ``breach_pd_multiplier = upper_band / base_pd`` — i.e. the
    factor on base PD at which the segment's stressed PD (and hence stressed
    EL) would reach its reality-check upper band. This answers "what shock
    breaks the metric?" (APS 220 reverse-stress).
    """

    lib = library or load_reality_check_bands()
    out: list[dict[str, Any]] = []
    for row in expected_loss_rows:
        base_pd = float(row["pd_decimal"])
        product = str(row["product"])
        band = lib.for_product(product)
        if band is None or base_pd <= 0:
            continue
        pd_upper = band.upper_band_pd
        out.append({
            "segment": row["segment"],
            "segment_label": row["segment_label"],
            "product": product,
            "base_pd_decimal": _round_rate(base_pd),
            "reality_check_upper_band_decimal": _round_rate(pd_upper),
            "breach_pd_multiplier": round(pd_upper / base_pd, 2),
            "as_of_date": row["as_of_date"],
        })
    return out


def build_portfolio_monitor_rows(
    registry: BenchmarkRegistry,
) -> list[dict[str, Any]]:
    """Build segment-level monitoring metrics from latest observations."""

    summaries = {
        parameter: _rate_summary_by_segment(registry, parameter)
        for parameter in MONITOR_PARAMETERS
    }
    keys: set[tuple[str, str]] = set()
    for summary in summaries.values():
        keys.update(summary)

    out: list[dict[str, Any]] = []
    for segment, product in sorted(keys):
        row: dict[str, Any] = {
            "segment": segment,
            "segment_label": segment_label(segment),
            "product": product,
        }
        source_counts = 0
        dates: list[str] = []
        for parameter in MONITOR_PARAMETERS:
            summary = summaries[parameter].get((segment, product))
            value_key = f"{parameter}_decimal"
            count_key = f"{parameter}_source_count"
            if summary is None:
                row[value_key] = ""
                row[count_key] = 0
                continue
            row[value_key] = _round_rate(summary["median_decimal"])
            row[count_key] = summary["source_count"]
            source_counts += int(summary["source_count"])
            dates.append(str(summary["as_of_date"]))
        row["monitor_source_count"] = source_counts
        row["as_of_date"] = max(dates) if dates else ""
        out.append(row)
    return out


def build_report_summary(
    registry: BenchmarkRegistry,
    *,
    library: RealityCheckBandLibrary | None = None,
    raw_data_dir: Path | str | None = None,
) -> dict[str, Any]:
    """Return metadata plus model-input tables for the rendered report."""

    observations = registry.query_observations()
    bundle = build_model_input_bundle(registry, library=library)
    bank_industry_rows = build_bank_industry_input_rows(raw_data_dir)
    dates = sorted({
        str(row.get("as_of_date"))
        for rows in (*bundle.values(), bank_industry_rows)
        for row in rows
        if row.get("as_of_date")
    })
    return {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "n_observations": len(observations),
            "n_segments": len({obs.segment for obs in observations}),
            "data_as_of_min": dates[0] if dates else "",
            "data_as_of_max": dates[-1] if dates else "",
        },
        **bundle,
        "bank_industry_inputs": bank_industry_rows,
    }


def build_bank_industry_input_rows(
    raw_data_dir: Path | str | None,
) -> list[dict[str, Any]]:
    """Build latest per-bank industry monitor rows from local Pillar 3 PDFs."""

    if raw_data_dir is None:
        return []
    root = Path(raw_data_dir)
    pillar3_dir = root if root.name.lower() == "pillar3" else root / "pillar3"
    if not pillar3_dir.exists():
        return []

    metric_rows: list[dict[str, Any]] = []
    for spec in BANK_INDUSTRY_SOURCES:
        pdf_path = pillar3_dir / spec["filename"]
        if not pdf_path.exists():
            continue
        for row in _extract_latest_bank_industry_metric_rows(pdf_path, spec):
            metric_rows.append(row)

    wide_rows = _to_wide_bank_industry_rows(metric_rows)
    wide_rows.sort(
        key=lambda row: (
            BANK_ORDER.get(str(row["bank_code"]).lower(), 99),
            str(row["industry"]).lower(),
        )
    )
    return wide_rows


def _extract_latest_bank_industry_metric_rows(
    pdf_path: Path,
    spec: dict[str, str],
) -> list[dict[str, Any]]:
    module = import_module(spec["module"])
    extractor = getattr(module, spec["function"])

    from ingestion.aggregation.pillar3_big4_aggregator import (
        intra_bank_industry_totals,
    )

    df = intra_bank_industry_totals(extractor(pdf_path))
    if df.empty:
        return []

    latest_date = max(df["as_of_date"])
    df = df[df["as_of_date"] == latest_date].copy()

    rows: list[dict[str, Any]] = []
    for _, group in df.groupby(["industry_published", "metric"], sort=False):
        metric = str(group["metric"].iloc[0])
        target_metric = BANK_INDUSTRY_METRICS.get(metric)
        if target_metric is None:
            continue

        selected = group
        if "geography" in group.columns:
            total_mask = group["geography"].astype(str).str.lower() == "total"
            if total_mask.any():
                selected = group[total_mask]

        values = [
            float(value)
            for value in selected["value_aud_m"].tolist()
            if not _is_missing(value)
        ]
        if not values or len(values) != len(selected):
            continue
        value = values[0] if len(values) == 1 else sum(values)
        rows.append({
            "bank_code": spec["bank_code"],
            "bank": spec["bank"],
            "industry": str(group["industry_published"].iloc[0]),
            "metric": target_metric,
            "value_aud_m": round(float(value), 2),
            "as_of_date": _iso_date(group["as_of_date"].iloc[0]),
        })
    return rows


def _to_wide_bank_industry_rows(
    metric_rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for row in metric_rows:
        key = (str(row["bank_code"]), str(row["industry"]))
        wide = out.setdefault(key, {
            "bank_code": row["bank_code"],
            "bank": row["bank"],
            "industry": row["industry"],
            "exposure_aud_m": "",
            "npe_aud_m": "",
            "provision_aud_m": "",
            "write_offs_aud_m": "",
            "npe_rate_decimal": "",
            "write_off_rate_decimal": "",
            "as_of_date": row["as_of_date"],
        })
        metric = str(row["metric"])
        wide[metric] = row["value_aud_m"]
        if str(row["as_of_date"]) > str(wide["as_of_date"]):
            wide["as_of_date"] = row["as_of_date"]

    for row in out.values():
        exposure = _float_or_none(row["exposure_aud_m"])
        npe = _float_or_none(row["npe_aud_m"])
        write_offs = _float_or_none(row["write_offs_aud_m"])
        row["npe_rate_decimal"] = _safe_ratio(npe, exposure)
        row["write_off_rate_decimal"] = _safe_ratio(write_offs, exposure)
    return list(out.values())


def _safe_ratio(numerator: float | None, denominator: float | None) -> float | str:
    if numerator is None or denominator is None or denominator <= 0:
        return ""
    return _round_rate(numerator / denominator)


def _float_or_none(value: object) -> float | None:
    if value in ("", None) or _is_missing(value):
        return None
    return float(value)


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    try:
        return bool(value != value)
    except Exception:
        return False


def _iso_date(value: object) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return "" if value is None else str(value)


def _latest_numeric_observations(
    observations: Iterable[RawObservation],
) -> list[RawObservation]:
    latest: dict[tuple[str, str, str, str], RawObservation] = {}
    for obs in observations:
        if obs.value is None:
            continue
        key = (obs.segment, _product_for(obs), obs.parameter, obs.source_id)
        cur = latest.get(key)
        if cur is None or obs.as_of_date > cur.as_of_date:
            latest[key] = obs
    return list(latest.values())


def _rate_summary_by_segment(
    registry: BenchmarkRegistry,
    parameter: str,
) -> dict[tuple[str, str], dict[str, Any]]:
    grouped: dict[tuple[str, str], list[RawObservation]] = defaultdict(list)
    for obs in _latest_numeric_observations(
        _observations_for_model_parameter(registry, parameter),
    ):
        grouped[(obs.segment, _product_for(obs))].append(obs)

    out: dict[tuple[str, str], dict[str, Any]] = {}
    for key, rows in grouped.items():
        values = [float(row.value) for row in rows if row.value is not None]
        if not values:
            continue
        out[key] = {
            "median_decimal": _median(values),
            "min_decimal": min(values),
            "max_decimal": max(values),
            "source_count": len(values),
            "as_of_date": max(row.as_of_date.isoformat() for row in rows),
        }
    return out


def _observations_for_model_parameter(
    registry: BenchmarkRegistry,
    parameter: str,
) -> list[RawObservation]:
    rows = registry.query_observations(parameter=parameter)
    if parameter == "lgd":
        return [row for row in rows if _is_direct_lgd(row)]
    if parameter == "loss_rate":
        proxy_rows = [
            row for row in registry.query_observations(parameter="lgd")
            if row.value is not None and not _is_direct_lgd(row)
        ]
        return rows + proxy_rows
    return rows


def _is_direct_lgd(obs: RawObservation) -> bool:
    if obs.parameter != "lgd":
        return False
    if obs.data_definition_class is DataDefinitionClass.REGULATORY_FLOOR_LGD:
        return True
    source_id = obs.source_id.upper()
    if "LGD" not in source_id:
        return False
    excluded_tokens = ("ECL", "IMPAIR", "LOSS_RATE", "REALISED_LOSS")
    return not any(token in source_id for token in excluded_tokens)


def _product_for(obs: RawObservation) -> str:
    return obs.product or SEGMENT_TO_PRODUCT.get(obs.segment, obs.segment)


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def _round_rate(value: float) -> float:
    return round(float(value), 6)
