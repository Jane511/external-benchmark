"""Downturn calibration — PD cycle adjustment + LGD uplift.

Note: LGD decomposition (collateral x haircut x time x costs) was extracted
to `future_lgd/` and belongs in a downstream LGD model project. This module
retains PD cycle adjustment and downturn PD/LGD uplift functions only.

**Design principle:** aggregate LGD entries (`component=None`) go through
the normal adjust -> triangulate pipeline at the caller's discretion; this
module handles only the downturn-uplift arithmetic on long-run LGD rates.

Two public functions:
    pd_cycle_adjustment(base_lra, includes_stress, external_stress_rate, margin=0.55)
    lgd_downturn_uplift(long_run_lgd, product_type, custom_uplift=None) -> DownturnResult

`DownturnCalibrator` is a thin class shell exposing both functions as
methods for callers that prefer a registry-bound object.
"""
from __future__ import annotations

from typing import Optional

from src.models import DownturnResult
from src.registry import BenchmarkRegistry


# ---------------------------------------------------------------------------
# Plan §7: LGD uplift factors by product type
# ---------------------------------------------------------------------------

DEFAULT_UPLIFT_FACTORS: dict[str, float] = {
    "residential_property": 1.45,
    "commercial_property": 1.75,
    "development": 2.00,
    "residual_stock": 1.60,
    "corporate_sme_secured": 1.50,
    "corporate_sme_unsecured": 1.60,
    "trade_finance": 1.40,
    "invoice_finance": 1.50,
    "working_capital_secured": 1.50,
    "working_capital_unsecured": 1.60,
}


# ---------------------------------------------------------------------------
# PD cycle adjustment — uplift base LRA toward an external stress rate
# ---------------------------------------------------------------------------

def pd_cycle_adjustment(
    base_lra: float,
    includes_stress: bool,
    external_stress_rate: float,
    margin: float = 0.55,
) -> float:
    """Uplift a long-run average PD that doesn't span a downturn cycle.

    If `includes_stress` is True the base_lra already reflects a stress period
    and is returned unchanged. Otherwise the LRA is moved `margin` of the way
    toward `external_stress_rate` (plan §7: 0.3-0.8 range; default 0.55 midpoint).

    The uplift never drags the LRA below its input; if the external rate is
    already below base_lra (unusual), base_lra wins.
    """
    if not 0.3 <= margin <= 0.8:
        raise ValueError(f"margin must be in [0.3, 0.8], got {margin}")
    if includes_stress:
        return base_lra
    gap = max(external_stress_rate - base_lra, 0.0)
    return base_lra + margin * gap


# ---------------------------------------------------------------------------
# LGD uplift
# ---------------------------------------------------------------------------

def lgd_downturn_uplift(
    long_run_lgd: float,
    product_type: str,
    custom_uplift: Optional[float] = None,
) -> DownturnResult:
    """Apply product-specific downturn multiplier to a long-run LGD.

    Returns a DownturnResult carrying both the regulatory-capital LGD
    (= downturn_lgd) and the IFRS-9 ECL LGD (= long_run_lgd), so downstream
    consumers pick the right one for the right regulatory purpose.
    """
    if not 0.0 <= long_run_lgd <= 1.0:
        raise ValueError(f"long_run_lgd must be in [0, 1], got {long_run_lgd}")

    if custom_uplift is not None:
        if custom_uplift <= 0:
            raise ValueError(f"custom_uplift must be > 0, got {custom_uplift}")
        uplift = custom_uplift
    elif product_type in DEFAULT_UPLIFT_FACTORS:
        uplift = DEFAULT_UPLIFT_FACTORS[product_type]
    else:
        raise ValueError(
            f"Unknown product_type {product_type!r}. Known: "
            f"{sorted(DEFAULT_UPLIFT_FACTORS)} (or pass custom_uplift)."
        )

    downturn_lgd = min(long_run_lgd * uplift, 1.0)  # LGD cannot exceed 100%
    return DownturnResult(
        long_run_lgd=long_run_lgd,
        uplift=uplift,
        downturn_lgd=downturn_lgd,
        product_type=product_type,
        lgd_for_capital=downturn_lgd,   # APRA / Basel capital uses downturn LGD
        lgd_for_ecl=long_run_lgd,        # IFRS 9 ECL uses long-run LGD
    )


# ---------------------------------------------------------------------------
# Registry-backed calibrator (shell — decomposition method moved to future_lgd/)
# ---------------------------------------------------------------------------

class DownturnCalibrator:
    """Bind the stateless functions above to a registry for convenience.

    Previously also exposed `lgd_decomposition` with registry auto-lookup;
    that method and its `_lookup_component` helper were extracted to
    `future_lgd/src/lgd_decomposition.py` because facility-level LGD
    decomposition belongs in the downstream LGD model project, not the
    external benchmark engine.
    """

    def __init__(self, registry: BenchmarkRegistry) -> None:
        self._registry = registry

    def pd_cycle_adjustment(
        self,
        base_lra: float,
        includes_stress: bool,
        external_stress_rate: float,
        margin: float = 0.55,
    ) -> float:
        return pd_cycle_adjustment(
            base_lra, includes_stress, external_stress_rate, margin=margin,
        )

    def lgd_downturn_uplift(
        self,
        long_run_lgd: float,
        product_type: str,
        custom_uplift: Optional[float] = None,
    ) -> DownturnResult:
        return lgd_downturn_uplift(long_run_lgd, product_type, custom_uplift)
