"""Two-stage adjustment chain: shared definition alignment -> institution-specific.

Architecture (per plan §4):

    raw_value
        |
        v
    [Stage 1] _AdjustmentBase._apply_definition_alignment()
        - APRA impaired -> PD equivalent
        - illion BFRI -> default rate
        - rating-agency global -> AU IG / sub-IG
        |
        v
    [Stage 2] subclass.stage2()
        - BankAdjustment: peer_mix, geography_ig
        - PrivateCreditAdjustment: selection_bias, LVR, industry, trading_history,
                                   unsecured, invoice concentration overlay
        |
        v
    AdjustmentResult(steps=Stage1+Stage2, final_multiplier=product, adjusted_value)

`AdjustmentEngine(institution_type)` is the public entry point. It persists
non-what-if results to the `adjustments` + `audit_log` tables; what-if runs
return an AdjustmentResult with scenario_label='what_if' and skip persistence.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

import yaml
from sqlalchemy.engine import Engine

from src.db import Adjustment, AuditLog, make_session_factory
from src.models import (
    AdjustmentResult,
    AdjustmentStep,
    InstitutionType,
    SourceType,
)


DEFAULT_PROFILES_PATH = (
    Path(__file__).parent.parent / "config" / "adjustment_profiles.yaml"
)


def load_adjustment_profiles(
    path: Path | str | None = None,
) -> dict[str, Any]:
    """Load and return the adjustment_profiles.yaml contents."""
    resolved = Path(path) if path else DEFAULT_PROFILES_PATH
    with open(resolved, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _effective(what_if: Optional[dict], key: str, default: float) -> float:
    """`what_if` overrides take precedence over computed/default values."""
    if what_if and key in what_if:
        return float(what_if[key])
    return float(default)


def _select_concentration(share: float, overlay: dict) -> tuple[float, str]:
    """Bucket a debtor concentration share into one of the overlay tiers."""
    if share < 0.10:
        return overlay["below_10pct"], "top debtor <10%"
    if share < 0.25:
        return overlay["10_to_25pct"], "top debtor 10-25%"
    if share < 0.50:
        return overlay["25_to_50pct"], "top debtor 25-50%"
    return overlay["above_50pct"], "top debtor >=50%"


# ---------------------------------------------------------------------------
# Stage 1: shared definition alignment
# ---------------------------------------------------------------------------

class _AdjustmentBase:
    """Base class carrying Stage 1 logic — shared between bank and PC chains."""

    def __init__(self, profiles: dict[str, Any]) -> None:
        self._profiles = profiles

    def _apply_definition_alignment(
        self,
        source_type: SourceType,
        *,
        grade: Optional[str] = None,
        what_if: Optional[dict] = None,
    ) -> list[AdjustmentStep]:
        """Return Stage 1 steps. Empty when no alignment rule matches the source type."""
        da = self._profiles["definition_alignment"]
        steps: list[AdjustmentStep] = []

        if source_type == SourceType.APRA_ADI:
            cfg = da["apra_impaired_to_pd"]
            mult = _effective(what_if, "apra_impaired_to_pd", cfg["multiplier"])
            steps.append(AdjustmentStep(
                name="definition_alignment_apra_impaired_to_pd",
                multiplier=mult,
                source_reference=cfg["source_reference"],
                rationale=cfg["rationale"],
            ))

        elif source_type == SourceType.BUREAU:
            cfg = da["illion_bfri_to_default_rate"]
            mult = _effective(what_if, "illion_bfri_to_default_rate", cfg["default"])
            steps.append(AdjustmentStep(
                name="definition_alignment_bfri_to_default_rate",
                multiplier=mult,
                source_reference=cfg["source_reference"],
                rationale=cfg["rationale"],
            ))

        elif source_type == SourceType.RATING_AGENCY:
            if grade == "sub_IG":
                cfg = da["rating_agency_global_to_au_sub_ig"]
                mult = _effective(
                    what_if, "rating_agency_global_to_au_sub_ig", cfg["multiplier"],
                )
            else:  # default to IG treatment
                cfg = da["rating_agency_global_to_au_ig"]
                mult = _effective(
                    what_if, "rating_agency_global_to_au_ig", cfg["default"],
                )
            steps.append(AdjustmentStep(
                name="definition_alignment_rating_agency_global_to_au",
                multiplier=mult,
                source_reference=cfg["source_reference"],
                rationale=cfg["rationale"],
            ))

        # pillar3 / apra_adi-non-impaired / regulatory / icc_trade / industry_body /
        # listed_peer / rba / insolvency / bureau (non-BFRI) -> no Stage 1 rule
        return steps


# ---------------------------------------------------------------------------
# Stage 2: bank
# ---------------------------------------------------------------------------

class BankAdjustment(_AdjustmentBase):
    """Stage 2 for banks: peer_mix (always), geography_ig (only rating-agency sources)."""

    def stage2(
        self,
        source_type: SourceType,
        *,
        peer_mix: Optional[float] = None,
        geography_ig: Optional[float] = None,
        what_if: Optional[dict] = None,
    ) -> list[AdjustmentStep]:
        cfg = self._profiles["bank_stage2"]
        steps: list[AdjustmentStep] = []

        pm = _effective(
            what_if, "peer_mix",
            peer_mix if peer_mix is not None else cfg["peer_mix"]["default"],
        )
        steps.append(AdjustmentStep(
            name="peer_mix",
            multiplier=pm,
            source_reference=cfg["peer_mix"]["source_reference"],
            rationale=cfg["peer_mix"]["rationale"],
        ))

        # Geography adjustment only meaningful for global rating-agency data.
        if source_type == SourceType.RATING_AGENCY:
            gi = _effective(
                what_if, "geography_ig",
                geography_ig if geography_ig is not None else cfg["geography_ig"]["default"],
            )
            steps.append(AdjustmentStep(
                name="geography_ig",
                multiplier=gi,
                source_reference=cfg["geography_ig"]["source_reference"],
                rationale=cfg["geography_ig"]["rationale"],
            ))

        return steps


# ---------------------------------------------------------------------------
# Stage 2: private credit
# ---------------------------------------------------------------------------

class PrivateCreditAdjustment(_AdjustmentBase):
    """Stage 2 for private credit: product-driven multipliers + concentration overlay."""

    def stage2(
        self,
        *,
        product: str,
        selection_bias: Optional[float] = None,
        lvr_adj: Optional[float] = None,
        industry: Optional[float] = None,
        trading_history_adj: Optional[float] = None,
        unsecured: Optional[float] = None,
        debtor_concentration: Optional[float] = None,
        what_if: Optional[dict] = None,
    ) -> list[AdjustmentStep]:
        pc_cfg = self._profiles["private_credit_stage2"]
        if product not in pc_cfg:
            raise ValueError(
                f"Unknown PC product: {product!r}. Known: {sorted(pc_cfg.keys())}"
            )
        prod_cfg = pc_cfg[product]
        steps: list[AdjustmentStep] = []

        if "selection_bias" in prod_cfg:
            sb = _effective(
                what_if, "selection_bias",
                selection_bias if selection_bias is not None
                else prod_cfg["selection_bias"]["default"],
            )
            steps.append(AdjustmentStep(
                name="selection_bias",
                multiplier=sb,
                source_reference="Plan §4 / differences doc",
                rationale=f"PC borrowers rejected by banks (product={product})",
            ))

        if "lvr" in prod_cfg:
            lv = _effective(
                what_if, "lvr_adj",
                lvr_adj if lvr_adj is not None else prod_cfg["lvr"]["default"],
            )
            steps.append(AdjustmentStep(
                name="lvr_adj",
                multiplier=lv,
                source_reference="LGD guide §3",
                rationale=f"LVR uplift for {product}",
            ))

        if "industry" in prod_cfg:
            ind = _effective(
                what_if, "industry",
                industry if industry is not None else prod_cfg["industry"]["default"],
            )
            # Include the step even at 1.0 when the product config declares an
            # industry range — it's a mandatory ANZSIC review point for governance.
            steps.append(AdjustmentStep(
                name="industry",
                multiplier=ind,
                source_reference="ANZSIC mapping",
                rationale=f"Industry risk (ANZSIC) for {product}",
            ))

        if trading_history_adj is not None or (what_if and "trading_history_adj" in what_if):
            th_cfg = self._profiles["trading_history_adj"]
            th = _effective(
                what_if, "trading_history_adj",
                trading_history_adj if trading_history_adj is not None
                else th_cfg["default"],
            )
            steps.append(AdjustmentStep(
                name="trading_history_adj",
                multiplier=th,
                source_reference=th_cfg["source_reference"],
                rationale=th_cfg["rationale"],
            ))

        if "unsecured" in prod_cfg:
            un = _effective(
                what_if, "unsecured",
                unsecured if unsecured is not None else prod_cfg["unsecured"]["default"],
            )
            steps.append(AdjustmentStep(
                name="unsecured",
                multiplier=un,
                source_reference="Plan §4 PC table",
                rationale=f"Unsecured multiplier for {product}",
            ))

        if product == "invoice_finance":
            overlay = self._profiles["invoice_concentration_overlay"]
            if what_if and "concentration_overlay" in what_if:
                co_mult = float(what_if["concentration_overlay"])
                co_note = "what-if override"
            elif debtor_concentration is None:
                co_mult = overlay["default_when_absent"]
                co_note = "default (no debtor_concentration passed)"
            else:
                co_mult, co_note = _select_concentration(debtor_concentration, overlay)
            steps.append(AdjustmentStep(
                name="concentration_overlay",
                multiplier=co_mult,
                source_reference="adjustment_profiles.yaml invoice_concentration_overlay",
                rationale=f"Invoice finance concentration ({co_note})",
            ))

        return steps


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

class AdjustmentEngine:
    """Public entry point. Instantiate once per institution type; call adjust()."""

    def __init__(
        self,
        institution_type: InstitutionType,
        engine: Engine,
        profiles: Optional[dict[str, Any]] = None,
        actor: str = "system",
    ) -> None:
        self._inst = institution_type
        self._engine = engine
        self._factory = make_session_factory(engine)
        self._actor = actor
        self._profiles = profiles if profiles is not None else load_adjustment_profiles()

        if institution_type == InstitutionType.BANK:
            self._stage2_impl: _AdjustmentBase = BankAdjustment(self._profiles)
        elif institution_type == InstitutionType.PRIVATE_CREDIT:
            self._stage2_impl = PrivateCreditAdjustment(self._profiles)
        else:
            raise ValueError(f"Unknown institution_type: {institution_type!r}")

    def adjust(
        self,
        raw_value: float,
        source_type: SourceType,
        asset_class: str,
        product: str,
        *,
        grade: Optional[str] = None,
        source_id: str = "",
        # Bank kwargs
        peer_mix: Optional[float] = None,
        geography_ig: Optional[float] = None,
        # PC kwargs
        selection_bias: Optional[float] = None,
        lvr_adj: Optional[float] = None,
        industry: Optional[float] = None,
        trading_history_adj: Optional[float] = None,
        unsecured: Optional[float] = None,
        debtor_concentration: Optional[float] = None,
        # Common
        what_if: Optional[dict] = None,
    ) -> AdjustmentResult:
        """Apply Stage 1 + Stage 2 adjustments; persist unless what_if is set."""
        # Stage 1 — shared, same call for both institutions.
        stage1 = self._stage2_impl._apply_definition_alignment(
            source_type, grade=grade, what_if=what_if,
        )

        # Stage 2 — institution-specific.
        if self._inst == InstitutionType.BANK:
            assert isinstance(self._stage2_impl, BankAdjustment)
            stage2 = self._stage2_impl.stage2(
                source_type=source_type,
                peer_mix=peer_mix,
                geography_ig=geography_ig,
                what_if=what_if,
            )
        else:
            assert isinstance(self._stage2_impl, PrivateCreditAdjustment)
            stage2 = self._stage2_impl.stage2(
                product=product,
                selection_bias=selection_bias,
                lvr_adj=lvr_adj,
                industry=industry,
                trading_history_adj=trading_history_adj,
                unsecured=unsecured,
                debtor_concentration=debtor_concentration,
                what_if=what_if,
            )

        all_steps = stage1 + stage2
        final_mult = 1.0
        for step in all_steps:
            final_mult *= step.multiplier
        adjusted = raw_value * final_mult

        result = AdjustmentResult(
            raw_value=raw_value,
            adjusted_value=adjusted,
            institution_type=self._inst,
            product=product,
            asset_class=asset_class,
            steps=all_steps,
            final_multiplier=final_mult,
            scenario_label="what_if" if what_if else None,
        )

        # Persist only non-what-if adjustments — what-if is in-memory only per plan.
        if what_if is None:
            self._persist(source_id or "anonymous", result)
        return result

    def _persist(self, source_id: str, result: AdjustmentResult) -> None:
        """Write one row to `adjustments` + one row to `audit_log`. Atomic."""
        steps_json = json.dumps([step.model_dump() for step in result.steps])
        with self._factory() as s:
            s.add(Adjustment(
                source_id=source_id,
                institution_type=result.institution_type.value,
                product=result.product,
                asset_class=result.asset_class,
                raw_value=result.raw_value,
                adjusted_value=result.adjusted_value,
                steps_json=steps_json,
            ))
            s.add(AuditLog(
                operation="adjust",
                entity_id=source_id,
                params_json=json.dumps({
                    "institution_type": result.institution_type.value,
                    "product": result.product,
                    "asset_class": result.asset_class,
                    "steps_count": len(result.steps),
                    "final_multiplier": result.final_multiplier,
                }),
                result_summary=f"raw={result.raw_value} -> adj={result.adjusted_value:.6f}",
                actor=self._actor,
            ))
            s.commit()
