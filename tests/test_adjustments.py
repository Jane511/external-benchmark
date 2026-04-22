"""Tests for src/adjustments.py — the critical Tier 3 layer.

The flagship test is the load-bearing one: CBA CRE raw PD 2.5%, applied
through the PC chain with selection_bias 1.7, LVR 1.15, trading_history 1.10,
must produce 0.053762 to within 1e-6 — three steps, no Stage 1 (Pillar 3).

Also covers:
- Definition alignment applied to BOTH institution types (BUREAU -> illion -> 1.4)
- Bank chain is near-neutral (peer_mix 1.0 default, no geography for Pillar 3)
- Invoice concentration overlay (0.30 -> 1.25; absent -> 1.10)
- What-if mode: scenario_label='what_if', no DB writes
"""
from __future__ import annotations

import json

import pytest
from sqlalchemy import func, select

from src.adjustments import AdjustmentEngine, load_adjustment_profiles
from src.db import Adjustment, AuditLog, create_engine_and_schema, make_session_factory
from src.models import InstitutionType, SourceType


@pytest.fixture()
def pc_engine():
    db_engine = create_engine_and_schema(":memory:")
    adjuster = AdjustmentEngine(InstitutionType.PRIVATE_CREDIT, db_engine, actor="test")
    factory = make_session_factory(db_engine)
    return adjuster, factory


@pytest.fixture()
def bank_engine():
    db_engine = create_engine_and_schema(":memory:")
    adjuster = AdjustmentEngine(InstitutionType.BANK, db_engine, actor="test")
    factory = make_session_factory(db_engine)
    return adjuster, factory


# ---------------------------------------------------------------------------
# PINNED FLAGSHIP TEST — non-negotiable
# ---------------------------------------------------------------------------

def test_flagship_pc_cre_adjustment(pc_engine) -> None:
    """CBA CRE 2.5% -> 5.3762% via selection_bias x LVR x trading_history (3 steps)."""
    adjuster, _ = pc_engine
    result = adjuster.adjust(
        raw_value=0.025,
        source_type=SourceType.PILLAR3,                 # Pillar 3 -> no Stage 1
        asset_class="commercial_property_investment",
        product="bridging_commercial",
        selection_bias=1.7,
        lvr_adj=1.15,
        trading_history_adj=1.10,
        source_id="CBA_PILLAR3_CRE_2024H2",
    )
    assert abs(result.adjusted_value - 0.053762) < 1e-6
    assert len(result.steps) == 3
    # Verify step names in order
    step_names = [s.name for s in result.steps]
    assert step_names == ["selection_bias", "lvr_adj", "trading_history_adj"]
    assert result.scenario_label is None  # persisted, not what-if


# ---------------------------------------------------------------------------
# Bank chain is near-neutral by default
# ---------------------------------------------------------------------------

def test_bank_residential_mortgage_near_neutral(bank_engine) -> None:
    adjuster, _ = bank_engine
    result = adjuster.adjust(
        raw_value=0.0085,
        source_type=SourceType.PILLAR3,
        asset_class="residential_mortgage",
        product="residential_mortgage",
        source_id="CBA_PILLAR3_RES_2024H2",
    )
    # Stage 1: no rule for Pillar 3
    # Stage 2: peer_mix default 1.0; no geography step (Pillar 3, not rating agency)
    assert result.final_multiplier == pytest.approx(1.0)
    assert result.adjusted_value == pytest.approx(0.0085)
    assert len(result.steps) == 1  # peer_mix only
    assert result.steps[0].name == "peer_mix"


# ---------------------------------------------------------------------------
# Definition alignment applies to BOTH institution types
# ---------------------------------------------------------------------------

def test_definition_alignment_applied_to_private_credit_for_bfri(pc_engine) -> None:
    """PC adjustment of an illion BFRI value shows definition_alignment AND selection_bias."""
    adjuster, _ = pc_engine
    result = adjuster.adjust(
        raw_value=0.04,
        source_type=SourceType.BUREAU,               # triggers BFRI Stage 1
        asset_class="corporate_sme",
        product="working_capital_unsecured",
        selection_bias=2.0,
        industry=1.0,   # explicit, matches default
    )
    step_names = [s.name for s in result.steps]
    assert "definition_alignment_bfri_to_default_rate" in step_names
    assert "selection_bias" in step_names
    # Stage 1 should come first
    assert step_names[0] == "definition_alignment_bfri_to_default_rate"
    assert result.steps[0].multiplier == pytest.approx(1.4)


def test_definition_alignment_applied_to_bank_for_apra_impaired(bank_engine) -> None:
    adjuster, _ = bank_engine
    result = adjuster.adjust(
        raw_value=0.01,
        source_type=SourceType.APRA_ADI,
        asset_class="residential_mortgage",
        product="residential_mortgage",
    )
    step_names = [s.name for s in result.steps]
    assert step_names[0] == "definition_alignment_apra_impaired_to_pd"
    assert result.steps[0].multiplier == 1.5


# ---------------------------------------------------------------------------
# Invoice finance concentration overlay
# ---------------------------------------------------------------------------

def test_invoice_finance_concentration_0_30_is_1_25(pc_engine) -> None:
    adjuster, _ = pc_engine
    result = adjuster.adjust(
        raw_value=0.012,
        source_type=SourceType.INDUSTRY_BODY,
        asset_class="invoice_finance",
        product="invoice_finance",
        debtor_concentration=0.30,
    )
    overlay = next(s for s in result.steps if s.name == "concentration_overlay")
    assert overlay.multiplier == 1.25


def test_invoice_finance_no_concentration_defaults_to_1_10(pc_engine) -> None:
    adjuster, _ = pc_engine
    result = adjuster.adjust(
        raw_value=0.012,
        source_type=SourceType.INDUSTRY_BODY,
        asset_class="invoice_finance",
        product="invoice_finance",
        # debtor_concentration omitted
    )
    overlay = next(s for s in result.steps if s.name == "concentration_overlay")
    assert overlay.multiplier == 1.10


def test_invoice_finance_concentration_buckets(pc_engine) -> None:
    adjuster, _ = pc_engine
    cases = [(0.05, 1.00), (0.15, 1.10), (0.30, 1.25), (0.60, 1.40)]
    for share, expected in cases:
        r = adjuster.adjust(
            raw_value=0.01,
            source_type=SourceType.INDUSTRY_BODY,
            asset_class="invoice_finance",
            product="invoice_finance",
            debtor_concentration=share,
        )
        overlay = next(s for s in r.steps if s.name == "concentration_overlay")
        assert overlay.multiplier == expected, (
            f"share={share} expected {expected}, got {overlay.multiplier}"
        )


# ---------------------------------------------------------------------------
# What-if mode — no persistence, scenario_label set
# ---------------------------------------------------------------------------

def test_what_if_mode_does_not_write_to_db(pc_engine) -> None:
    adjuster, factory = pc_engine

    def counts():
        with factory() as s:
            adj = s.scalars(select(func.count(Adjustment.pk))).one()
            aud = s.scalars(
                select(func.count(AuditLog.pk)).where(AuditLog.operation == "adjust")
            ).one()
            return adj, aud

    before = counts()
    result = adjuster.adjust(
        raw_value=0.025,
        source_type=SourceType.PILLAR3,
        asset_class="commercial_property_investment",
        product="bridging_commercial",
        what_if={"selection_bias": 2.5, "lvr_adj": 1.3},
    )
    after = counts()

    assert result.scenario_label == "what_if"
    assert before == after, "what-if must not write to adjustments or audit_log"


def test_persisted_adjustment_writes_adjustments_and_audit_rows(pc_engine) -> None:
    adjuster, factory = pc_engine
    adjuster.adjust(
        raw_value=0.025,
        source_type=SourceType.PILLAR3,
        asset_class="commercial_property_investment",
        product="bridging_commercial",
        selection_bias=1.7, lvr_adj=1.15, trading_history_adj=1.10,
        source_id="TEST_X",
    )
    with factory() as s:
        adj_rows = s.scalars(select(Adjustment)).all()
        adj_audit = s.scalars(
            select(AuditLog).where(AuditLog.operation == "adjust")
        ).all()
    assert len(adj_rows) == 1
    assert adj_rows[0].source_id == "TEST_X"
    assert adj_rows[0].adjusted_value == pytest.approx(0.053762, abs=1e-6)
    assert len(adj_audit) == 1
    steps = json.loads(adj_rows[0].steps_json)
    assert len(steps) == 3


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------

def test_pc_unknown_product_raises(pc_engine) -> None:
    adjuster, _ = pc_engine
    with pytest.raises(ValueError, match="Unknown PC product"):
        adjuster.adjust(
            raw_value=0.01,
            source_type=SourceType.PILLAR3,
            asset_class="x",
            product="not_a_real_product",
        )


def test_load_adjustment_profiles_from_default_path() -> None:
    """Sanity check — YAML parses and has the shape adjustments.py expects."""
    profiles = load_adjustment_profiles()
    assert "definition_alignment" in profiles
    assert "bank_stage2" in profiles
    assert "private_credit_stage2" in profiles
    assert "bridging_commercial" in profiles["private_credit_stage2"]
    assert profiles["invoice_concentration_overlay"]["25_to_50pct"] == 1.25
