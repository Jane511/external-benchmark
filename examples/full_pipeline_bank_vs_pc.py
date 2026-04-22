"""Flagship example — same raw CBA CRE PD, bank vs private credit chains.

Runs the pinned Tier 3 test end-to-end and prints both adjustment trails:

    Raw value:     2.500% (CBA Pillar 3 CRE 2024H2)
    Bank output:   ~2.500% (peer_mix 1.00 near-neutral, no geography step)
    PC output:     5.376% (selection_bias 1.7 x LVR 1.15 x trading_history 1.10)
    PC / Bank:     ~2.15x

This is the demonstration the `differences between bank and private credit`
plan document asks for: single engine, two configurations, same raw input,
institutionally appropriate outputs with full step-by-step provenance.

Run:
    python examples/full_pipeline_bank_vs_pc.py
"""
from __future__ import annotations

import sys
from pathlib import Path

# Allow running directly from the examples/ directory.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.adjustments import AdjustmentEngine  # noqa: E402
from src.db import create_engine_and_schema  # noqa: E402
from src.models import InstitutionType, SourceType  # noqa: E402


def _print_trail(label: str, result) -> None:
    print(f"\n{label}")
    print(f"  raw_value:      {result.raw_value:.4%}")
    print(f"  adjusted_value: {result.adjusted_value:.4%}")
    print(f"  final x:        {result.final_multiplier:.4f}")
    print(f"  scenario_label: {result.scenario_label}")
    print(f"  steps ({len(result.steps)}):")
    for step in result.steps:
        print(f"    - {step.name:<38} x {step.multiplier:<5}  {step.rationale}")


def main() -> None:
    raw_pd = 0.025
    asset_class = "commercial_property_investment"
    source_id = "CBA_PILLAR3_CRE_2024H2"

    engine = create_engine_and_schema(":memory:")

    # --- Bank path ---------------------------------------------------------
    bank_adj = AdjustmentEngine(InstitutionType.BANK, engine, actor="demo")
    bank_result = bank_adj.adjust(
        raw_value=raw_pd,
        source_type=SourceType.PILLAR3,
        asset_class=asset_class,
        product=asset_class,                 # banks ignore product
        source_id=source_id,
    )

    # --- Private credit path ----------------------------------------------
    pc_adj = AdjustmentEngine(InstitutionType.PRIVATE_CREDIT, engine, actor="demo")
    pc_result = pc_adj.adjust(
        raw_value=raw_pd,
        source_type=SourceType.PILLAR3,
        asset_class=asset_class,
        product="bridging_commercial",
        selection_bias=1.7,
        lvr_adj=1.15,
        trading_history_adj=1.10,
        source_id=source_id,
    )

    # --- Report ------------------------------------------------------------
    print("=" * 72)
    print("  External Benchmark Engine — Flagship: Bank vs Private Credit")
    print("=" * 72)
    print(f"\nInput: raw {asset_class} PD = {raw_pd:.4%}  (source: {source_id})")

    _print_trail("Bank adjustment", bank_result)
    _print_trail("Private credit adjustment", pc_result)

    ratio = pc_result.adjusted_value / bank_result.adjusted_value
    print(f"\n  PC / Bank ratio: {ratio:.2f}x")
    print(f"  Expected flagship: ~2.15x (PC = 5.3762%, Bank ~= 2.5%)")


if __name__ == "__main__":  # pragma: no cover
    main()
