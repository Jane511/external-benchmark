"""DEPRECATED — adjustment logic moved to downstream consuming projects.

This module previously contained:
  - AdjustmentEngine (Stage 1 + Stage 2 chain orchestrator)
  - BankAdjustment (peer_mix, geography_ig)
  - PrivateCreditAdjustment (selection_bias, lvr, security_type,
    asset_class, trading_history, concentration overlay)
  - load_adjustment_profiles()

All adjustment logic is now the responsibility of the consuming project
(e.g. PD workbook for PD adjustments, LGD project for LGD overlays).
The engine publishes raw, source-attributable observations only.
"""

raise ImportError(
    "src.adjustments is deprecated. "
    "Adjustment logic has been moved to downstream consuming projects "
    "(e.g. the PD workbook's compute_lra_per_product). "
    "The external benchmark engine publishes raw observations only — "
    "see src.observations for the new API."
)
