"""Aggregation layer — combines per-bank canonical rows into Big-4 views.

Distinct from ``ingestion/adapters/`` (which produces per-bank rows).
This package consumes adapter outputs and produces aggregated rows
with explicit metadata about what was summed, what was refused, and
what was flagged.

Design rules per Phase 3.C:

- Adapters preserve disclosure shape; aggregation imposes canonical
  shape. Per-bank rows are inputs; canonical-bucket rows are outputs.
- Hard refusals, never silent fallbacks. Five refusal categories are
  enumerated in :mod:`.pillar3_big4_aggregator` errors.
- Intra-bank aggregation runs first (collapses per-bank sub-row
  dimensions); inter-bank aggregation operates only on bank-industry
  totals.
- ``contributing_banks`` is populated at this layer — adapters never
  pre-populate it.
- No synthetic dates. Per-bank ``as_of_date`` values are preserved in
  a manifest on every aggregated row.
"""
