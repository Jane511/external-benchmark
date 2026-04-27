"""DEPRECATED — replaced by src.observations (raw-only).

Previously: CalibrationFeed exposed adjusted PD values via
for_central_tendency / for_bayesian_blending / for_external_blending /
for_pluto_tasche / for_logistic_recalibration — each returning a
typed Pydantic object with the Stage 1 + Stage 2 adjustment chain
already applied.

Now: src.observations.PeerObservations.for_segment() returns the raw
per-source observations as a list, with each observation carrying its
own source, vintage, methodology note. The PD workbook's
compute_lra_per_product() takes these raw observations and applies
its own EBA MoC adjustments per Brief 2.
"""

raise ImportError(
    "src.calibration_feed is deprecated. "
    "Use src.observations.PeerObservations.for_segment() instead. "
    "It returns raw per-source observations; the consuming project "
    "owns all adjustment logic."
)
