"""DEPRECATED — replaced by src.validation (raw-only flagging, no averaging).

Previously: BenchmarkTriangulator combined adjusted sources into one
segment-level benchmark via simple_average / weighted_by_years /
quality_weighted / trimmed_mean.

Now: the engine deliberately publishes only raw per-source observations.
Cross-source averaging is a use-case-specific decision that belongs in
the consuming project (e.g. the PD workbook's compute_lra_per_product
chooses how to weight sources). The engine instead flags data-quality
issues — spread, outliers, vintage staleness — without computing a
consensus number. See src.validation.compute_validation_flags.
"""

raise ImportError(
    "src.triangulation is deprecated. "
    "The engine no longer triangulates across sources. "
    "Use src.validation.compute_validation_flags() to surface spread / "
    "outliers / vintage flags, and let the consuming project decide how "
    "to weight raw observations from src.observations.PeerObservations."
)
