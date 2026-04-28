"""Rating-agency and securitisation aggregate indices.

Distinct from per-lender Pillar 3 ingestion: these adapters consume
market-wide indices (S&P SPIN, RBA Securitisation Dataset aggregates,
RBA Financial Stability Review aggregates).

All adapters emit RawObservation rows tagged with
``SourceType.RATING_AGENCY_INDEX`` or ``SourceType.RBA_AGGREGATE``.
Consumers can filter by source_type to include / exclude the aggregates.
"""
