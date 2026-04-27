"""Tests for src.observations.PeerObservations — raw API surface."""
from __future__ import annotations

from datetime import date, timedelta

import pytest

from src.db import create_engine_and_schema
from src.models import RawObservation, SourceType
from src.observations import PeerObservations
from src.registry import BenchmarkRegistry


@pytest.fixture()
def registry_with_observations() -> BenchmarkRegistry:
    engine = create_engine_and_schema(":memory:")
    reg = BenchmarkRegistry(engine, actor="test")
    today = date(2026, 4, 27)
    reg.add_observations([
        RawObservation(
            source_id="cba", source_type=SourceType.BANK_PILLAR3,
            segment="commercial_property", parameter="pd",
            value=0.025, as_of_date=today - timedelta(days=30),
            reporting_basis="Pillar 3 trailing 4-quarter average",
            methodology_note="CR6 EAD-weighted Average PD",
            page_or_table_ref="CR6 row 4",
        ),
        RawObservation(
            source_id="nab", source_type=SourceType.BANK_PILLAR3,
            segment="commercial_property", parameter="pd",
            value=0.028, as_of_date=today - timedelta(days=60),
            reporting_basis="Pillar 3 trailing 4-quarter average",
            methodology_note="CR6 EAD-weighted Average PD",
        ),
        RawObservation(
            source_id="judo", source_type=SourceType.NON_BANK_LISTED,
            segment="commercial_property", parameter="pd",
            value=0.045, as_of_date=today - timedelta(days=90),
            reporting_basis="Half-yearly results — Pillar 3 equivalent",
            methodology_note="Average PD on commercial real estate book",
        ),
        RawObservation(
            source_id="cba", source_type=SourceType.BANK_PILLAR3,
            segment="residential_mortgage", parameter="pd",
            value=0.005, as_of_date=today - timedelta(days=30),
            reporting_basis="Pillar 3 trailing 4-quarter average",
            methodology_note="Residential mortgage PD",
        ),
    ])
    return reg


def test_for_segment_returns_observations_with_validation_flags(registry_with_observations):
    peer = PeerObservations(registry_with_observations, today=date(2026, 4, 27))
    obs_set = peer.for_segment("commercial_property")
    assert obs_set.segment == "commercial_property"
    assert obs_set.n_sources == 3
    assert {o.source_id for o in obs_set.observations} == {"cba", "nab", "judo"}
    # Validation flags computed alongside, no consensus value
    assert obs_set.validation_flags.bank_vs_nonbank_ratio is not None
    assert obs_set.validation_flags.bank_vs_nonbank_ratio > 1


def test_for_segment_filters_by_source_type(registry_with_observations):
    peer = PeerObservations(registry_with_observations, today=date(2026, 4, 27))
    big4 = peer.for_segment(
        "commercial_property", source_type=SourceType.BANK_PILLAR3,
    )
    assert {o.source_id for o in big4.observations} == {"cba", "nab"}


def test_observation_set_by_source_type_helper(registry_with_observations):
    peer = PeerObservations(registry_with_observations, today=date(2026, 4, 27))
    obs_set = peer.for_segment("commercial_property")
    assert {o.source_id for o in obs_set.by_source_type(big4_only=True)} == {"cba", "nab"}
    assert {o.source_id for o in obs_set.by_source_type(nonbank_only=True)} == {"judo"}


def test_for_segment_only_pd_default(registry_with_observations):
    peer = PeerObservations(registry_with_observations)
    # Add an LGD observation; should be filtered out by default
    registry_with_observations.add_observation(
        RawObservation(
            source_id="cba", source_type=SourceType.BANK_PILLAR3,
            segment="commercial_property", parameter="lgd",
            value=0.40, as_of_date=date(2026, 4, 1),
            reporting_basis="Pillar 3 quarterly",
            methodology_note="Average LGD CR6",
        )
    )
    pd_obs = peer.for_segment("commercial_property")
    assert all(o.parameter == "pd" for o in pd_obs.observations)


def test_all_segments_returns_distinct_segments(registry_with_observations):
    peer = PeerObservations(registry_with_observations)
    segments = peer.all_segments()
    assert "commercial_property" in segments
    assert "residential_mortgage" in segments


def test_no_adjustment_methods_present_on_peer_observations():
    """Critical guarantee: PeerObservations exposes no adjusted-value getters."""
    forbidden = (
        "for_central_tendency", "for_logistic_recalibration",
        "for_bayesian_blending", "for_external_blending", "for_pluto_tasche",
        "triangulate", "adjust",
    )
    for name in forbidden:
        assert not hasattr(PeerObservations, name), (
            f"PeerObservations must not expose {name} — that path is the "
            "consuming project's responsibility under Brief 1."
        )
