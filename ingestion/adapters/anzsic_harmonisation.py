"""Resolver utility for the ANZSIC harmonisation map.

Loads ``config/anzsic_harmonisation_map.yaml`` once per process and
exposes lookups from per-bank published industry labels to canonical
buckets. Used by the cross-bank integration test (Phase 3.B.2 §6) and
available to adapters that want to verify their published labels are
mapped before emit.

Phase 3.B.2 §2.2: strict matching. ``resolve()`` raises on unknown
labels — never returns a fallback. The map is the contract.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Final

import yaml


_MAP_PATH: Final[Path] = (
    Path(__file__).resolve().parents[2]
    / "config" / "anzsic_harmonisation_map.yaml"
)


class UnknownIndustryLabelError(KeyError):
    """Raised when a per-bank published label has no harmonisation entry.

    Recovery: add the label to ``config/anzsic_harmonisation_map.yaml``
    under the appropriate bank's ``per_bank_mapping`` section. Do NOT
    add a wildcard fallback — the map is the contract.
    """


@dataclass(frozen=True)
class CanonicalBucket:
    """One canonical bucket entry from the harmonisation map."""
    key: str
    description: str
    anzsic_divisions: tuple[str, ...]
    business_lending: bool
    pool_reason: str | None


@lru_cache(maxsize=1)
def _load_map() -> dict:
    with _MAP_PATH.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _normalise(label: str) -> str:
    return " ".join(label.strip().split()).lower()


def canonical_buckets() -> dict[str, CanonicalBucket]:
    """Return all canonical buckets keyed by their key."""
    raw = _load_map().get("canonical_buckets", {}) or {}
    out: dict[str, CanonicalBucket] = {}
    for k, v in raw.items():
        out[k] = CanonicalBucket(
            key=k,
            description=v.get("description", ""),
            anzsic_divisions=tuple(v.get("anzsic_divisions") or ()),
            # business_lending defaults to True for ANZSIC buckets unless
            # explicitly tagged false (e.g. consumer buckets).
            business_lending=bool(v.get("business_lending", True)),
            pool_reason=v.get("pool_reason"),
        )
    return out


def resolve(bank_code: str, label: str) -> str:
    """Resolve ``(bank, label)`` → canonical bucket key.

    Case-insensitive, whitespace-normalised exact match against the
    bank's ``labels`` map. Raises :class:`UnknownIndustryLabelError`
    when no match exists.
    """
    m = _load_map()
    per_bank = m.get("per_bank_mapping", {}) or {}
    if bank_code not in per_bank:
        raise UnknownIndustryLabelError(
            f"unknown bank_code {bank_code!r}; configured banks: "
            f"{sorted(per_bank)}"
        )
    labels = per_bank[bank_code].get("labels", {}) or {}
    norm_lookup = {_normalise(k): v for k, v in labels.items()}
    hit = norm_lookup.get(_normalise(label))
    if hit is None:
        raise UnknownIndustryLabelError(
            f"{bank_code!r}: no harmonisation entry for label {label!r}; "
            f"add it under per_bank_mapping.{bank_code}.labels in "
            f"{_MAP_PATH.name} (no silent fallback)"
        )
    return hit


def resolve_rba_d14_1(label: str) -> str:
    """Resolve an RBA D14.1 industry label to a canonical bucket key.

    Phase 3.C addition. The harmonisation map carries D14.1 routings
    in a separate ``rba_d14_1_mapping`` block (rather than in
    ``per_bank_mapping``) because D14.1 is a system-wide source, not a
    bank. This function is the parallel of :func:`resolve` for D14.1
    rows.
    """
    m = _load_map()
    block = m.get("rba_d14_1_mapping", {}) or {}
    labels = block.get("labels", {}) or {}
    norm_lookup = {_normalise(k): v for k, v in labels.items()}
    hit = norm_lookup.get(_normalise(label))
    if hit is None:
        raise UnknownIndustryLabelError(
            f"rba_d14_1: no harmonisation entry for label {label!r}; "
            f"add it under rba_d14_1_mapping.labels in "
            f"{_MAP_PATH.name} (no silent fallback)"
        )
    return hit


def is_business_lending(canonical_key: str) -> bool:
    """Whether a canonical key represents a business-lending bucket.

    Used by the cross-bank integration test to assert that consumer
    rows do not silently route to ``business_lending_anzsic_*``.
    """
    bucket = canonical_buckets().get(canonical_key)
    if bucket is None:
        raise UnknownIndustryLabelError(
            f"unknown canonical key {canonical_key!r}; defined keys: "
            f"{sorted(canonical_buckets())}"
        )
    return bucket.business_lending
