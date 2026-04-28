"""Reality-check band lookup API.

Exposes per-product upper/lower bands plus the source observations
that justify them. Used by downstream credit projects (PD, LGD, ECL,
stress testing) for sanity-checking calibrated values.

The engine itself does not enforce or apply these bands. They're
metadata. Consumers decide what to do (flag, block, override with
sign-off, etc.).
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml


@dataclass(frozen=True)
class RealityCheckBand:
    product: str
    upper_band_pd: float
    lower_band_pd: float
    upper_sources: list[str]
    lower_sources: list[str]
    rationale: str


@dataclass(frozen=True)
class RealityCheckBandLibrary:
    bands_by_product: dict[str, RealityCheckBand]
    system_wide_references: dict[str, dict[str, str]]
    last_review_date: str
    next_review_due: str

    def for_product(self, product: str) -> Optional[RealityCheckBand]:
        return self.bands_by_product.get(product)

    def all_products(self) -> list[str]:
        return list(self.bands_by_product.keys())

    def all_referenced_source_ids(self) -> set[str]:
        """Every source_id mentioned by any band's upper or lower list,
        plus every source_id mentioned in system_wide_references.

        Useful for verifying that every justification source actually
        exists in `raw_observations` (test invariant)."""
        ids: set[str] = set()
        for band in self.bands_by_product.values():
            ids.update(band.upper_sources)
            ids.update(band.lower_sources)
        for ref in self.system_wide_references.values():
            sid = ref.get("source_id")
            if sid:
                ids.add(sid)
        return ids


def load_reality_check_bands(
    yaml_path: Optional[Path] = None,
) -> RealityCheckBandLibrary:
    """Load reality-check bands. Default path: config/reality_check_bands.yaml."""
    if yaml_path is None:
        yaml_path = (
            Path(__file__).resolve().parent.parent
            / "config" / "reality_check_bands.yaml"
        )
    with yaml_path.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    bands: dict[str, RealityCheckBand] = {}
    for product, body in (raw.get("reality_check_bands") or {}).items():
        bands[product] = RealityCheckBand(
            product=product,
            upper_band_pd=float(body["upper_band_pd"]),
            lower_band_pd=float(body["lower_band_pd"]),
            upper_sources=list(body.get("upper_sources") or []),
            lower_sources=list(body.get("lower_sources") or []),
            rationale=body.get("rationale", ""),
        )

    refs = raw.get("system_wide_references") or {}
    last = raw.get("last_review") or {}

    return RealityCheckBandLibrary(
        bands_by_product=bands,
        system_wide_references=refs,
        last_review_date=last.get("date", "unknown"),
        next_review_due=last.get("next_review_due", "unknown"),
    )
