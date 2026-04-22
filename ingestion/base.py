"""Abstract scraper base + raw-data intermediate format.

Scrapers subclass `BaseScraper` and return a list of `ScrapedDataPoint`
objects. `transform.py` converts those into `BenchmarkEntry` objects the
core registry can accept.

The intermediate format exists so each scraper can emit its natural raw
representation (percent vs ratio, raw publisher labels for asset classes)
without every scraper re-implementing the mapping logic. Keeps each
scraper thin and the mapping rules centralised.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
from typing import Any


@dataclass(frozen=True)
class ScrapedDataPoint:
    """Raw data point output by a scraper, pre-transform.

    Fields map directly to the eventual `BenchmarkEntry` shape plus a small
    amount of source-side metadata that helps `transform.py` infer the
    canonical enum values.
    """

    source_name: str                # e.g. "APRA_RESIDENTIAL_90DPD"
    publisher: str                  # e.g. "APRA"
    raw_value: float                # number as read from the source
    raw_unit: str                   # "ratio" | "percent" | "basis_points" | "months" | "dollars"
    value_date: date                # period end date from the source
    period_years: int               # how many years of history the value spans
    asset_class_raw: str            # raw label as it appears in the source
    geography: str                  # "AU" for all current sources
    url: str                        # source URL (or file path for local)
    retrieval_date: date            # date scraper ran
    quality_indicators: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


class BaseScraper(ABC):
    """Base class for all ingestion scrapers.

    Minimum contract: a scrape() that returns ScrapedDataPoints, a validate()
    that filters out obviously-wrong values, and two metadata properties for
    logging / scheduling.
    """

    @abstractmethod
    def scrape(self) -> list[ScrapedDataPoint]:
        """Download / read the source and emit raw data points."""

    @abstractmethod
    def validate(self, points: list[ScrapedDataPoint]) -> list[ScrapedDataPoint]:
        """Return only points whose values fall in plausible ranges."""

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Human-readable source name for logs and the status command."""

    @property
    @abstractmethod
    def expected_frequency_days(self) -> int:
        """How often this source publishes new data (used by refresh scheduling)."""


def plausible_value_range(value: float, lo: float, hi: float) -> bool:
    """Helper: value ∈ [lo, hi] inclusive. Scrapers use this in validate()."""
    return lo <= value <= hi
