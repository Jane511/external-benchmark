"""Adapter contract: live publisher file in, canonical DataFrame out.

Scrapers that support live files construct an adapter and pass it the XLSX
path. The adapter returns a DataFrame whose columns match the canonical
shape declared by `canonical_columns`, which the scraper then iterates
into `ScrapedDataPoint` objects — identical to the fixture path from that
point onward.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd


class AbstractAdapter(ABC):
    """Normalises a live publisher file to a canonical DataFrame shape."""

    @property
    @abstractmethod
    def source_name(self) -> str:
        """The sources.yaml key this adapter serves, e.g. ``apra_adi_performance``."""

    @property
    @abstractmethod
    def canonical_columns(self) -> list[str]:
        """Column names the adapter guarantees to produce (order not required)."""

    @abstractmethod
    def normalise(self, file_path: Path) -> pd.DataFrame:
        """Read ``file_path`` and return a long-format canonical DataFrame.

        An adapter that finds no usable data SHOULD return an empty DataFrame
        with the canonical columns rather than raise — empty is a valid
        outcome for partially-populated or brand-new releases.
        """

    def validate_output(self, df: pd.DataFrame) -> None:
        """Raise if the DataFrame is missing any canonical column."""
        missing = set(self.canonical_columns) - set(df.columns)
        if missing:
            raise ValueError(
                f"{type(self).__name__} output missing required columns: "
                f"{sorted(missing)}"
            )
