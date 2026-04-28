"""Shared base for non-bank ASX-listed lender disclosure adapters.

Each non-bank lender publishes loan-performance data in slightly different
formats (annual reports, half-yearly disclosures, ASX investor decks). The
adapters share a small amount of plumbing — canonical column set,
segment-alias lookup against ingestion/segment_mapping.yaml, conversion
from rows to RawObservation — so the per-source files can focus on the
parsing that's actually source-specific.

NO adjustment of any kind happens here or in the subclasses. Definitions
are mapped to canonical segment names (so "Commercial real estate" and
"CRE lending" both land under "commercial_property"), but the published
PD/LGD value is reported as-is. Per Brief 1, the engine publishes raw,
source-attributable observations only.
"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml

from ingestion.adapters.base import AbstractAdapter
from src.models import RawObservation, SourceType


CANONICAL_OBSERVATION_COLUMNS: list[str] = [
    "source_id",
    "source_type",
    "segment",
    "product",
    "parameter",
    "data_definition_class",
    "value",
    "as_of_date",
    "reporting_basis",
    "methodology_note",
    "sample_size_n",
    "period_start",
    "period_end",
    "source_url",
    "page_or_table_ref",
]


_DEFAULT_MAPPING_PATH = (
    Path(__file__).resolve().parent.parent / "segment_mapping.yaml"
)


@lru_cache(maxsize=1)
def _load_segment_mapping(path: str = str(_DEFAULT_MAPPING_PATH)) -> dict:
    """Cached load of segment_mapping.yaml."""
    p = Path(path)
    if not p.exists():
        return {"canonical_segments": {}}
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {"canonical_segments": {}}


def map_segment(source_id: str, raw_label: str, *, mapping_path: Optional[Path] = None) -> Optional[str]:
    """Return the canonical segment ID for ``raw_label`` published by ``source_id``.

    Returns None when no mapping matches. Adapters typically log and skip
    unmapped rows rather than guess — adding a new alias to the YAML is
    cheaper than producing a wrong observation.

    Match is case-insensitive substring on the alias list.
    """
    mapping = _load_segment_mapping(
        str(mapping_path) if mapping_path else str(_DEFAULT_MAPPING_PATH)
    )
    sid = source_id.lower()
    needle = raw_label.strip().lower()
    if not needle:
        return None
    for canonical, cfg in mapping.get("canonical_segments", {}).items():
        aliases_by_source = (cfg or {}).get("aliases", {}) or {}
        for alias in aliases_by_source.get(sid, []):
            if alias.strip().lower() in needle or needle in alias.strip().lower():
                return canonical
    return None


def rows_to_observations(df: pd.DataFrame) -> list[RawObservation]:
    """Convert an adapter DataFrame (canonical columns) to RawObservation list.

    Validation is delegated to Pydantic — bad rows raise. Adapters can call
    this directly when feeding the registry.
    """
    out: list[RawObservation] = []
    for record in df.to_dict(orient="records"):
        st_value = record["source_type"]
        record["source_type"] = (
            st_value if isinstance(st_value, SourceType) else SourceType(st_value)
        )
        out.append(RawObservation(**record))
    return out


class NonBankDisclosureAdapter(AbstractAdapter):
    """Base for non-bank ASX-listed lender disclosure adapters.

    Subclass contract:
      - SOURCE_ID:         lower-case lender id (e.g. "judo")
      - SOURCE_TYPE:       SourceType.NON_BANK_LISTED (default) or override
      - REPORTING_BASIS:   string describing the publication, e.g.
                           "Annual report — credit risk section"
      - SOURCE_URL:        canonical URL of the investor relations page
      - normalise():       parse `file_path` and return a long-format
                           DataFrame with columns matching
                           CANONICAL_OBSERVATION_COLUMNS
    """

    SOURCE_ID: str = "OVERRIDE_ME"
    SOURCE_TYPE: SourceType = SourceType.NON_BANK_LISTED
    REPORTING_BASIS: str = "OVERRIDE_ME"
    SOURCE_URL: str = ""

    @property
    def source_name(self) -> str:
        return self.SOURCE_ID

    @property
    def canonical_columns(self) -> list[str]:
        return list(CANONICAL_OBSERVATION_COLUMNS)

    def map_segment(self, raw_label: str) -> Optional[str]:
        return map_segment(self.SOURCE_ID, raw_label)

    def empty_frame(self) -> pd.DataFrame:
        """Return an empty DataFrame with the canonical column set."""
        return pd.DataFrame(columns=self.canonical_columns)

    def normalise(self, file_path: Path) -> pd.DataFrame:  # pragma: no cover
        raise NotImplementedError(
            f"{type(self).__name__}.normalise must be implemented by the subclass"
        )
