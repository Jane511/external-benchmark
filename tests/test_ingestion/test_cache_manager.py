"""Tests for ingestion/cache_manager.py — status + clear across subdirs."""
from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.cache_manager import CacheManager


@pytest.fixture()
def manager(tmp_path: Path) -> CacheManager:
    return CacheManager(cache_base=tmp_path)


def test_empty_cache_status_reports_zero_counts_per_subdir(manager) -> None:
    status = manager.cache_status()
    assert "apra" in status
    assert "pillar3" in status
    assert "icc" in status
    for subdir, info in status.items():
        assert info["count"] == 0
        assert info["latest"] is None
        assert info["latest_age_days"] is None


def test_cache_status_reports_populated_subdir(manager, tmp_path: Path) -> None:
    apra_dir = tmp_path / "apra"
    apra_dir.mkdir()
    (apra_dir / "ADI_Performance_Q3_2025.xlsx").write_bytes(b"a")
    (apra_dir / "ADI_Property_Exposures_Q3_2025.xlsx").write_bytes(b"b")

    status = manager.cache_status()
    apra = status["apra"]
    assert apra["count"] == 2
    assert apra["latest"] in {
        "ADI_Performance_Q3_2025.xlsx", "ADI_Property_Exposures_Q3_2025.xlsx",
    }
    assert apra["latest_age_days"] == 0
    # pillar3 still empty
    assert status["pillar3"]["count"] == 0


def test_clear_specific_source_only_removes_that_subdir(manager, tmp_path: Path) -> None:
    apra_dir = tmp_path / "apra"; apra_dir.mkdir()
    pillar3_dir = tmp_path / "pillar3"; pillar3_dir.mkdir()
    (apra_dir / "a.xlsx").write_bytes(b"a")
    (pillar3_dir / "cba.xlsx").write_bytes(b"b")

    removed = manager.clear_cache(source="apra")
    assert removed == 1
    assert list(apra_dir.iterdir()) == []
    assert len(list(pillar3_dir.iterdir())) == 1   # untouched


def test_clear_all_sources_removes_everything(manager, tmp_path: Path) -> None:
    for sub in ("apra", "pillar3", "icc"):
        d = tmp_path / sub
        d.mkdir()
        (d / "file.bin").write_bytes(b"x")

    removed = manager.clear_cache(source=None)
    assert removed == 3
    for sub in ("apra", "pillar3", "icc"):
        d = tmp_path / sub
        assert list(d.iterdir()) == []
