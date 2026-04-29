"""End-to-end test for the APRA Insight + CFR governance pipeline.

Covers the four acceptance points from the build prompt:

1. Run the ``GovernancePublicationsDownloader`` against fixture landing pages
   listing 5 APRA Insight items and 4 CFR items; assert each manifest holds
   the expected count.
2. Re-run with the same fixtures; assert idempotency (no manifest growth,
   no new audit-log rows).
3. Generate the Board report and assert the "Recent regulator commentary"
   subsection lists the 3 most recent of each.
4. With empty manifests (no scrape yet), the report's commentary subsection
   gracefully emits the "No recent regulator commentary captured." line
   instead of crashing.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlalchemy import select

import ingestion.adapters.apra_insight_adapter as apra_module
from scripts.download_sources.governance_publications_downloader import (
    GovernancePublicationsDownloader,
)
from src.benchmark_report import BenchmarkCalibrationReport
from src.db import AuditLog, create_engine_and_schema, make_session_factory
from src.registry import BenchmarkRegistry


# ---------------------------------------------------------------------------
# Fixtures: 5 APRA Insight + 4 CFR items
# ---------------------------------------------------------------------------

APRA_INSIGHT_LANDING = """
<html><body>
  <ul>
    <li>
      <a href="/news-and-publications/insight-issue-five-2026">Insight Issue Five 2026</a>
      <time datetime="2026-04-20">20 April 2026</time>
    </li>
    <li>
      <a href="/news-and-publications/insight-issue-four-2026">Insight Issue Four 2026</a>
      <time datetime="2026-03-15">15 March 2026</time>
    </li>
    <li>
      <a href="/news-and-publications/insight-issue-three-2026">Insight Issue Three 2026</a>
      <time datetime="2026-02-10">10 February 2026</time>
    </li>
    <li>
      <a href="/news-and-publications/insight-issue-two-2025">Insight Issue Two 2025</a>
      <time datetime="2025-11-05">5 November 2025</time>
    </li>
    <li>
      <a href="/news-and-publications/insight-issue-one-2025">Insight Issue One 2025</a>
      <time datetime="2025-08-01">1 August 2025</time>
    </li>
  </ul>
</body></html>
"""

CFR_PUBLICATIONS_LANDING = """
<html><body>
  <article>
    <a href="/publications/cfr-statement-march-2026.html">CFR Statement March 2026</a>
    <time datetime="2026-03-25">25 March 2026</time>
  </article>
  <article>
    <a href="/publications/payments-policy-feb-2026.html">Payments Policy February 2026</a>
    <time datetime="2026-02-12">12 February 2026</time>
  </article>
  <article>
    <a href="/publications/housing-credit-q4-2025.html">Housing Credit Q4 2025</a>
    <time datetime="2025-12-18">18 December 2025</time>
  </article>
  <article>
    <a href="/publications/financial-stability-q3-2025.html">Financial Stability Q3 2025</a>
    <time datetime="2025-09-30">30 September 2025</time>
  </article>
</body></html>
"""


# ---------------------------------------------------------------------------
# Fake HTTP layer
# ---------------------------------------------------------------------------


class _FakeResponse:
    def __init__(self, payload: bytes, status: int = 200) -> None:
        self.content = payload
        self.text = payload.decode("utf-8", errors="replace")
        self.status_code = status

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeSession:
    """Routes requests by URL; everything that isn't a landing page returns a tiny PDF stub."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.headers: dict[str, str] = {}

    def get(self, url: str, timeout: int = 30) -> _FakeResponse:
        self.calls.append(url)
        if url == "https://www.apra.gov.au/news-and-publications/apra-insight":
            return _FakeResponse(APRA_INSIGHT_LANDING.encode("utf-8"))
        if url == "https://www.cfr.gov.au/publications/":
            return _FakeResponse(CFR_PUBLICATIONS_LANDING.encode("utf-8"))
        return _FakeResponse(b"%PDF stub-content")


@pytest.fixture()
def patched_requests(monkeypatch: pytest.MonkeyPatch) -> _FakeSession:
    """Replace ``requests.Session`` in the adapter module with a single shared fake."""
    fake = _FakeSession()
    monkeypatch.setattr(apra_module.requests, "Session", lambda: fake)
    return fake


# ---------------------------------------------------------------------------
# 1 + 2: download and idempotency
# ---------------------------------------------------------------------------


def test_downloader_captures_5_apra_and_4_cfr_then_is_idempotent(
    tmp_path: Path, patched_requests: _FakeSession,
) -> None:
    db = tmp_path / "audit.db"
    create_engine_and_schema(db)  # eager-create so both runs share the file
    cache = tmp_path / "raw"

    downloader = GovernancePublicationsDownloader(cache_base=cache, db_path=db)
    first = downloader.download_all()
    assert len(first["apra_insight"]) == 5
    assert len(first["cfr_publications"]) == 4

    apra_manifest = json.loads(
        (cache / "apra" / "insight" / "_manifest.json").read_text(encoding="utf-8"),
    )
    cfr_manifest = json.loads(
        (cache / "cfr" / "_manifest.json").read_text(encoding="utf-8"),
    )
    assert len(apra_manifest["items"]) == 5
    assert len(cfr_manifest["items"]) == 4

    # Newest-first ordering is preserved in the manifest.
    apra_dates = [r["published_date"] for r in apra_manifest["items"]]
    assert apra_dates == sorted(apra_dates, reverse=True)
    cfr_dates = [r["published_date"] for r in cfr_manifest["items"]]
    assert cfr_dates == sorted(cfr_dates, reverse=True)

    # Audit log: one row per capture (5 + 4 = 9).
    engine = create_engine_and_schema(db)
    factory = make_session_factory(engine)
    with factory() as session:
        rows = list(session.scalars(
            select(AuditLog).where(AuditLog.operation == "read_source_document"),
        ).all())
    assert len(rows) == 9

    # Idempotency: re-run with the same fixtures captures nothing new.
    second = downloader.download_all()
    assert second["apra_insight"] == []
    assert second["cfr_publications"] == []

    apra_after = json.loads(
        (cache / "apra" / "insight" / "_manifest.json").read_text(encoding="utf-8"),
    )
    cfr_after = json.loads(
        (cache / "cfr" / "_manifest.json").read_text(encoding="utf-8"),
    )
    assert len(apra_after["items"]) == 5
    assert len(cfr_after["items"]) == 4

    with factory() as session:
        rows_after = list(session.scalars(
            select(AuditLog).where(AuditLog.operation == "read_source_document"),
        ).all())
    assert len(rows_after) == 9


# ---------------------------------------------------------------------------
# 3: report subsection lists 3 most recent of each
# ---------------------------------------------------------------------------


def test_benchmark_report_lists_three_most_recent_of_each(
    tmp_path: Path, patched_requests: _FakeSession,
) -> None:
    db = tmp_path / "audit.db"
    cache = tmp_path / "raw"
    GovernancePublicationsDownloader(
        cache_base=cache, db_path=db,
    ).download_all()

    registry = BenchmarkRegistry(create_engine_and_schema(db), actor="test")
    report = BenchmarkCalibrationReport(
        registry=registry, period_label="Q1 2026", raw_data_dir=cache,
    )
    data = report.generate()
    commentary = data["supporting_documentation"]["recent_regulator_commentary"]

    apra = commentary["apra_insight"]
    cfr = commentary["cfr_publications"]
    assert len(apra) == 3
    assert len(cfr) == 3
    assert [r["published_date"] for r in apra] == ["2026-04-20", "2026-03-15", "2026-02-10"]
    assert [r["published_date"] for r in cfr] == ["2026-03-25", "2026-02-12", "2025-12-18"]
    assert commentary["empty_message"] == ""

    md = report.to_markdown()
    assert "### Recent regulator commentary" in md
    assert "Insight Issue Five 2026" in md
    assert "CFR Statement March 2026" in md
    # The 4th-newest CFR item must NOT be in the top-3 list.
    assert "Financial Stability Q3 2025" not in md


# ---------------------------------------------------------------------------
# 4: empty manifests fall back to the friendly message
# ---------------------------------------------------------------------------


def test_report_handles_empty_manifests(tmp_path: Path) -> None:
    cache = tmp_path / "raw"  # no governance scrape ran -> manifests don't exist
    db = tmp_path / "audit.db"
    registry = BenchmarkRegistry(create_engine_and_schema(db), actor="test")

    report = BenchmarkCalibrationReport(
        registry=registry, period_label="Q1 2026", raw_data_dir=cache,
    )
    data = report.generate()
    commentary = data["supporting_documentation"]["recent_regulator_commentary"]
    assert commentary["apra_insight"] == []
    assert commentary["cfr_publications"] == []
    assert commentary["empty_message"] == "No recent regulator commentary captured."

    md = report.to_markdown()
    assert "### Recent regulator commentary" in md
    assert "No recent regulator commentary captured." in md
