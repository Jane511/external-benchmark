"""Tests for the APRA Insight publication-capture adapter.

Network is fully mocked — every test feeds the adapter a pre-baked landing
HTML and a fake ``requests.Session`` whose ``get()`` returns canned bytes
for the article URLs. The adapter contract under test is:

  * Listing parser yields ``(title, url, published_date)`` rows.
  * Manifest dedupe (URL is the key) skips entries already captured.
  * Each captured entry produces an ``audit_log`` row.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import pytest
from sqlalchemy import select

from ingestion.adapters.apra_insight_adapter import (
    ApraInsightScraper,
    PublicationEntry,
)
from src.db import AuditLog, create_engine_and_schema, make_session_factory


_LANDING_HTML = """
<html><body>
  <ul class="insight-list">
    <li>
      <a href="/news-and-publications/insight-issue-three-2026">
        Insight Issue Three 2026
      </a>
      <time datetime="2026-04-15">15 April 2026</time>
    </li>
    <li>
      <a href="/news-and-publications/insight-issue-two-2026">Insight Issue Two 2026</a>
      <span class="date">12 March 2026</span>
    </li>
    <li>
      <a href="/news-and-publications/insight-issue-one-2026">Insight Issue One 2026</a>
      <span class="date">5 February 2026</span>
    </li>
    <li>
      <a href="/news-and-publications/apra-insight">Back to APRA Insight</a>
    </li>
  </ul>
</body></html>
"""


class _FakeResponse:
    def __init__(self, content: bytes, status: int = 200) -> None:
        self.content = content
        self.text = content.decode("utf-8", errors="replace")
        self.status_code = status

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeSession:
    """Minimal ``requests.Session`` stand-in. Records every URL fetched."""

    def __init__(self, responses: dict[str, bytes]) -> None:
        self.responses = responses
        self.calls: list[str] = []
        self.headers: dict[str, str] = {}

    def get(self, url: str, timeout: int = 30) -> _FakeResponse:
        self.calls.append(url)
        if url not in self.responses:
            return _FakeResponse(b"", status=404)
        return _FakeResponse(self.responses[url])


# ---------------------------------------------------------------------------
# Listing parser
# ---------------------------------------------------------------------------

def test_parse_listing_yields_three_dated_entries() -> None:
    scraper = ApraInsightScraper()
    rows: list[PublicationEntry] = list(scraper._parse_listing(_LANDING_HTML))
    titles = [r.title for r in rows]
    assert "Insight Issue Three 2026" in titles[0]
    # Exactly three real items — the "Back to APRA Insight" anchor is filtered out.
    assert len(rows) == 3
    assert all(r.published_date.startswith("2026-") for r in rows)


def test_parse_listing_skips_landing_self_link() -> None:
    scraper = ApraInsightScraper()
    rows = list(scraper._parse_listing(_LANDING_HTML))
    urls = {r.url for r in rows}
    assert scraper.landing_url not in urls
    assert all("/apra-insight" not in r.url.rstrip("/").rsplit("/", 1)[-1] for r in rows)


# ---------------------------------------------------------------------------
# Manifest path resolution
# ---------------------------------------------------------------------------

def test_manifest_path_under_cache_base(tmp_path: Path) -> None:
    scraper = ApraInsightScraper(cache_base=tmp_path / "raw")
    assert scraper.manifest_path == tmp_path / "raw" / "apra" / "insight" / "_manifest.json"


# ---------------------------------------------------------------------------
# End-to-end run via fake session
# ---------------------------------------------------------------------------

def _wire_responses(tmp_root: Path) -> tuple[ApraInsightScraper, _FakeSession]:
    article_bytes = b"%PDF stub for fixture"
    base = "https://www.apra.gov.au/news-and-publications/"
    responses = {
        f"{base}insight-issue-three-2026": article_bytes,
        f"{base}insight-issue-two-2026":   article_bytes,
        f"{base}insight-issue-one-2026":   article_bytes,
    }
    session = _FakeSession(responses)
    db_path = tmp_root / "audit.db"
    engine = create_engine_and_schema(db_path)
    scraper = ApraInsightScraper(
        cache_base=tmp_root / "raw",
        session=session,
        audit_engine=engine,
        actor="test",
    )
    return scraper, session


def test_run_captures_three_entries_and_writes_manifest(tmp_path: Path) -> None:
    scraper, _session = _wire_responses(tmp_path)
    captured = scraper.run(landing_html=_LANDING_HTML)
    assert len(captured) == 3

    manifest = json.loads(scraper.manifest_path.read_text(encoding="utf-8"))
    assert manifest["source_key"] == "apra_insight"
    assert len(manifest["items"]) == 3
    # Newest first (April 2026 before March, before February).
    dates = [item["published_date"] for item in manifest["items"]]
    assert dates == sorted(dates, reverse=True)
    for item in manifest["items"]:
        assert item["sha256"]
        assert item["local_path"].endswith(".pdf") or item["local_path"].endswith(".html")
        # Path uses forward slashes regardless of OS.
        assert "\\" not in item["local_path"]


def test_rerun_is_idempotent_when_manifest_already_holds_urls(tmp_path: Path) -> None:
    scraper, session = _wire_responses(tmp_path)
    first = scraper.run(landing_html=_LANDING_HTML)
    assert len(first) == 3

    # Drop the call log — what we care about is that pass 2 captures nothing.
    session.calls.clear()

    second = scraper.run(landing_html=_LANDING_HTML)
    assert second == []


def test_run_writes_one_audit_row_per_capture(tmp_path: Path) -> None:
    scraper, _session = _wire_responses(tmp_path)
    captured = scraper.run(landing_html=_LANDING_HTML)
    assert captured

    factory = make_session_factory(scraper.audit_engine)
    with factory() as session:
        rows = list(session.scalars(
            select(AuditLog).where(AuditLog.entity_id == "apra_insight"),
        ).all())
    assert len(rows) == len(captured)
    payload = json.loads(rows[0].params_json)
    assert payload["url"].startswith("https://www.apra.gov.au/")
    assert payload["title"]
    assert payload["file_hash"]
