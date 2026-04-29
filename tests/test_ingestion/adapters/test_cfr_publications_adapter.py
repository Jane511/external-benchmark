"""Tests for the CFR publications capture adapter (newest-first listing)."""

from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import select

from ingestion.adapters.cfr_publications_adapter import CfrPublicationsScraper
from src.db import AuditLog, create_engine_and_schema, make_session_factory


_LANDING_HTML = """
<html><body>
  <article>
    <a href="https://www.cfr.gov.au/publications/cfr-statement-on-housing-2026.html">
      CFR Statement on Housing — March 2026
    </a>
    <time datetime="2026-03-20">20 March 2026</time>
  </article>
  <article>
    <a href="/publications/policy-statements/2026/payments-policy.html">
      Payments Policy Update
    </a>
    <span>10 February 2026</span>
  </article>
  <article>
    <a href="/publications/quarterly-statement-2025-q4.html">
      Quarterly Statement 2025 Q4
    </a>
    <span>2025-12-15</span>
  </article>
  <a href="/publications/">All publications</a>
  <a href="https://example.com/external">External link — not a CFR publication</a>
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
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.headers: dict[str, str] = {}

    def get(self, url: str, timeout: int = 30) -> _FakeResponse:
        self.calls.append(url)
        return _FakeResponse(b"%PDF fixture")


def test_parse_listing_filters_external_and_index_links() -> None:
    scraper = CfrPublicationsScraper()
    rows = list(scraper._parse_listing(_LANDING_HTML))
    urls = [r.url for r in rows]
    assert len(rows) == 3
    assert "https://example.com/external" not in urls
    assert scraper.landing_url not in urls


def test_parse_listing_extracts_iso_and_long_dates() -> None:
    scraper = CfrPublicationsScraper()
    rows = list(scraper._parse_listing(_LANDING_HTML))
    by_url = {r.url: r.published_date for r in rows}
    march = "https://www.cfr.gov.au/publications/cfr-statement-on-housing-2026.html"
    feb = "https://www.cfr.gov.au/publications/policy-statements/2026/payments-policy.html"
    dec = "https://www.cfr.gov.au/publications/quarterly-statement-2025-q4.html"
    assert by_url[march] == "2026-03-20"
    assert by_url[feb] == "2026-02-10"
    assert by_url[dec] == "2025-12-15"


def test_run_captures_and_dedupes(tmp_path: Path) -> None:
    session = _FakeSession()
    engine = create_engine_and_schema(tmp_path / "audit.db")
    scraper = CfrPublicationsScraper(
        cache_base=tmp_path / "raw",
        session=session,
        audit_engine=engine,
        actor="test",
    )

    first = scraper.run(landing_html=_LANDING_HTML)
    assert len(first) == 3

    manifest = json.loads(scraper.manifest_path.read_text(encoding="utf-8"))
    assert len(manifest["items"]) == 3
    # Newest first by published_date.
    assert manifest["items"][0]["published_date"] == "2026-03-20"

    # Re-run is a no-op.
    second = scraper.run(landing_html=_LANDING_HTML)
    assert second == []
    manifest2 = json.loads(scraper.manifest_path.read_text(encoding="utf-8"))
    assert len(manifest2["items"]) == 3

    # Audit log captures one row per first-run capture, none on re-run.
    factory = make_session_factory(engine)
    with factory() as s:
        rows = list(s.scalars(
            select(AuditLog).where(AuditLog.entity_id == "cfr_publications"),
        ).all())
    assert len(rows) == 3
