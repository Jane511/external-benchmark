"""Council of Financial Regulators (CFR) publication capture.

Newest-first listing scrape with manifest-driven dedupe — the CFR analogue
of :class:`ingestion.adapters.apra_insight_adapter.ApraInsightScraper`.

The scraper inherits the manifest schema, dedupe contract, audit-log writes,
and download mechanics from
:class:`ingestion.adapters.apra_insight_adapter._NewestFirstPublicationScraper`,
overriding only the listing parser to match cfr.gov.au markup.

CFR publications are PDF documents of varying length; this adapter captures
the file but does NOT parse it.
"""

from __future__ import annotations

from typing import Iterable
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from ingestion.adapters.apra_insight_adapter import (
    PublicationEntry,
    _NewestFirstPublicationScraper,
    _date_from_container,
)


class CfrPublicationsScraper(_NewestFirstPublicationScraper):
    """Capture publications listed on https://www.cfr.gov.au/publications/."""

    source_name = "cfr_publications"
    publisher = "Council of Financial Regulators"
    landing_url = "https://www.cfr.gov.au/publications/"
    listing_href_prefix = "/publications/"

    def _parse_listing(self, html: str) -> Iterable[PublicationEntry]:
        soup = BeautifulSoup(html, "html.parser")
        seen: set[str] = set()
        out: list[PublicationEntry] = []

        for anchor in soup.find_all("a"):
            href = (anchor.get("href") or "").strip()
            if not href:
                continue
            absolute = urljoin(self.landing_url, href)
            parsed = urlparse(absolute)
            if "cfr.gov.au" not in parsed.netloc and parsed.netloc:
                continue
            # Must be under /publications/ but not the landing page itself.
            if not parsed.path.startswith("/publications/"):
                continue
            if parsed.path.rstrip("/") in {"/publications", ""}:
                continue
            if absolute in seen:
                continue
            title = " ".join(anchor.get_text(" ", strip=True).split())
            if not title:
                continue
            published = _date_from_container(anchor)
            seen.add(absolute)
            out.append(PublicationEntry(
                title=title, url=absolute, published_date=published,
            ))
        return out
