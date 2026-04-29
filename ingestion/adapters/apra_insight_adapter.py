"""APRA Insight publication capture (newest-first listing scrape).

APRA Insight is released irregularly. The scraper:

1. Fetches the landing page.
2. Parses every publication entry with its title, page URL, and published date.
3. Compares URLs against a per-source ``_manifest.json`` in the cache dir.
4. Downloads only entries the manifest doesn't already record.
5. Appends a row to the manifest and writes one ``audit_log`` row per capture.

The captured files are PDFs (or HTML article pages — APRA mixes both). This
adapter does NOT parse them; structured signals are out of scope.

This module also exposes ``_NewestFirstPublicationScraper``, a small base class
shared with :mod:`ingestion.adapters.cfr_publications_adapter`. They share the
manifest schema, the dedupe-by-url contract, and the audit/log plumbing — only
the listing-page selectors differ between sites.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from sqlalchemy.engine import Engine

from ingestion.source_registry import SOURCE_URLS
from src.db import AuditLog, create_engine_and_schema, make_session_factory


logger = logging.getLogger(__name__)


_MONTH_NUM = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}


@dataclass(frozen=True)
class PublicationEntry:
    """One row parsed from a listing page."""
    title: str
    url: str               # absolute URL (may be PDF or HTML article page)
    published_date: str    # ISO date "YYYY-MM-DD"; "" if unparseable


@dataclass(frozen=True)
class CapturedPublication:
    """Manifest row + audit-log payload for one captured publication."""
    source_key: str
    title: str
    url: str
    published_date: str
    fetched_at: str        # ISO datetime UTC
    local_path: str        # repo-relative posix path
    sha256: str


class _NewestFirstPublicationScraper:
    """Base for landing-page scrapers with a per-source manifest.

    Subclasses set ``source_name``, ``landing_url``, ``publisher`` and override
    ``_parse_listing()`` to match their site's HTML shape. Everything else —
    manifest IO, dedupe, hashing, audit-log writes — is inherited.
    """

    source_name: str = ""
    publisher: str = ""
    landing_url: str = ""
    listing_href_prefix: str = ""   # e.g. "/news-and-publications/" — used by default parser

    def __init__(
        self,
        *,
        cache_base: Path | str = "data/raw",
        session: requests.Session | None = None,
        audit_db_path: Path | str | None = None,
        audit_engine: Engine | None = None,
        actor: str = "governance_publications_downloader",
    ) -> None:
        self.cache_base = Path(cache_base)
        self.session = session or requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "external-benchmark-engine/0.2"
            ),
        })
        self.audit_db_path = Path(audit_db_path) if audit_db_path else None
        self.audit_engine = audit_engine
        self.actor = actor

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, *, landing_html: str | None = None) -> list[CapturedPublication]:
        """Fetch the landing page, capture every NEW entry, return what was captured."""
        html = landing_html if landing_html is not None else self._get_text(self.landing_url)
        entries = list(self._parse_listing(html))
        manifest = self._load_manifest()
        seen_urls = {item.get("url", "") for item in manifest.get("items", [])}

        captured: list[CapturedPublication] = []
        for entry in entries:
            if not entry.url or entry.url in seen_urls:
                continue
            try:
                local = self._download(entry)
            except _DownloadError as exc:
                logger.warning(
                    "%s: skipping entry %r — %s", self.source_name, entry.url, exc,
                )
                continue
            row = CapturedPublication(
                source_key=self.source_name,
                title=entry.title,
                url=entry.url,
                published_date=entry.published_date,
                fetched_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                local_path=_rel(local),
                sha256=_sha256(local),
            )
            captured.append(row)
            seen_urls.add(entry.url)
            self._append_manifest(row)
            self._write_audit(row)
        return captured

    # ------------------------------------------------------------------
    # Listing parser — subclasses may override
    # ------------------------------------------------------------------

    def _parse_listing(self, html: str) -> Iterable[PublicationEntry]:
        """Default listing parser.

        Walks every anchor whose href starts with ``listing_href_prefix``. For
        each anchor it extracts the title (the anchor text), the URL (resolved
        against the landing page), and the published date (the first plausible
        date string found in the anchor's nearest container — ``<article>`` /
        ``<li>`` / ``<div>``).

        Subclasses override this when the site uses a different shape.
        """
        if not self.listing_href_prefix:
            return []
        soup = BeautifulSoup(html, "html.parser")
        seen: set[str] = set()
        out: list[PublicationEntry] = []
        for anchor in soup.find_all("a"):
            href = (anchor.get("href") or "").strip()
            if not href.startswith(self.listing_href_prefix):
                continue
            url = urljoin(self.landing_url, href)
            if url in seen or url.rstrip("/") == self.landing_url.rstrip("/"):
                continue
            title = " ".join(anchor.get_text(" ", strip=True).split())
            if not title:
                continue
            published = _date_from_container(anchor)
            seen.add(url)
            out.append(PublicationEntry(title=title, url=url, published_date=published))
        return out

    # ------------------------------------------------------------------
    # Manifest IO
    # ------------------------------------------------------------------

    @property
    def cache_dir(self) -> Path:
        """Resolve the per-source subdirectory under ``cache_base``.

        The registry's ``cache_dir`` is repo-relative (e.g. ``data/raw/apra/insight/``).
        Conventionally callers pass ``cache_base="data/raw"`` so the registry path
        needs the ``data/raw/`` prefix stripped to avoid double-nesting. We do that
        whenever the prefix is present; if the registry path is already a bare
        ``apra/insight``-style relative, we fall through unchanged.
        """
        spec = SOURCE_URLS[self.source_name]
        relative = spec["cache_dir"].rstrip("/")
        if relative.startswith("data/raw/"):
            relative = relative[len("data/raw/"):]
        return self.cache_base / relative

    @property
    def manifest_path(self) -> Path:
        return self.cache_dir / "_manifest.json"

    def _load_manifest(self) -> dict:
        path = self.manifest_path
        if not path.exists():
            return {"source_key": self.source_name, "items": []}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise _DownloadError(f"corrupt manifest at {path}: {exc}") from exc
        if "items" not in payload:
            payload["items"] = []
        payload.setdefault("source_key", self.source_name)
        return payload

    def _append_manifest(self, row: CapturedPublication) -> None:
        manifest = self._load_manifest()
        manifest["items"].append(asdict(row))
        manifest["items"].sort(
            key=lambda item: (item.get("published_date", ""), item.get("title", "")),
            reverse=True,
        )
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        self.manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=False), encoding="utf-8",
        )

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    def _download(self, entry: PublicationEntry) -> Path:
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        filename = self._filename_for(entry)
        dest = self.cache_dir / filename
        if dest.exists():
            return dest
        try:
            response = self.session.get(entry.url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise _DownloadError(f"failed to fetch {entry.url}: {exc}") from exc
        dest.write_bytes(response.content)
        return dest

    def _filename_for(self, entry: PublicationEntry) -> str:
        slug = _slug_from_url(entry.url) or _slug_from_title(entry.title) or "item"
        suffix = _suffix_from_url(entry.url)
        spec = SOURCE_URLS[self.source_name]
        pattern = spec["files"][0]["filename_pattern"]
        # filename_pattern uses .pdf; honour actual content if not a PDF URL.
        rendered = pattern.format(slug=slug)
        if suffix and not rendered.lower().endswith(suffix.lower()):
            rendered = Path(rendered).stem + suffix
        return rendered

    # ------------------------------------------------------------------
    # Audit + HTTP helpers
    # ------------------------------------------------------------------

    def _write_audit(self, row: CapturedPublication) -> None:
        engine = self.audit_engine
        if engine is None and self.audit_db_path is not None:
            engine = create_engine_and_schema(self.audit_db_path)
        if engine is None:
            return
        factory = make_session_factory(engine)
        with factory() as session:
            session.add(AuditLog(
                operation="read_source_document",
                entity_id=row.source_key,
                params_json=json.dumps({
                    "url": row.url,
                    "title": row.title,
                    "published_date": row.published_date,
                    "retrieved_at": row.fetched_at,
                    "file_hash": row.sha256,
                    "local_cached_file": row.local_path,
                }),
                result_summary="captured",
                actor=self.actor,
            ))
            session.commit()

    def _get_text(self, url: str) -> str:
        response = self.session.get(url, timeout=30)
        try:
            response.raise_for_status()
        except requests.RequestException as exc:
            raise _DownloadError(f"failed to fetch {url}: {exc}") from exc
        return response.text


# ---------------------------------------------------------------------------
# Public scraper
# ---------------------------------------------------------------------------


class ApraInsightScraper(_NewestFirstPublicationScraper):
    """Capture APRA Insight items as a manifest of new publications."""

    source_name = "apra_insight"
    publisher = "Australian Prudential Regulation Authority"
    landing_url = "https://www.apra.gov.au/news-and-publications/apra-insight"
    listing_href_prefix = "/news-and-publications/"

    def _parse_listing(self, html: str) -> Iterable[PublicationEntry]:
        soup = BeautifulSoup(html, "html.parser")
        seen: set[str] = set()
        out: list[PublicationEntry] = []
        for anchor in soup.find_all("a"):
            href = (anchor.get("href") or "").strip()
            if not href.startswith(self.listing_href_prefix):
                continue
            absolute = urljoin(self.landing_url, href)
            if absolute.rstrip("/") == self.landing_url.rstrip("/"):
                continue
            # Skip the Insight landing page itself and obvious non-issue links.
            tail = urlparse(absolute).path.rstrip("/").rsplit("/", 1)[-1]
            if not tail or tail in {"apra-insight", "news-and-publications"}:
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


# ---------------------------------------------------------------------------
# Helpers (module-private)
# ---------------------------------------------------------------------------


class _DownloadError(Exception):
    """Internal error raised when a single entry can't be captured."""


def _date_from_container(anchor) -> str:
    """Walk up to two ancestor levels and pull the first ISO/long-form date string."""
    container = anchor
    for _ in range(3):
        if container is None:
            break
        text = container.get_text(" ", strip=True) if hasattr(container, "get_text") else ""
        iso = _try_iso_date(text)
        if iso:
            return iso
        container = getattr(container, "parent", None)
    # <time datetime="..."> sibling
    parent = anchor.parent
    if parent is not None:
        for time_tag in parent.find_all("time"):
            iso = _try_iso_date(time_tag.get("datetime", ""))
            if iso:
                return iso
            iso = _try_iso_date(time_tag.get_text(" ", strip=True))
            if iso:
                return iso
    return ""


_LONG_DATE_RE = re.compile(
    r"\b(?P<day>\d{1,2})\s+(?P<month>"
    r"jan|january|feb|february|mar|march|apr|april|may|jun|june|"
    r"jul|july|aug|august|sep|sept|september|oct|october|nov|november|dec|december"
    r")\s+(?P<year>20\d{2})\b",
    re.IGNORECASE,
)
_ISO_DATE_RE = re.compile(r"\b(20\d{2})-(\d{2})-(\d{2})\b")
_SLASH_DATE_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(20\d{2})\b")


def _try_iso_date(text: str) -> str:
    if not text:
        return ""
    match = _ISO_DATE_RE.search(text)
    if match:
        try:
            return date(int(match.group(1)), int(match.group(2)), int(match.group(3))).isoformat()
        except ValueError:
            pass
    match = _LONG_DATE_RE.search(text)
    if match:
        month = _MONTH_NUM.get(match.group("month").lower())
        if month is not None:
            try:
                return date(int(match.group("year")), month, int(match.group("day"))).isoformat()
            except ValueError:
                pass
    match = _SLASH_DATE_RE.search(text)
    if match:
        try:
            return date(int(match.group(3)), int(match.group(2)), int(match.group(1))).isoformat()
        except ValueError:
            pass
    return ""


def _slug_from_url(url: str) -> str:
    if not url:
        return ""
    path = urlparse(url).path.rstrip("/")
    if not path:
        return ""
    tail = path.rsplit("/", 1)[-1]
    stem = tail.rsplit(".", 1)[0] if "." in tail else tail
    return _safe_slug(stem)


def _slug_from_title(title: str) -> str:
    return _safe_slug(title)


def _safe_slug(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9]+", "-", value).strip("-")
    return cleaned[:80].lower()


def _suffix_from_url(url: str) -> str:
    path = urlparse(url).path.lower()
    for ext in (".pdf", ".html", ".htm"):
        if path.endswith(ext):
            return ext
    return ""


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _rel(path: Path) -> str:
    try:
        return str(path.relative_to(Path.cwd())).replace("\\", "/")
    except ValueError:
        return str(path).replace("\\", "/")
