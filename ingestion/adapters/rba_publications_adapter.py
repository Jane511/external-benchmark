"""RBA publication download scrapers.

These classes capture narrative RBA PDFs for governance and
forward-looking-information overlays. They intentionally do not emit
BenchmarkEntry or RawObservation rows; structured parsing of the documents is
out of scope for this layer.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from sqlalchemy.engine import Engine

from ingestion.downloader import DownloadError, FileDownloader
from ingestion.source_registry import SOURCE_URLS
from src.db import AuditLog, create_engine_and_schema, make_session_factory


_RBA_ROOT = "https://www.rba.gov.au"
_MONTH_ORDER = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}
_MONTH_NAME = {
    "jan": "January", "feb": "February", "mar": "March", "apr": "April",
    "may": "May", "jun": "June", "jul": "July", "aug": "August",
    "sep": "September", "oct": "October", "nov": "November", "dec": "December",
}


@dataclass(frozen=True)
class RbaPublicationFetch:
    source_key: str
    source: str
    publisher: str
    url: str
    resolved_pdf_url: str
    local_cached_file: str
    period: str
    retrieval_date: str
    file_hash: str


class _RbaPublicationScraper:
    source_name: str = ""
    source_title: str = ""
    landing_url: str = ""
    issue_re: re.Pattern[str] | None = None
    filename_prefix: str = ""

    def __init__(
        self,
        *,
        cache_base: Path | str = "data/raw",
        session: requests.Session | None = None,
        audit_db_path: Path | str | None = None,
        audit_engine: Engine | None = None,
        actor: str = "rba_downloader",
    ) -> None:
        self.cache_base = Path(cache_base)
        self.session = session or requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "external-benchmark-engine/0.2"
            )
        })
        self.audit_db_path = Path(audit_db_path) if audit_db_path else None
        self.audit_engine = audit_engine
        self.actor = actor

    def run(self, *, force_refresh: bool = False) -> RbaPublicationFetch:
        pdf_url, period = self._discover_latest_url()
        path = self._fetch(pdf_url, period, force_refresh=force_refresh)
        result = self._write_metadata(path, pdf_url, period)
        self._write_audit(result)
        self._parse(path)
        return result

    def _discover_latest_url(
        self,
        landing_html: str | None = None,
    ) -> tuple[str, str]:
        html = landing_html if landing_html is not None else self._get_text(self.landing_url)
        direct = self._find_download_pdf(html, self.landing_url)
        if direct is not None:
            return direct

        issue_url, period = self._latest_issue_url(html)
        issue_html = self._get_text(issue_url)
        pdf = self._find_download_pdf(issue_html, issue_url)
        if pdf is None:
            raise DownloadError(
                f"{self.source_name}: latest issue {issue_url} has no Download PDF link"
            )
        return pdf[0], period

    def _fetch(
        self,
        pdf_url: str,
        period: str,
        *,
        force_refresh: bool = False,
    ) -> Path:
        filename = self._filename(period)
        downloader = FileDownloader(self.cache_base, "rba")
        path = downloader.download_and_cache(pdf_url, filename, force_refresh=force_refresh)
        if not path.exists() or path.read_bytes()[:4] != b"%PDF":
            raise DownloadError(f"{self.source_name}: downloaded file is not a PDF: {path}")
        return path

    def _parse(self, path: Path) -> None:
        return None

    def _latest_issue_url(self, html: str) -> tuple[str, str]:
        if self.issue_re is None:
            raise DownloadError(f"{self.source_name}: no issue URL parser configured")
        soup = BeautifulSoup(html, "html.parser")
        candidates: set[tuple[int, int, str, str]] = set()
        for anchor in soup.select("a"):
            href = (anchor.get("href") or "").strip()
            match = self.issue_re.match(href)
            if not match:
                continue
            year = int(match.group("year"))
            month_key = match.group("month").lower()
            month_num = _MONTH_ORDER.get(month_key, 0)
            if not month_num:
                continue
            period = f"{_MONTH_NAME[month_key]} {year}"
            candidates.add((year, month_num, href, period))
        if not candidates:
            raise DownloadError(
                f"{self.source_name}: no issue links found on {self.landing_url}"
            )
        latest = sorted(candidates, reverse=True)[0]
        return urljoin(_RBA_ROOT, latest[2]), latest[3]

    @staticmethod
    def _find_download_pdf(html: str, base_url: str) -> tuple[str, str] | None:
        soup = BeautifulSoup(html, "html.parser")
        for anchor in soup.select("a"):
            href = (anchor.get("href") or "").strip()
            text = " ".join(anchor.get_text(" ", strip=True).lower().split())
            if not href.lower().endswith(".pdf"):
                continue
            if "download pdf" in text or "chart pack" in text:
                return urljoin(base_url, href), _period_from_url_or_text(href, text)
        return None

    def _filename(self, period: str) -> str:
        year, quarter = _year_quarter_from_period(period)
        safe_period = period.replace(" ", "_")
        pattern = SOURCE_URLS[self.source_name]["files"][0]["filename_pattern"]
        return pattern.format(period=safe_period, quarter=quarter, year=year)

    def _write_metadata(
        self,
        path: Path,
        pdf_url: str,
        period: str,
    ) -> RbaPublicationFetch:
        retrieval_date = date.today().isoformat()
        file_hash = _sha256(path)
        rel_path = _rel(path)
        result = RbaPublicationFetch(
            source_key=self.source_name,
            source=self.source_title,
            publisher="Reserve Bank of Australia",
            url=self.landing_url,
            resolved_pdf_url=pdf_url,
            local_cached_file=rel_path,
            period=period,
            retrieval_date=retrieval_date,
            file_hash=file_hash,
        )
        meta_path = path.with_suffix(path.suffix + ".metadata.json")
        meta_path.write_text(json.dumps(result.__dict__, indent=2), encoding="utf-8")
        return result

    def _write_audit(self, result: RbaPublicationFetch) -> None:
        engine = self.audit_engine
        if engine is None and self.audit_db_path is not None:
            engine = create_engine_and_schema(self.audit_db_path)
        if engine is None:
            return
        factory = make_session_factory(engine)
        with factory() as session:
            session.add(AuditLog(
                operation="read_source_document",
                entity_id=result.source_key,
                params_json=json.dumps({
                    "url": result.resolved_pdf_url,
                    "retrieved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    "file_hash": result.file_hash,
                    "local_cached_file": result.local_cached_file,
                    "period": result.period,
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
            raise DownloadError(f"{self.source_name}: failed to fetch {url}: {exc}") from exc
        return response.text


class RbaFsrScraper(_RbaPublicationScraper):
    source_name = "rba_fsr"
    source_title = "RBA Financial Stability Review"
    landing_url = "https://www.rba.gov.au/publications/fsr/"
    issue_re = re.compile(r"^/publications/fsr/(?P<year>\d{4})/(?P<month>[a-z]{3})/?$")


class RbaSmpScraper(_RbaPublicationScraper):
    source_name = "rba_smp"
    source_title = "RBA Statement on Monetary Policy"
    landing_url = "https://www.rba.gov.au/publications/smp/"
    issue_re = re.compile(r"^/publications/smp/(?P<year>\d{4})/(?P<month>[a-z]{3})/?$")


class RbaChartPackScraper(_RbaPublicationScraper):
    source_name = "rba_chart_pack"
    source_title = "RBA Chart Pack"
    landing_url = "https://www.rba.gov.au/chart-pack/"

    def _discover_latest_url(
        self,
        landing_html: str | None = None,
    ) -> tuple[str, str]:
        html = landing_html if landing_html is not None else self._get_text(self.landing_url)
        found = self._find_download_pdf(html, self.landing_url)
        if found is None:
            raise DownloadError(
                f"{self.source_name}: no Chart Pack PDF link found on {self.landing_url}"
            )
        pdf_url, _period = found
        return pdf_url, _chart_pack_period()


def _period_from_url_or_text(href: str, text: str) -> str:
    haystack = f"{href} {text}".lower()
    match = re.search(r"(20\d{2})[-/](jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)", haystack)
    if match:
        return f"{_MONTH_NAME[match.group(2)]} {match.group(1)}"
    match = re.search(r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[-/]?(20\d{2})", haystack)
    if match:
        return f"{_MONTH_NAME[match.group(1)]} {match.group(2)}"
    return _chart_pack_period()


def _chart_pack_period(today: date | None = None) -> str:
    d = today or date.today()
    return f"Q{((d.month - 1) // 3) + 1} {d.year}"


def _year_quarter_from_period(period: str) -> tuple[str, str]:
    q_match = re.match(r"Q([1-4])\s+(20\d{2})$", period)
    if q_match:
        return q_match.group(2), f"Q{q_match.group(1)}"
    parts = period.split()
    if len(parts) == 2 and parts[1].isdigit():
        month = next((k for k, name in _MONTH_NAME.items() if name == parts[0]), "jan")
        q = ((_MONTH_ORDER[month] - 1) // 3) + 1
        return parts[1], f"Q{q}"
    year = str(date.today().year)
    return year, f"Q{((date.today().month - 1) // 3) + 1}"


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
