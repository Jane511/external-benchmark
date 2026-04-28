"""Download non-bank ASX-listed lender disclosures (graceful per-source).

Usage:
    python scripts/download_sources/non_bank_downloader.py
    python scripts/download_sources/non_bank_downloader.py --lender judo
    python scripts/download_sources/non_bank_downloader.py --force-refresh

Coverage
--------
Targets the 9 non-bank lenders identified in
``ingestion/segment_mapping.yaml``:

  judo, liberty, pepper, resimac, moneyme, plenti, wisr, qualitas,
  metrics_credit

Each has a different investor-relations layout. Some IR pages are
actively bot-protected (Cloudflare / Akamai / 403) or JS-rendered, in
which case the downloader records a ``MANUAL`` outcome and writes a
per-lender ``_MANUAL.md`` note pointing the analyst at the correct URL.
This is intentionally graceful — partial success is the realistic
outcome for non-bank lender IR scraping at scale.

Outcome levels
--------------
- **OK**     PDF downloaded into ``data/raw/non_bank/<lender>/``
- **MANUAL** site is reachable but blocks bots, or the page is
  JS-rendered; analyst must fetch by hand. A ``_MANUAL.md`` note is
  written with the IR URL and any sample disclosure file pattern that
  matches the corresponding adapter's expectations.
- **FAIL**   network error or unrecoverable HTTP error. Logged with
  the exception class.

The downloader does NOT fabricate data when a fetch fails. Adapters
already accept "no input" as a valid outcome (per
``ingestion/adapters/base.py``); empty raw_observations is preferable
to fake observations.

URL split (primary vs secondary)
--------------------------------
A small number of IR sites split disclosures across two paths (e.g.
quarterly results on one page, annual reports on another). Those
lenders carry an ``ir_url_secondary`` and the downloader retries the
secondary URL automatically when the primary returns no matching link.
"""

from __future__ import annotations

import argparse
import fnmatch
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from common import logger, safe_download, validate_pdf  # type: ignore
else:
    from .common import logger, safe_download, validate_pdf


_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0 Safari/537.36 external-benchmark-engine/0.2"
)


# Per-lender configuration. Each entry has:
#   ir_url:           investor relations / disclosures landing page
#   ir_url_secondary: optional fallback page (used when primary yields no match)
#   file_pattern:     fnmatch glob over href basename
#   fallback_keywords: substring matches over (text + href) when the
#                     primary glob misses
#   exclude_keywords: optional substring vetoes over (text + href)
#   fallback_ext:     extension to require during fallback search
#   manual_hint:      one-line guidance written into _MANUAL.md if blocked
_LENDERS: dict[str, dict] = {
    "judo": {
        "ir_url": "https://www.judo.bank/regulatory-disclosures",
        "file_pattern": "*pillar*3*.pdf",
        "fallback_keywords": [
            "pillar 3", "pillar-3", "basel iii", "fy25", "fy26", "h1", "apra",
        ],
        "fallback_ext": ".pdf",
        "manual_hint": (
            "Judo Bank Pillar 3 disclosures are on /regulatory-disclosures, "
            "not the marketing /about/investor-centre. Quarterly cadence."
        ),
    },
    "liberty": {
        "ir_url": "https://www.lfgroup.com.au/reports/asx-announcements",
        "ir_url_secondary": "https://www.lfgroup.com.au/",
        "file_pattern": "*annual*report*.pdf",
        "fallback_keywords": [
            "annual report", "annual review", "fy24", "fy25",
            "results", "1h", "2h",
        ],
        "fallback_ext": ".pdf",
        "manual_hint": (
            "Liberty Financial's IR is on lfgroup.com.au (was "
            "libertyfinancial.com.au; old domain DNS-fails). ASX:LFG. "
            "Annual + half-yearly cadence."
        ),
    },
    "pepper": {
        "ir_url": "https://www.peppermoney.com.au/about/debt-investors",
        "file_pattern": "*results*.pdf",
        "fallback_keywords": [
            "results presentation", "fy24", "fy25", "fy26",
            "1h", "2h", "annual report",
        ],
        "exclude_keywords": ["green bond", "bond framework", "framework"],
        "fallback_ext": ".pdf",
        "manual_hint": (
            "Pepper Money debt-investors page. ASX:PPM. Half-yearly + "
            "annual cadence. Reports include explicit asset-finance loan "
            "loss expense and arrears tables."
        ),
    },
    "resimac": {
        "ir_url": "https://www.resimac.com.au/en/Investors",
        "ir_url_secondary": "https://www.resimac.com.au/investors/annual-reports",
        "file_pattern": "*results*.pdf",
        "fallback_keywords": [
            "interim", "half-year", "annual report", "fy24", "fy25",
            "investor presentation",
        ],
        "fallback_ext": ".pdf",
        "manual_hint": (
            "Resimac IR landing is /en/Investors. Annual reports on "
            "/investors/annual-reports. ASX:RMC. Half-yearly + annual "
            "cadence."
        ),
    },
    "moneyme": {
        "ir_url": "https://investors.moneyme.com.au/investor-centre/?page=asx-announcements",
        "ir_url_secondary": "https://investors.moneyme.com.au/",
        "file_pattern": "*results*.pdf",
        "fallback_keywords": [
            "results", "annual report", "fy25", "fy26",
            "1h", "2h", "investor presentation",
        ],
        "fallback_ext": ".pdf",
        "manual_hint": (
            "MoneyMe IR moved to dedicated subdomain "
            "investors.moneyme.com.au. ASX:MME. Half-yearly cadence."
        ),
    },
    "plenti": {
        "ir_url": "https://www.plenti.com.au/shareholders",
        "ir_url_secondary": "https://www.plenti.com.au/shareholders/results",
        "file_pattern": "*results*.pdf",
        "fallback_keywords": [
            "quarterly trading update", "fy25", "fy26", "results",
            "asx", "1q", "2q", "3q", "4q",
        ],
        "fallback_ext": ".pdf",
        "manual_hint": (
            "Plenti IR path renamed from /investors/ to /shareholders. "
            "ASX:PLT. Quarterly trading updates + annual + half-yearly. "
            "Plenti also publishes explicit 90+ DPD and net credit loss "
            "rates in its quarterly updates."
        ),
    },
    "wisr": {
        "ir_url": "https://investorhub.wisr.com.au/",
        "ir_url_secondary": "https://wisr.com.au/shareholders",
        "file_pattern": "*results*.pdf",
        "fallback_keywords": [
            "results", "annual report", "quarterly", "fy25", "fy26",
        ],
        "fallback_ext": ".pdf",
        "manual_hint": (
            "Wisr now uses dedicated investorhub.wisr.com.au for "
            "announcements. ASX:WZR. Quarterly trading updates."
        ),
    },
    "qualitas": {
        "ir_url": "https://investors.qualitas.com.au/investor-centre/",
        "file_pattern": "*results*.pdf",
        "fallback_keywords": [
            "results", "annual report", "1h", "fy25", "fy26",
            "investor presentation",
        ],
        "fallback_ext": ".pdf",
        "manual_hint": (
            "Qualitas IR is on dedicated subdomain "
            "investors.qualitas.com.au (main domain has SSL handshake "
            "quirks). ASX:QAL. Half-yearly + annual."
        ),
    },
    "metrics_credit": {
        "ir_url": "https://www.metrics.com.au/listed-funds/",
        "ir_url_secondary": "https://metrics.com.au/funding-solutions/metrics-real-estate-income-fund/",
        "file_pattern": "*monthly*report*.pdf",
        "fallback_keywords": [
            "mreif", "mxt", "mre", "mot", "monthly report", "fund update",
        ],
        "fallback_ext": ".pdf",
        "manual_hint": (
            "Metrics fund reports live on /listed-funds/ and "
            "/funding-solutions/, not /news-and-insights/. PDFs are "
            "usually under metrics.com.au/wp-content/uploads/YYYY/MM/. "
            "Monthly cadence for fund reports."
        ),
    },
}


@dataclass
class _FetchAttempt:
    """Outcome of one URL attempt — used to combine primary + secondary."""

    url: str
    status: str          # "OK", "MANUAL", "FAIL"
    detail: str          # short detail string
    matched_url: str | None = None  # link found (when status=="OK")
    tier: str | None = None         # match tier (when status=="OK")


class NonBankDisclosureDownloader:
    """Download non-bank ASX-listed lender disclosures with graceful failure."""

    LENDERS = _LENDERS

    def __init__(
        self,
        cache_dir: Path = Path("data/raw/non_bank"),
        max_retries: int = 3,
        timeout: int = 30,
    ) -> None:
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.max_retries = max_retries
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": _UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    def download_lender(
        self, lender: str, force_refresh: bool = False,
    ) -> tuple[str, Path | None, str]:
        """Returns (outcome, path_or_None, detail). outcome in {OK,MANUAL,FAIL}."""
        if lender not in self.LENDERS:
            logger.error("Unknown lender %r", lender)
            return ("FAIL", None, "unknown lender")

        cfg = self.LENDERS[lender]
        lender_dir = self.cache_dir / lender
        lender_dir.mkdir(parents=True, exist_ok=True)

        # Try primary URL; fall through to secondary if defined and primary
        # didn't yield a usable link.
        primary_url = cfg["ir_url"]
        primary_attempt = self._try_fetch_link(primary_url, cfg)

        chosen = primary_attempt
        attempts = [primary_attempt]
        secondary_url = cfg.get("ir_url_secondary")
        if primary_attempt.status != "OK" and secondary_url:
            logger.info(
                "Primary %s for %s did not yield a match (%s); "
                "retrying with secondary %s",
                primary_url, lender, primary_attempt.detail, secondary_url,
            )
            secondary_attempt = self._try_fetch_link(secondary_url, cfg)
            attempts.append(secondary_attempt)
            if secondary_attempt.status == "OK":
                chosen = secondary_attempt

        if chosen.status != "OK":
            # All URLs failed: write a combined manual note.
            combined = "; ".join(
                f"{a.url} -> {a.detail}" for a in attempts
            )
            self._write_manual(lender_dir, lender, cfg, combined)
            return ("MANUAL", lender_dir / "_MANUAL.md", combined)

        # Got a link — download the file.
        assert chosen.matched_url is not None
        filename = self._derive_filename(chosen.matched_url, lender)
        filepath = lender_dir / filename
        if filepath.exists() and not force_refresh:
            logger.info("Using cached %s: %s", lender, filepath)
            # Remove stale _MANUAL.md if a real file is now present.
            self._clear_manual(lender_dir)
            return ("OK", filepath, "cached")

        ok = safe_download(
            self.session, chosen.matched_url, filepath,
            max_retries=self.max_retries, timeout=self.timeout,
            validator=validate_pdf,
        )
        if not ok:
            self._write_manual(
                lender_dir, lender, cfg,
                f"download failed for {chosen.matched_url}",
            )
            return ("MANUAL", lender_dir / "_MANUAL.md", "download failed")

        # Real file in place; clear any stale _MANUAL.md for this lender.
        self._clear_manual(lender_dir)
        return ("OK", filepath, f"matched via {chosen.tier} ({chosen.url})")

    def _try_fetch_link(self, url: str, cfg: dict) -> _FetchAttempt:
        """Fetch ``url`` and look for a matching disclosure link.

        Returns a _FetchAttempt describing the outcome — never raises.
        """
        try:
            r = self.session.get(url, timeout=self.timeout)
        except requests.RequestException as exc:
            return _FetchAttempt(
                url=url, status="MANUAL",
                detail=f"network error: {type(exc).__name__}",
            )

        if r.status_code in (401, 403):
            return _FetchAttempt(
                url=url, status="MANUAL",
                detail=f"HTTP {r.status_code} — bot-protected",
            )
        if r.status_code >= 400:
            return _FetchAttempt(
                url=url, status="MANUAL",
                detail=f"HTTP {r.status_code} — page not found",
            )

        soup = BeautifulSoup(r.content, "html.parser")
        link, tier = self._find_link(
            soup,
            page_url=url,
            file_pattern=cfg["file_pattern"],
            fallback_keywords=cfg["fallback_keywords"],
            exclude_keywords=cfg.get("exclude_keywords", []),
            fallback_ext=cfg["fallback_ext"],
        )
        if not link:
            return _FetchAttempt(
                url=url, status="MANUAL",
                detail=(
                    f"page reachable but no PDF anchor matched "
                    f"glob={cfg['file_pattern']!r} or fallbacks"
                ),
            )
        return _FetchAttempt(
            url=url, status="OK", detail=f"matched via {tier}",
            matched_url=link, tier=tier,
        )

    def download_all(
        self, force_refresh: bool = False,
    ) -> dict[str, tuple[str, Path | None, str]]:
        return {
            lender: self.download_lender(lender, force_refresh=force_refresh)
            for lender in self.LENDERS
        }

    @staticmethod
    def _find_link(
        soup: BeautifulSoup,
        page_url: str,
        file_pattern: str,
        fallback_keywords: list[str],
        exclude_keywords: list[str],
        fallback_ext: str,
    ) -> tuple[str | None, str]:
        pat = file_pattern.lower()
        ext = fallback_ext.lower()
        vetoes = [k.lower() for k in exclude_keywords]

        anchors = soup.find_all("a")

        for a in anchors:
            href = (a.get("href") or "").strip()
            text = a.get_text(" ", strip=True) or ""
            combined = (text + " " + href).lower()
            if vetoes and any(k in combined for k in vetoes):
                continue
            tail = href.rsplit("/", 1)[-1].split("?", 1)[0].lower()
            if fnmatch.fnmatch(tail, pat) or fnmatch.fnmatch(text.lower(), pat):
                return urljoin(page_url, href), "primary-glob"

        needles = [k.lower() for k in fallback_keywords]
        for a in anchors:
            href = (a.get("href") or "").strip().lower()
            text = (a.get_text(" ", strip=True) or "").lower()
            combined = text + " " + href
            if vetoes and any(k in combined for k in vetoes):
                continue
            if not href.split("?", 1)[0].endswith(ext):
                continue
            if any(k in combined for k in needles):
                return urljoin(page_url, a.get("href", "")), "fallback-keywords"

        return None, "none"

    @staticmethod
    def _derive_filename(url: str, lender: str) -> str:
        tail = urlparse(url).path.rsplit("/", 1)[-1]
        if tail and "." in tail:
            return re.sub(r"[^A-Za-z0-9._-]+", "_", tail)
        return f"{lender}_disclosure.pdf"

    def _write_manual(
        self, lender_dir: Path, lender: str, cfg: dict, reason: str,
    ) -> None:
        path = lender_dir / "_MANUAL.md"
        path.write_text(_MANUAL_TEMPLATE.format(
            lender=lender,
            ir_url=cfg["ir_url"],
            ir_url_secondary=cfg.get("ir_url_secondary", "(none)"),
            file_pattern=cfg["file_pattern"],
            captured_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            reason=reason,
            hint=cfg["manual_hint"],
        ), encoding="utf-8")

    @staticmethod
    def _clear_manual(lender_dir: Path) -> None:
        """Remove a stale _MANUAL.md once a real file lands.

        The brief specifies that an obsolete gate must not linger after
        the URL fix succeeds — a stale gate misleads analysts.
        """
        gate = lender_dir / "_MANUAL.md"
        if gate.exists():
            try:
                gate.unlink()
                logger.info("Removed stale gate file %s", gate)
            except OSError as exc:  # pragma: no cover
                logger.warning("Could not remove %s: %s", gate, exc)


_MANUAL_TEMPLATE = """# {lender} — manual download required

Captured: {captured_at}
IR URL (primary):   {ir_url}
IR URL (secondary): {ir_url_secondary}
Reason:   {reason}

**Hint:** {hint}

After manual download, drop the PDF into:
``data/raw/non_bank/{lender}/`` (any filename matching ``{file_pattern}``
will be picked up by the matching adapter).
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Download non-bank ASX disclosures")
    parser.add_argument(
        "--lender",
        choices=list(_LENDERS) + ["all"],
        default="all",
    )
    parser.add_argument("--cache-dir", default="data/raw/non_bank")
    parser.add_argument("--force-refresh", action="store_true")
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--timeout", type=int, default=20)
    args = parser.parse_args()

    dl = NonBankDisclosureDownloader(
        cache_dir=Path(args.cache_dir),
        max_retries=args.max_retries,
        timeout=args.timeout,
    )

    if args.lender == "all":
        results = dl.download_all(force_refresh=args.force_refresh)
        ok = manual = fail = 0
        for lender, (outcome, path, detail) in results.items():
            tag = {"OK": "OK    ", "MANUAL": "MANUAL", "FAIL": "FAIL  "}[outcome]
            print(f"{lender:18s} {tag}  {detail:30s}  {path or ''}")
            if outcome == "OK":
                ok += 1
            elif outcome == "MANUAL":
                manual += 1
            else:
                fail += 1
        print(f"\nSummary: {ok} OK / {manual} MANUAL / {fail} FAIL")
        return 0 if (ok + manual) > 0 else 1

    outcome, path, detail = dl.download_lender(
        args.lender, force_refresh=args.force_refresh,
    )
    print(f"{args.lender}: {outcome}  {detail}  {path or ''}")
    return 0 if outcome != "FAIL" else 1


if __name__ == "__main__":
    raise SystemExit(main())
