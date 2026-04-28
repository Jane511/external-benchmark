"""Download external rating-agency RMBS indices (S&P SPIN).

Usage:
    python scripts/download_sources/external_indices_downloader.py
    python scripts/download_sources/external_indices_downloader.py --index sp_spin
    python scripts/download_sources/external_indices_downloader.py --dry-run
    python scripts/download_sources/external_indices_downloader.py --force-refresh

Coverage and access tier
------------------------
- **S&P SPIN** (Australian RMBS performance index). The press-release
  page is public but release URLs are generated per article. Downloader
  writes a ``MANUAL.md`` note when no direct PDF link is present.

The corresponding adapter (``ingestion/external_indices/sp_spin_adapter.py``)
accepts "no input" as a valid outcome and parses manually staged PDFs.
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from common import logger, safe_download, validate_by_extension  # type: ignore
else:
    from .common import logger, safe_download, validate_by_extension


_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0 Safari/537.36 external-benchmark-engine/0.2"
)


_INDICES: dict[str, dict] = {
    "sp_spin": {
        "landing": "https://www.spglobal.com/ratings/en/regulatory/topic/spin",
        "primary_globs": ("*spin*.pdf", "*australian*rmbs*.pdf"),
        "manual_hint": (
            "S&P SPIN is a free public source, but each release has a "
            "generated URL and there is no stable direct download link. "
            "Download the latest PDF manually and stage it here."
        ),
    },
}


_MANUAL_TEMPLATE = """# {index} — manual download required

Captured: {captured_at}
Landing:  {landing}
Reason:   {reason}

**Hint:** {hint}

After manual download, drop the file into:
``data/raw/external_indices/{index}/`` for the matching adapter.
"""


class ExternalIndicesDownloader:
    INDICES = _INDICES

    def __init__(
        self,
        cache_dir: Path = Path("data/raw/external_indices"),
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
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        })

    def download_index(
        self, index: str, force_refresh: bool = False,
    ) -> tuple[str, Path | None, str]:
        if index not in self.INDICES:
            return ("FAIL", None, "unknown index")
        cfg = self.INDICES[index]
        idx_dir = self.cache_dir / index
        idx_dir.mkdir(parents=True, exist_ok=True)

        try:
            r = self.session.get(cfg["landing"], timeout=self.timeout)
        except requests.RequestException as exc:
            self._write_manual(idx_dir, index, cfg, str(exc))
            return ("MANUAL", idx_dir / "_MANUAL.md",
                    f"network error: {type(exc).__name__}")

        if r.status_code in (401, 403):
            self._write_manual(idx_dir, index, cfg,
                               f"HTTP {r.status_code} — bot/login-protected")
            return ("MANUAL", idx_dir / "_MANUAL.md",
                    f"HTTP {r.status_code}")

        if r.status_code >= 400:
            self._write_manual(idx_dir, index, cfg, f"HTTP {r.status_code}")
            return ("MANUAL", idx_dir / "_MANUAL.md",
                    f"HTTP {r.status_code}")

        # Snapshot the landing page so the adapter has a stable artefact.
        landing_html = idx_dir / "landing.html"
        landing_html.write_text(r.text, encoding="utf-8")

        # Try to pull a directly-linked file matching the primary globs.
        soup = BeautifulSoup(r.content, "html.parser")
        url = self._find_link(soup, cfg["landing"], cfg["primary_globs"])
        if not url:
            self._write_manual(
                idx_dir, index, cfg,
                "page reachable but no direct download link matched",
            )
            return ("MANUAL", idx_dir / "_MANUAL.md",
                    "no link matched")

        filename = self._derive_filename(url, index)
        filepath = idx_dir / filename
        if filepath.exists() and not force_refresh:
            logger.info("Using cached %s: %s", index, filepath)
            return ("OK", filepath, "cached")

        ok = safe_download(
            self.session, url, filepath,
            max_retries=self.max_retries, timeout=self.timeout,
            validator=validate_by_extension,
        )
        if not ok:
            self._write_manual(idx_dir, index, cfg, f"download failed for {url}")
            return ("MANUAL", idx_dir / "_MANUAL.md", "download failed")
        return ("OK", filepath, "downloaded")

    def download_all(
        self, force_refresh: bool = False,
    ) -> dict[str, tuple[str, Path | None, str]]:
        return {
            idx: self.download_index(idx, force_refresh=force_refresh)
            for idx in self.INDICES
        }

    @staticmethod
    def _find_link(
        soup: BeautifulSoup, page_url: str, globs: tuple[str, ...],
    ) -> str | None:
        import fnmatch
        for a in soup.select("a"):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            tail = href.rsplit("/", 1)[-1].split("?", 1)[0].lower()
            for g in globs:
                if fnmatch.fnmatch(tail, g.lower()):
                    return urljoin(page_url, href)
        return None

    @staticmethod
    def _derive_filename(url: str, index: str) -> str:
        tail = urlparse(url).path.rsplit("/", 1)[-1]
        if tail and "." in tail:
            return re.sub(r"[^A-Za-z0-9._-]+", "_", tail)
        return f"{index}_latest.pdf"

    def _write_manual(
        self, idx_dir: Path, index: str, cfg: dict, reason: str,
    ) -> None:
        path = idx_dir / "_MANUAL.md"
        path.write_text(_MANUAL_TEMPLATE.format(
            index=index,
            captured_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            landing=cfg["landing"],
            reason=reason,
            hint=cfg["manual_hint"],
        ), encoding="utf-8")


def main() -> int:
    p = argparse.ArgumentParser(description="Download external RMBS indices")
    p.add_argument("--index", choices=list(_INDICES) + ["all"], default="all")
    p.add_argument("--cache-dir", default="data/raw/external_indices")
    p.add_argument("--force-refresh", action="store_true")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print configured indices without fetching external sites.",
    )
    p.add_argument("--timeout", type=int, default=20)
    args = p.parse_args()

    if args.dry_run:
        indices = _INDICES if args.index == "all" else {args.index: _INDICES[args.index]}
        for idx, cfg in indices.items():
            print(f"{idx}: {cfg['landing']}")
        return 0

    dl = ExternalIndicesDownloader(
        cache_dir=Path(args.cache_dir), timeout=args.timeout,
    )
    if args.index == "all":
        results = dl.download_all(force_refresh=args.force_refresh)
        ok = manual = fail = 0
        for idx, (outcome, path, detail) in results.items():
            tag = {"OK": "OK    ", "MANUAL": "MANUAL", "FAIL": "FAIL  "}[outcome]
            print(f"{idx:25s} {tag}  {detail:30s}  {path or ''}")
            if outcome == "OK": ok += 1
            elif outcome == "MANUAL": manual += 1
            else: fail += 1
        print(f"\nSummary: {ok} OK / {manual} MANUAL / {fail} FAIL")
        return 0 if (ok + manual) > 0 else 1

    outcome, path, detail = dl.download_index(
        args.index, force_refresh=args.force_refresh,
    )
    print(f"{args.index}: {outcome}  {detail}  {path or ''}")
    return 0 if outcome != "FAIL" else 1


if __name__ == "__main__":
    raise SystemExit(main())
