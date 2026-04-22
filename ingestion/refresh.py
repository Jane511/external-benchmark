"""Refresh orchestrator: scrape -> transform -> registry.add/supersede.

Conflict resolution (plan §Refresh):
    registry has no entry              -> add
    registry has same value_date+value -> skip (unchanged)
    registry has same value_date, different value -> supersede (correction)
    registry has older value_date      -> supersede (new data)
    registry has newer value_date      -> skip (we already have newer data)

Dry-run mode runs scrape + transform + conflict resolution but does NOT call
`registry.add()` / `registry.supersede()`. The `RefreshReport` still shows the
actions that *would* be taken, so analysts can preview changes before committing.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Literal, Optional

import yaml

from ingestion.apra_adi import ApraAdiScraper
from ingestion.asic_abs_import import ASICABSFailureRateImporter
from ingestion.base import BaseScraper, ScrapedDataPoint
from ingestion.icc_trade import IccTradeScraper
from ingestion.pillar3.anz import ANZScraper
from ingestion.pillar3.cba import CBAScraper
from ingestion.pillar3.nab import NABScraper
from ingestion.pillar3.wbc import WBCScraper
from ingestion.transform import scraped_to_entry
from src.models import BenchmarkEntry
from src.registry import BenchmarkRegistry


DEFAULT_SOURCES_PATH = Path(__file__).parent / "config" / "sources.yaml"

Action = Literal["add", "supersede", "skip_unchanged", "skip_newer_in_registry", "error"]


@dataclass(frozen=True)
class RefreshAction:
    """One outcome in a refresh run — source_id resolved to an action."""
    source_id: str
    action: Action
    reason: str
    value: Optional[float] = None
    value_date: Optional[date] = None


@dataclass
class RefreshReport:
    """Summary of a refresh run across one or more scrapers."""
    source_name: str
    started_at: datetime
    ended_at: datetime
    dry_run: bool
    actions: list[RefreshAction] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def counts(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for a in self.actions:
            out[a.action] = out.get(a.action, 0) + 1
        return out

    def summary(self) -> str:
        parts = [f"{k}={v}" for k, v in sorted(self.counts.items())]
        if self.errors:
            parts.append(f"errors={len(self.errors)}")
        return ", ".join(parts) or "no actions"


class RefreshOrchestrator:
    """Drives scrape -> transform -> registry for all configured sources."""

    _SCRAPER_REGISTRY: dict[str, type[BaseScraper]] = {
        "apra_adi": ApraAdiScraper,
        "pillar3_cba": CBAScraper,
        "pillar3_nab": NABScraper,
        "pillar3_wbc": WBCScraper,
        "pillar3_anz": ANZScraper,
        "icc_trade": IccTradeScraper,
        "asic_abs": ASICABSFailureRateImporter,
    }

    def __init__(
        self,
        registry: BenchmarkRegistry,
        sources_config: Optional[dict[str, Any]] = None,
        local_overrides: Optional[dict[str, Path]] = None,
        scraper_extras: Optional[dict[str, dict[str, Any]]] = None,
    ) -> None:
        """
        Args:
            registry: core engine registry
            sources_config: parsed sources.yaml (loaded from default path if None)
            local_overrides: map of source_name -> local file path
            scraper_extras: map of source_name -> extra kwargs for the scraper
                (e.g. reporting_date, period_code for Pillar 3)
        """
        self._registry = registry
        self._config = (
            sources_config
            if sources_config is not None
            else self._load_sources(DEFAULT_SOURCES_PATH)
        )
        self._overrides = local_overrides or {}
        self._extras = scraper_extras or {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def refresh_source(
        self, source_name: str, *, dry_run: bool = False,
    ) -> RefreshReport:
        """Run one scraper end-to-end and return a report."""
        started = _utcnow()
        report = RefreshReport(
            source_name=source_name, started_at=started,
            ended_at=started, dry_run=dry_run,
        )

        if source_name not in self._config["sources"]:
            report.errors.append(f"Unknown source: {source_name!r}")
            report.ended_at = _utcnow()
            return report

        source_cfg = self._config["sources"][source_name]
        scraper_key = source_cfg["scraper"]
        if scraper_key not in self._SCRAPER_REGISTRY:
            report.errors.append(f"No scraper registered for {scraper_key!r}")
            report.ended_at = _utcnow()
            return report

        try:
            scraper = self._build_scraper(source_name, scraper_key, source_cfg)
            points = scraper.validate(scraper.scrape())
            asset_class_map = source_cfg.get("asset_class_mapping")
            for p in points:
                try:
                    entry = scraped_to_entry(
                        p, override_asset_class_map=asset_class_map,
                    )
                except ValueError as exc:
                    report.errors.append(f"transform: {exc}")
                    continue
                action = self._apply_entry(entry, dry_run=dry_run)
                report.actions.append(action)
        except Exception as exc:  # noqa: BLE001
            report.errors.append(f"{type(exc).__name__}: {exc}")

        report.ended_at = _utcnow()
        return report

    def refresh_all(self, *, dry_run: bool = False) -> list[RefreshReport]:
        reports: list[RefreshReport] = []
        for source_name in self._config["sources"]:
            reports.append(self.refresh_source(source_name, dry_run=dry_run))
        return reports

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _build_scraper(
        self, source_name: str, scraper_key: str, source_cfg: dict[str, Any],
    ) -> BaseScraper:
        cls = self._SCRAPER_REGISTRY[scraper_key]
        path = self._overrides.get(source_name)
        extras = self._extras.get(source_name, {})

        # All currently-wired scrapers take (source_path, config, **extras).
        # When `path` is None, the scraper's cache-aware `_resolve_source_path()`
        # will download via FileDownloader. Stubs (NAB/WBC/ANZ) still fail in
        # scrape() with NotImplementedError, which the orchestrator catches.
        return cls(source_path=path, config=source_cfg, **extras)

    def _apply_entry(
        self, entry: BenchmarkEntry, *, dry_run: bool,
    ) -> RefreshAction:
        """Resolve the conflict policy and optionally write to the registry."""
        history = self._registry.get_version_history(entry.source_id)

        if not history:
            if not dry_run:
                self._registry.add(entry)
            return RefreshAction(
                source_id=entry.source_id, action="add",
                reason="new source_id",
                value=entry.value, value_date=entry.value_date,
            )

        latest = history[-1]
        if latest.value_date > entry.value_date:
            return RefreshAction(
                source_id=entry.source_id, action="skip_newer_in_registry",
                reason=f"registry has value_date {latest.value_date} > scraped {entry.value_date}",
                value=entry.value, value_date=entry.value_date,
            )
        if latest.value_date == entry.value_date and latest.value == entry.value:
            return RefreshAction(
                source_id=entry.source_id, action="skip_unchanged",
                reason="identical value_date and value already in registry",
                value=entry.value, value_date=entry.value_date,
            )
        # Either same date different value, or older date in registry
        if not dry_run:
            self._registry.supersede(entry.source_id, entry)
        reason = (
            "value correction (same date, new value)"
            if latest.value_date == entry.value_date
            else f"newer data ({latest.value_date} -> {entry.value_date})"
        )
        return RefreshAction(
            source_id=entry.source_id, action="supersede",
            reason=reason, value=entry.value, value_date=entry.value_date,
        )

    @staticmethod
    def _load_sources(path: Path) -> dict[str, Any]:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)
