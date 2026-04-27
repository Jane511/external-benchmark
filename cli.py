"""Click-based CLI for the External Benchmark Engine.

Usage examples (from project root):

    python cli.py --db ./bench.db seed
    python cli.py --db ./bench.db list
    python cli.py --db ./bench.db history CBA_PILLAR3_RES_2024H2
    python cli.py --db ./bench.db report stale
    python cli.py --db ./bench.db report coverage --segment residential_mortgage
    python cli.py --db ./bench.db feed central_tendency --segment residential_mortgage
    python cli.py --db ./bench.db export --format json > bench.json

Reuses a single SQLite file (default `./benchmarks.db`) across invocations,
so `seed` then `list` work as you'd expect.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import click

from src.db import create_engine_and_schema
from src.governance import GovernanceReporter
from src.models import InstitutionType, SourceType
from src.observations import PeerObservations
from src.registry import BenchmarkRegistry
from src.seed_data import load_seed_data

# NOTE (Brief 1):
#   src.adjustments, src.calibration_feed, src.triangulation are now
#   deprecation stubs that raise on import. The CLI no longer wires
#   them up. The legacy `feed` and `--institution` flags emit a
#   deprecation message and exit; use `observations` for the raw API.

# Ingestion layer (lazy-imported by subcommands so missing extras don't break other commands).


DEFAULT_DB = "./benchmarks.db"


def _get_registry(db_path: str) -> BenchmarkRegistry:
    engine = create_engine_and_schema(db_path)
    return BenchmarkRegistry(engine, actor="cli")


def _get_governance(db_path: str, institution: str):
    """Build registry + governance reporter (institution kept for governance only)."""
    engine = create_engine_and_schema(db_path)
    inst = (
        InstitutionType.BANK if institution == "bank"
        else InstitutionType.PRIVATE_CREDIT
    )
    registry = BenchmarkRegistry(engine, actor="cli")
    return registry, inst


# ---------------------------------------------------------------------------
# Root group
# ---------------------------------------------------------------------------

@click.group(help="External Benchmark Engine CLI.")
@click.option(
    "--db", type=click.Path(), default=DEFAULT_DB, show_default=True,
    help="SQLite database path.",
)
@click.option(
    "--institution", type=click.Choice(["bank", "private_credit"]),
    default="bank", show_default=True,
    help="[DEPRECATED — Brief 1] Engine outputs are institution-agnostic; "
         "adjustments live in consuming projects. Retained for governance "
         "report templating only.",
)
@click.pass_context
def cli(ctx: click.Context, db: str, institution: str) -> None:
    ctx.ensure_object(dict)
    ctx.obj["db"] = db
    ctx.obj["institution"] = institution
    if institution != "bank":
        # Print on non-default value — the user explicitly opted in.
        click.echo(
            "NOTE: --institution is deprecated. The engine now publishes raw "
            "observations only; adjustments are applied by consuming projects "
            "(e.g. the PD workbook). The same engine output is consumed by all "
            "institution types. The flag is still honoured for the governance "
            "subreports, but its scope is shrinking.",
            err=True,
        )


# ---------------------------------------------------------------------------
# seed
# ---------------------------------------------------------------------------

@cli.command(help="Load the bundled Australian seed data into the registry.")
@click.pass_context
def seed(ctx: click.Context) -> None:
    registry = _get_registry(ctx.obj["db"])
    count = load_seed_data(registry)
    click.echo(f"Loaded {count} seed entries into {ctx.obj['db']}")


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------

@cli.command("list", help="List latest-version benchmark entries.")
@click.option("--source-type", help="Filter by source type (e.g. pillar3, rating_agency).")
@click.pass_context
def cmd_list(ctx: click.Context, source_type: str | None) -> None:
    registry = _get_registry(ctx.obj["db"])
    entries = registry.list()
    if source_type:
        entries = [e for e in entries if e.source_type.value == source_type]
    for e in entries:
        click.echo(
            f"{e.source_id:<45} {e.source_type.value:<15} "
            f"{e.data_type.value:<10} {e.asset_class:<32} value={e.value}"
        )
    click.echo(f"\n{len(entries)} entries.")


# ---------------------------------------------------------------------------
# history
# ---------------------------------------------------------------------------

@cli.command(help="Show version history for a source_id.")
@click.argument("source_id")
@click.pass_context
def history(ctx: click.Context, source_id: str) -> None:
    registry = _get_registry(ctx.obj["db"])
    versions = registry.get_version_history(source_id)
    if not versions:
        click.echo(f"No versions found for source_id={source_id!r}")
        return
    for v in versions:
        marker = "  " if v.superseded_by else "→ "
        click.echo(
            f"{marker}v{v.version}  value={v.value}  "
            f"retrieved={v.retrieval_date.isoformat()}  "
            f"superseded_by={v.superseded_by or '(latest)'}"
        )


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------

@cli.group(
    "report",
    help="Governance reports (stale / quality / coverage / peer / annual) + "
         "committee reports (benchmark / environment / combined).",
)
def report() -> None:
    pass


def _run_governance(
    ctx: click.Context, report_type: str, segments: list[str],
) -> None:
    """Shared dispatch for the five governance subcommands."""
    registry, inst = _get_governance(ctx.obj["db"], ctx.obj["institution"])
    reporter = GovernanceReporter(registry, inst)

    if report_type == "stale":
        rep = reporter.stale_benchmark_report()
    elif report_type == "quality":
        rep = reporter.quality_assessment_report()
    elif report_type == "coverage":
        if not segments:
            raise click.UsageError("Coverage report requires --segment ...")
        rep = reporter.coverage_report(segments)
    elif report_type == "peer":
        raise click.UsageError(
            "peer report needs an own-estimates JSON input — use the Python API for now."
        )
    elif report_type == "annual":
        rep = reporter.annual_review_package(segments or ["residential_mortgage"])
    else:   # pragma: no cover
        raise click.UsageError(f"Unknown report type: {report_type}")

    click.echo(f"Report: {rep.report_type}  institution={rep.institution_type.value}")
    click.echo(f"Flags ({len(rep.flags)}):")
    for flag in rep.flags:
        click.echo(f"  * {flag}")
    click.echo(f"Findings: {len(rep.findings)}")


# --- governance subcommands (preserve existing `report <type>` UX) ---------

@report.command("stale", help="Flag benchmarks older than their source-type cadence.")
@click.pass_context
def report_stale(ctx: click.Context) -> None:
    _run_governance(ctx, "stale", [])


@report.command("quality", help="Emit the 5-dimension quality matrix per source type.")
@click.pass_context
def report_quality(ctx: click.Context) -> None:
    _run_governance(ctx, "quality", [])


@report.command("coverage", help="Flag segments with < 2 distinct external sources.")
@click.option("--segment", multiple=True, help="Segment to check; repeat for more.")
@click.pass_context
def report_coverage(ctx: click.Context, segment: tuple[str, ...]) -> None:
    _run_governance(ctx, "coverage", list(segment))


@report.command("peer", help="Peer comparison (requires own_estimates; CLI stub).")
@click.pass_context
def report_peer(ctx: click.Context) -> None:
    _run_governance(ctx, "peer", [])


@report.command("annual", help="Aggregate governance package (MRC or Credit Committee header).")
@click.option("--segment", multiple=True)
@click.pass_context
def report_annual(ctx: click.Context, segment: tuple[str, ...]) -> None:
    _run_governance(ctx, "annual", list(segment))


# --- committee reports -----------------------------------------------------

@report.command("benchmark",
                help="Report 1 — External Benchmark RAW Observation Summary.")
@click.option("--format", "fmt",
              type=click.Choice(["docx", "html", "markdown"]),
              required=True)
@click.option("--institution-type",
              type=click.Choice(["bank", "private_credit"]),
              default=None,
              help="[DEPRECATED — Brief 1] Reports are now institution-agnostic. "
                   "Accepted for backward compatibility but ignored.")
@click.option("--output", type=click.Path(), default=None,
              help="Output path. Defaults to outputs/reports/Report_{period}_RawOnly.{ext}.")
@click.option("--period-label", default=None,
              help="Period label (e.g. 'Q3 2025'). Derived from today if omitted.")
@click.option("--source-type", default=None,
              help="Optional filter — only include this source_type "
                   "(e.g. bank_pillar3, non_bank_listed, rating_agency_index).")
@click.pass_context
def report_benchmark(
    ctx: click.Context,
    fmt: str,
    institution_type: Optional[str],
    output: Optional[str],
    period_label: Optional[str],
    source_type: Optional[str],
) -> None:
    from reports.benchmark_report import BenchmarkCalibrationReport

    if institution_type is not None:
        click.echo(
            "NOTE: --institution-type is deprecated. The engine now publishes "
            "raw observations only; the report is the same regardless of "
            "consumer institution. Adjustments live in the consuming project.",
            err=True,
        )

    engine = create_engine_and_schema(ctx.obj["db"])
    registry = BenchmarkRegistry(engine, actor="cli")
    peer = PeerObservations(registry)
    report_obj = BenchmarkCalibrationReport(
        registry=registry,
        peer_observations=peer,
        period_label=period_label,
    )

    ext = {"docx": "docx", "html": "html", "markdown": "md"}[fmt]
    period_slug = (period_label or report_obj._period).replace(" ", "_")
    default_path = (
        Path("outputs/reports") / f"Report_{period_slug}_RawOnly.{ext}"
    )
    out_path = Path(output) if output else default_path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if fmt == "docx":
        report_obj.to_docx(out_path)
    elif fmt == "html":
        out_path.write_text(report_obj.to_html(), encoding="utf-8")
    else:  # markdown
        out_path.write_text(report_obj.to_markdown(), encoding="utf-8")
    click.echo(f"Report written: {out_path} ({out_path.stat().st_size} bytes)")
    if source_type:
        click.echo(
            f"NOTE: --source-type {source_type!r} was supplied but the report "
            "currently emits all source types; pass the filter into "
            "PeerObservations.for_segment(source_type=...) for programmatic use.",
            err=True,
        )


@report.command("environment",
                help="Report 2 — industry / property environment. Sources "
                     "data from the industry-analysis sibling project.")
@click.option("--format", "fmt",
              type=click.Choice(["docx", "html", "markdown", "all"]),
              default="all", show_default=True)
@click.option("--data-dir", type=click.Path(), default=None,
              help="Path to industry-analysis/data/exports/. Defaults to "
                   "$EXTERNAL_BENCHMARK_INDUSTRY_ANALYSIS_DIR, then the "
                   "known-sibling-repo path.")
@click.option("--output", type=click.Path(), default=None,
              help="Output stem. Defaults to outputs/reports/"
                   "Report_Environment_<period>.<ext>.")
@click.option("--period-label", default=None)
@click.option("--stale-days", type=int, default=90, show_default=True)
@click.pass_context
def report_environment(
    ctx: click.Context,
    fmt: str,
    data_dir: Optional[str],
    output: Optional[str],
    period_label: Optional[str],
    stale_days: int,
) -> None:
    """Generate Report 2 in the requested format(s).

    This is a thin shim over `scripts/generate_reports.py environment`;
    both entrypoints share the same underlying renderer.
    """
    # Forward to the standalone generator so there is exactly one
    # implementation of the Report-2 emit logic. We re-invoke via the
    # click runner so option parsing stays consistent.
    from scripts.generate_reports import environment as env_cmd

    argv: list[str] = ["--format", fmt]
    if data_dir:
        argv.extend(["--data-dir", data_dir])
    if output:
        argv.extend(["--output", output])
    if period_label:
        argv.extend(["--period-label", period_label])
    if stale_days != 90:
        argv.extend(["--stale-days", str(stale_days)])
    ctx.invoke(env_cmd,
               fmt=fmt,
               data_dir=data_dir,
               output=output,
               period_label=period_label,
               stale_days=stale_days,
               verify=False)


@report.command("combined",
                help="Report 3 — combined dashboard (requires Reports 1 + 2; "
                     "not yet implemented).")
@click.pass_context
def report_combined(ctx: click.Context) -> None:
    click.echo(
        "Report 3 (Combined Dashboard) is not yet implemented. It needs "
        "Report 2 (Environment) wired up first, which depends on the "
        "industry-analysis sibling project.",
        err=True,
    )
    ctx.exit(2)


# ---------------------------------------------------------------------------
# feed (calibration feed)
# ---------------------------------------------------------------------------

@cli.command(help="[DEPRECATED — Brief 1] Use `observations` for the raw-only API.")
@click.argument("method", required=False)
@click.option("--segment", default=None)
@click.pass_context
def feed(ctx: click.Context, method: Optional[str], segment: Optional[str]) -> None:
    raise click.ClickException(
        "The `feed` command is deprecated. The engine no longer triangulates "
        "or applies adjustments — see Brief 1. Use:\n"
        "    python cli.py observations --segment <segment>\n"
        "for the raw per-source observations the consuming project now reads."
    )


# ---------------------------------------------------------------------------
# observations — raw per-source observation query (replaces `feed`)
# ---------------------------------------------------------------------------

@cli.command(help="List raw per-source observations for a segment.")
@click.option("--segment", required=True, help="Canonical segment ID.")
@click.option("--source-type", default=None,
              help="Optional filter (bank_pillar3, non_bank_listed, "
                   "rating_agency_index, rba_aggregate, ...).")
@click.option("--include-non-banks/--exclude-non-banks", default=True,
              show_default=True,
              help="Whether to include non-bank ASX-listed sources.")
@click.option("--format", "fmt", type=click.Choice(["json", "table"]),
              default="table", show_default=True)
@click.pass_context
def observations(
    ctx: click.Context, segment: str, source_type: Optional[str],
    include_non_banks: bool, fmt: str,
) -> None:
    registry = _get_registry(ctx.obj["db"])
    peer = PeerObservations(registry)
    st: Optional[SourceType] = None
    if source_type:
        try:
            st = SourceType(source_type)
        except ValueError as exc:
            raise click.ClickException(
                f"Unknown source_type {source_type!r}. Valid: "
                f"{[s.value for s in SourceType]}"
            ) from exc

    obs_set = peer.for_segment(segment, source_type=st)
    rows = obs_set.observations
    if not include_non_banks:
        rows = obs_set.by_source_type(big4_only=True)

    if not rows:
        click.echo(f"No observations for segment={segment!r}.")
        return

    if fmt == "json":
        payload = [o.model_dump(mode="json") for o in rows]
        flags = obs_set.validation_flags
        click.echo(json.dumps({
            "segment": segment,
            "observations": payload,
            "validation_flags": {
                "n_sources": flags.n_sources,
                "spread_pct": flags.spread_pct,
                "big4_spread_pct": flags.big4_spread_pct,
                "bank_vs_nonbank_ratio": flags.bank_vs_nonbank_ratio,
                "outlier_sources": flags.outlier_sources,
                "stale_sources": flags.stale_sources,
            },
        }, indent=2, default=str))
        return

    click.echo(f"Segment: {segment}  ({len(rows)} observations)")
    for o in rows:
        click.echo(
            f"  {o.source_id:<20} {o.source_type.value:<22} "
            f"{o.parameter:<4} {o.value:.4%}  as_of={o.as_of_date.isoformat()}  "
            f"basis={o.reporting_basis}"
        )
    flags = obs_set.validation_flags
    click.echo("")
    click.echo(
        f"Flags: n={flags.n_sources}, "
        f"spread={(flags.spread_pct or 0)*100:.1f}%, "
        f"big4_spread={(flags.big4_spread_pct or 0)*100:.1f}%, "
        f"nonbank/big4={flags.bank_vs_nonbank_ratio or '-'}, "
        f"outliers={flags.outlier_sources or 'none'}"
    )


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------

@cli.command(help="Export latest-version registry contents as JSON or CSV.")
@click.option("--format", "fmt", type=click.Choice(["json", "csv"]), default="json")
@click.option("--output", type=click.Path(), default=None,
              help="File path; prints to stdout if omitted.")
@click.pass_context
def export(ctx: click.Context, fmt: str, output: str | None) -> None:
    registry = _get_registry(ctx.obj["db"])
    payload = registry.export(format=fmt)
    if output:
        Path(output).write_text(payload, encoding="utf-8")
        click.echo(f"Wrote {output} ({len(payload)} chars)")
    else:
        click.echo(payload)


# ---------------------------------------------------------------------------
# ingest — automated data collection (Phase 1: APRA ADI)
# ---------------------------------------------------------------------------

@cli.group(help="Ingest external benchmark data into the registry.")
def ingest() -> None:
    pass


@ingest.command("apra", help="Scrape APRA ADI statistics (downloads + caches by default).")
@click.option(
    "--source-path", type=click.Path(exists=True, dir_okay=False),
    default=None,
    help="[deprecated] Path to a pre-downloaded APRA XLSX. Omit to use the cache/download path.",
)
@click.option(
    "--force-refresh", is_flag=True,
    help="Ignore any cached copy and re-download the APRA XLSX.",
)
@click.option("--dry-run", is_flag=True, help="Preview actions without writing.")
@click.option(
    "--source-key", default="apra_adi_performance", show_default=True,
    help="sources.yaml key to use (e.g. apra_adi_performance or apra_qpex).",
)
@click.pass_context
def ingest_apra(
    ctx: click.Context, source_path: Optional[str], force_refresh: bool,
    dry_run: bool, source_key: str,
) -> None:
    from ingestion.refresh import RefreshOrchestrator

    registry = _get_registry(ctx.obj["db"])
    overrides: dict[str, Path] = {}
    if source_path:
        overrides[source_key] = Path(source_path)

    extras: dict[str, dict] = {}
    if force_refresh:
        extras[source_key] = {"force_refresh": True}

    orchestrator = RefreshOrchestrator(
        registry=registry,
        local_overrides=overrides,
        scraper_extras=extras or None,
    )
    report = orchestrator.refresh_source(source_key, dry_run=dry_run)

    click.echo(f"Source: {report.source_name}  dry_run={report.dry_run}")
    click.echo(f"Summary: {report.summary()}")
    if report.errors:
        for err in report.errors:
            click.echo(f"  ERROR: {err}", err=True)
    click.echo(f"\nActions ({len(report.actions)}):")
    for a in report.actions:
        click.echo(
            f"  {a.action:<25}  {a.source_id}   value={a.value}  "
            f"value_date={a.value_date}   ({a.reason})"
        )


# --- pillar3 subgroup ------------------------------------------------------

_PILLAR3_BANKS: dict[str, str] = {
    "cba": "cba_pillar3",
    "nab": "nab_pillar3",
    "wbc": "wbc_pillar3",
    "anz": "anz_pillar3",
}


def _run_pillar3(
    ctx: click.Context,
    source_key: str,
    *,
    source_path: str | None,
    dry_run: bool,
    reporting_date,
    period_code: str | None,
    force_refresh: bool = False,
    allow_cache_download: bool = False,
) -> int:
    """Run one Pillar 3 scraper. Returns the exit code (0 ok, 1 error/skip).

    If source_path is None and allow_cache_download is False, the scraper is
    skipped with a message. If allow_cache_download is True, the scraper falls
    through to its FileDownloader path.
    """
    from ingestion.refresh import RefreshOrchestrator

    if source_path is None and not allow_cache_download:
        click.echo(f"  {source_key}: skipped (no source path provided)")
        return 0

    registry = _get_registry(ctx.obj["db"])
    extras: dict[str, object] = {}
    if reporting_date is not None:
        extras["reporting_date"] = (
            reporting_date.date()
            if hasattr(reporting_date, "date") else reporting_date
        )
    if period_code is not None:
        extras["period_code"] = period_code
    if force_refresh:
        extras["force_refresh"] = True

    overrides: dict[str, Path] = {}
    if source_path:
        overrides[source_key] = Path(source_path)

    orchestrator = RefreshOrchestrator(
        registry=registry,
        local_overrides=overrides,
        scraper_extras={source_key: extras} if extras else None,
    )
    report = orchestrator.refresh_source(source_key, dry_run=dry_run)

    click.echo(f"Source: {report.source_name}  dry_run={report.dry_run}")
    click.echo(f"Summary: {report.summary()}")
    for err in report.errors:
        click.echo(f"  ERROR: {err}", err=True)
    click.echo(f"\nActions ({len(report.actions)}):")
    for a in report.actions:
        click.echo(
            f"  {a.action:<25}  {a.source_id}   value={a.value}  "
            f"value_date={a.value_date}   ({a.reason})"
        )
    return 1 if report.errors else 0


@ingest.group("pillar3", invoke_without_command=True,
              help="Ingest Big 4 Pillar 3 disclosures.")
@click.option("--cba-path", type=click.Path(exists=True, dir_okay=False),
              default=None, help="CBA Excel companion path (used in group mode).")
@click.option("--nab-path", type=click.Path(exists=True, dir_okay=False),
              default=None, help="NAB Pillar 3 PDF path.")
@click.option("--wbc-path", type=click.Path(exists=True, dir_okay=False),
              default=None, help="Westpac Pillar 3 PDF path.")
@click.option("--anz-path", type=click.Path(exists=True, dir_okay=False),
              default=None, help="ANZ Pillar 3 PDF path.")
@click.option("--dry-run", is_flag=True)
@click.option("--reporting-date", type=click.DateTime(["%Y-%m-%d"]), default=None,
              help="Reporting period end date (e.g. 2025-06-30).")
@click.option("--period-code", default=None,
              help="Explicit period code for source_id (e.g. FY2025).")
@click.option("--force-refresh", is_flag=True,
              help="Re-download even when a cached file exists.")
@click.pass_context
def ingest_pillar3(
    ctx: click.Context,
    cba_path: str | None, nab_path: str | None,
    wbc_path: str | None, anz_path: str | None,
    dry_run: bool, reporting_date, period_code: str | None,
    force_refresh: bool,
) -> None:
    """When invoked without a bank subcommand, attempts each Big 4 in turn.

    Any bank whose `--<bank>-path` is omitted falls through to the
    FileDownloader / cache path (real PDFs would be fetched live).
    """
    if ctx.invoked_subcommand is not None:
        return

    for label, source_key, path in [
        ("CBA", "cba_pillar3", cba_path),
        ("NAB", "nab_pillar3", nab_path),
        ("WBC", "wbc_pillar3", wbc_path),
        ("ANZ", "anz_pillar3", anz_path),
    ]:
        click.echo(f"--- {label} ---")
        _run_pillar3(
            ctx, source_key,
            source_path=path, dry_run=dry_run,
            reporting_date=reporting_date, period_code=period_code,
            force_refresh=force_refresh,
            allow_cache_download=True,
        )


def _make_pillar3_bank_command(bank_label: str, source_key: str):
    """Factory for per-bank `ingest pillar3 <bank>` subcommands.

    CBA (Excel), NAB, WBC, ANZ (all PDF) share the same option surface —
    the only difference is which `source_key` they pass through to the
    orchestrator, which picks up bank-specific config from sources.yaml.
    """
    @click.option(
        "--source-path", type=click.Path(exists=True, dir_okay=False),
        default=None,
        help="Pre-downloaded file path. Omit to use cache/download.",
    )
    @click.option("--force-refresh", is_flag=True,
                  help="Ignore any cached copy and re-download.")
    @click.option("--dry-run", is_flag=True)
    @click.option("--reporting-date", type=click.DateTime(["%Y-%m-%d"]), default=None)
    @click.option("--period-code", default=None,
                  help=f"Override the auto-computed {bank_label} period code.")
    @click.pass_context
    def _cmd(
        ctx: click.Context, source_path: Optional[str], force_refresh: bool,
        dry_run: bool, reporting_date, period_code: str | None,
    ) -> None:
        rc = _run_pillar3(
            ctx, source_key,
            source_path=source_path, dry_run=dry_run,
            reporting_date=reporting_date, period_code=period_code,
            force_refresh=force_refresh, allow_cache_download=True,
        )
        if rc:
            ctx.exit(rc)

    _cmd.__doc__ = f"Scrape {bank_label} Pillar 3 disclosure."
    return _cmd


# Register the four bank subcommands using the factory.
ingest_pillar3.command("cba", help="Scrape CBA Pillar 3 Excel companion.")(
    _make_pillar3_bank_command("CBA", "cba_pillar3")
)
ingest_pillar3.command("nab", help="Scrape NAB Pillar 3 PDF.")(
    _make_pillar3_bank_command("NAB", "nab_pillar3")
)
ingest_pillar3.command("wbc", help="Scrape Westpac Pillar 3 PDF.")(
    _make_pillar3_bank_command("WBC", "wbc_pillar3")
)
ingest_pillar3.command("anz", help="Scrape ANZ Pillar 3 PDF.")(
    _make_pillar3_bank_command("ANZ", "anz_pillar3")
)


# --- ASIC + ABS failure-rate importer (Phase 6) ---------------------------

@ingest.command("asic-abs",
                help="Combine ASIC insolvency + ABS business counts into failure rates.")
@click.option(
    "--asic-dir", type=click.Path(), default=None,
    help="Directory containing ASIC extracts (default: data/asic/).",
)
@click.option(
    "--abs-dir", type=click.Path(), default=None,
    help="Directory containing ABS cat. 8165 extracts (default: data/abs/).",
)
@click.option("--force-refresh", is_flag=True,
              help="Accepted for CLI parity; no-op for importer sources.")
@click.option("--dry-run", is_flag=True)
@click.pass_context
def ingest_asic_abs(
    ctx: click.Context, asic_dir: Optional[str], abs_dir: Optional[str],
    force_refresh: bool, dry_run: bool,
) -> None:
    from ingestion.refresh import RefreshOrchestrator

    registry = _get_registry(ctx.obj["db"])
    extras: dict[str, object] = {}
    if asic_dir:
        extras["asic_dir"] = asic_dir
    if abs_dir:
        extras["abs_dir"] = abs_dir
    if force_refresh:
        extras["force_refresh"] = True

    orchestrator = RefreshOrchestrator(
        registry=registry,
        scraper_extras={"asic_abs": extras} if extras else None,
    )
    report = orchestrator.refresh_source("asic_abs", dry_run=dry_run)
    _print_refresh_report(report)


def _print_refresh_report(report) -> None:
    """Shared output format for ingest subcommands."""
    click.echo(f"Source: {report.source_name}  dry_run={report.dry_run}")
    click.echo(f"Summary: {report.summary()}")
    for err in report.errors:
        click.echo(f"  ERROR: {err}", err=True)
    click.echo(f"\nActions ({len(report.actions)}):")
    for a in report.actions:
        click.echo(
            f"  {a.action:<25}  {a.source_id}   value={a.value}  "
            f"value_date={a.value_date}   ({a.reason})"
        )


# --- ICC Trade Register (Phase 3; manual-download) -----------------------

@ingest.command("icc", help="Parse a manually-downloaded ICC Trade Register PDF.")
@click.option(
    "--source-path", type=click.Path(exists=True, dir_okay=False), default=None,
    help="Path to a downloaded ICC Trade Register PDF. Omit to auto-discover in data/raw/icc/.",
)
@click.option(
    "--report-year", type=int, default=None,
    help="Which edition to parse (e.g. 2024). Omit for latest cached.",
)
@click.option("--force-refresh", is_flag=True,
              help="Accepted for CLI parity; no-op for manual-download sources.")
@click.option("--dry-run", is_flag=True, help="Preview actions without writing.")
@click.pass_context
def ingest_icc(
    ctx: click.Context, source_path: Optional[str], report_year: Optional[int],
    force_refresh: bool, dry_run: bool,
) -> None:
    from ingestion.refresh import RefreshOrchestrator

    registry = _get_registry(ctx.obj["db"])
    extras: dict[str, object] = {}
    if report_year is not None:
        extras["report_year"] = report_year
    if force_refresh:
        extras["force_refresh"] = True

    overrides: dict[str, Path] = {}
    if source_path:
        overrides["icc_trade"] = Path(source_path)

    orchestrator = RefreshOrchestrator(
        registry=registry,
        local_overrides=overrides,
        scraper_extras={"icc_trade": extras} if extras else None,
    )
    report = orchestrator.refresh_source("icc_trade", dry_run=dry_run)

    click.echo(f"Source: {report.source_name}  dry_run={report.dry_run}")
    click.echo(f"Summary: {report.summary()}")
    for err in report.errors:
        click.echo(f"  ERROR: {err}", err=True)
    click.echo(f"\nActions ({len(report.actions)}):")
    for a in report.actions:
        click.echo(
            f"  {a.action:<25}  {a.source_id}   value={a.value}  "
            f"value_date={a.value_date}   ({a.reason})"
        )


@ingest.command("status", help="Show last retrieval date per source type.")
@click.pass_context
def ingest_status(ctx: click.Context) -> None:
    registry = _get_registry(ctx.obj["db"])
    entries = registry.list()
    if not entries:
        click.echo("Registry is empty.")
        return
    latest_by_source_type: dict[str, str] = {}
    for e in entries:
        key = e.source_type.value
        cur = latest_by_source_type.get(key)
        if cur is None or e.retrieval_date.isoformat() > cur:
            latest_by_source_type[key] = e.retrieval_date.isoformat()
    click.echo("Last retrieval date by source_type:")
    for src, dt in sorted(latest_by_source_type.items()):
        click.echo(f"  {src:<16} {dt}")


# ---------------------------------------------------------------------------
# cache — inspect / clear the raw download cache
# ---------------------------------------------------------------------------

@cli.group("cache", help="Manage the raw-data download cache.")
def cache_group() -> None:
    pass


@cache_group.command("status", help="Show cached files per source.")
@click.option(
    "--cache-base", type=click.Path(), default="data/raw", show_default=True,
    help="Root directory for cached raw files.",
)
def cache_status_cmd(cache_base: str) -> None:
    from ingestion.cache_manager import CacheManager

    mgr = CacheManager(cache_base=cache_base)
    status = mgr.cache_status()
    any_hits = any(info["count"] > 0 for info in status.values())
    if not any_hits:
        click.echo("No cached files.")
        return
    click.echo(f"Cache root: {cache_base}")
    for subdir, info in sorted(status.items()):
        keys = ",".join(info["source_keys"])
        if info["count"] == 0:
            click.echo(f"  {subdir:<10}  (empty)              keys=[{keys}]")
        else:
            click.echo(
                f"  {subdir:<10}  {info['count']} file(s), "
                f"latest={info['latest']} "
                f"age={info['latest_age_days']}d  keys=[{keys}]"
            )


@cache_group.command("clear", help="Clear cached files (one subdir or all).")
@click.option("--source", default=None,
              help="Subdir to clear (e.g. apra). Omit to clear all.")
@click.option("--yes", is_flag=True,
              help="Skip confirmation when clearing all sources.")
@click.option(
    "--cache-base", type=click.Path(), default="data/raw", show_default=True,
)
def cache_clear_cmd(source: Optional[str], yes: bool, cache_base: str) -> None:
    from ingestion.cache_manager import CacheManager

    if source is None and not yes:
        if not click.confirm(
            "Clear ALL cached files across all sources?", abort=False, default=False,
        ):
            click.echo("Aborted.")
            return
    mgr = CacheManager(cache_base=cache_base)
    count = mgr.clear_cache(source=source)
    target = f"source={source!r}" if source else "all sources"
    click.echo(f"Cleared {count} cached file(s) from {target}.")


if __name__ == "__main__":  # pragma: no cover
    cli(obj={})
