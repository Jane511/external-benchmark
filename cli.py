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

from src.adjustments import AdjustmentEngine
from src.calibration_feed import CalibrationFeed
from src.db import create_engine_and_schema
from src.governance import GovernanceReporter
from src.models import InstitutionType
from src.registry import BenchmarkRegistry
from src.seed_data import load_seed_data
from src.triangulation import BenchmarkTriangulator

# Ingestion layer (lazy-imported by subcommands so missing extras don't break other commands).


DEFAULT_DB = "./benchmarks.db"


def _get_registry(db_path: str) -> BenchmarkRegistry:
    engine = create_engine_and_schema(db_path)
    return BenchmarkRegistry(engine, actor="cli")


def _get_all(db_path: str, institution: str):
    """Build registry + adjustment_engine + triangulator + calibration_feed."""
    engine = create_engine_and_schema(db_path)
    inst = (
        InstitutionType.BANK if institution == "bank"
        else InstitutionType.PRIVATE_CREDIT
    )
    registry = BenchmarkRegistry(engine, actor="cli")
    adjuster = AdjustmentEngine(inst, engine, actor="cli")
    triangulator = BenchmarkTriangulator(inst)
    feed = CalibrationFeed(registry, adjuster, triangulator)
    return registry, feed, inst


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
)
@click.pass_context
def cli(ctx: click.Context, db: str, institution: str) -> None:
    ctx.ensure_object(dict)
    ctx.obj["db"] = db
    ctx.obj["institution"] = institution


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
    registry, _feed, inst = _get_all(ctx.obj["db"], ctx.obj["institution"])
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
                help="Report 1 — External Benchmark Calibration Summary.")
@click.option("--format", "fmt",
              type=click.Choice(["docx", "html", "markdown"]),
              required=True)
@click.option("--institution-type",
              type=click.Choice(["bank", "private_credit"]),
              default="bank", show_default=True)
@click.option("--output", type=click.Path(), default=None,
              help="Output path. Defaults to "
                   "outputs/reports/benchmark_{institution}_{period}.{ext}.")
@click.option("--period-label", default=None,
              help="Period label (e.g. 'Q3 2025'). Derived from today if omitted.")
@click.option("--compare-to", type=click.Path(exists=True), default=None,
              help="Prior-period SQLite snapshot for delta reporting. "
                   "(CLI-accepted; section 9 gracefully degrades if omitted.)")
@click.pass_context
def report_benchmark(
    ctx: click.Context,
    fmt: str,
    institution_type: str,
    output: Optional[str],
    period_label: Optional[str],
    compare_to: Optional[str],
) -> None:
    from reports.benchmark_report import BenchmarkCalibrationReport

    # Build fresh engine components bound to the CLI's DB.
    engine = create_engine_and_schema(ctx.obj["db"])
    inst = (InstitutionType.BANK if institution_type == "bank"
            else InstitutionType.PRIVATE_CREDIT)
    registry = BenchmarkRegistry(engine, actor="cli")
    adjuster = AdjustmentEngine(inst, engine, actor="cli")
    triangulator = BenchmarkTriangulator(inst)
    feed = CalibrationFeed(registry, adjuster, triangulator)
    from src.downturn import DownturnCalibrator
    downturn = DownturnCalibrator(registry)
    gov = GovernanceReporter(registry, inst)

    prior_registry = None
    if compare_to:
        prior_engine = create_engine_and_schema(str(compare_to))
        prior_registry = BenchmarkRegistry(prior_engine, actor="cli-prior")

    report_obj = BenchmarkCalibrationReport(
        registry=registry, adjustment_engine=adjuster,
        triangulator=triangulator, calibration_feed=feed,
        downturn_calibrator=downturn, governance_reporter=gov,
        institution_type=institution_type,
        period_label=period_label,
        prior_registry=prior_registry,
    )

    # Resolve the output path.
    ext = {"docx": "docx", "html": "html", "markdown": "md"}[fmt]
    period_slug = (period_label or report_obj._period).replace(" ", "_")
    default_path = (
        Path("outputs/reports")
        / f"benchmark_{institution_type}_{period_slug}.{ext}"
    )
    out_path = Path(output) if output else default_path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if fmt == "docx":
        report_obj.to_docx(out_path)
        click.echo(f"Report written: {out_path} ({out_path.stat().st_size} bytes)")
    elif fmt == "html":
        report_obj.to_html(out_path)
        click.echo(f"Report written: {out_path} ({out_path.stat().st_size} bytes)")
    elif fmt == "markdown":
        # Markdown emits two variants: a concise Board summary and a
        # full Technical Appendix. If the user supplied --output, we
        # use it as the stem and derive the two siblings.
        stem = out_path.with_suffix("")
        board_path = stem.parent / f"{stem.name}_Board.md"
        tech_path = stem.parent / f"{stem.name}_Technical.md"
        report_obj.to_board_markdown(board_path)
        report_obj.to_markdown(tech_path)
        click.echo(f"Board report:     {board_path} ({board_path.stat().st_size} bytes)")
        click.echo(f"Technical report: {tech_path} ({tech_path.stat().st_size} bytes)")


@report.command("environment",
                help="Report 2 — industry / property environment (requires "
                     "industry-analysis sibling project; not yet implemented).")
@click.pass_context
def report_environment(ctx: click.Context) -> None:
    click.echo(
        "Report 2 (Environment) is not yet implemented. It requires the "
        "industry-analysis sibling project's `data/exports/*.parquet` "
        "contracts, which haven't been synced. Build it when that project "
        "is available.",
        err=True,
    )
    ctx.exit(2)


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

@cli.command(help="Compute a calibration-feed output for a PD segment.")
@click.argument("method", type=click.Choice([
    "central_tendency", "logistic_recalibration",
    "bayesian_blending", "external_blending", "pluto_tasche",
]))
@click.option("--segment", required=True, help="Asset class / segment name.")
@click.option("--internal-years", type=float, default=5.0,
              help="Internal data length (external_blending only).")
@click.pass_context
def feed(
    ctx: click.Context, method: str, segment: str, internal_years: float,
) -> None:
    _registry, calib, _inst = _get_all(ctx.obj["db"], ctx.obj["institution"])
    try:
        if method == "central_tendency":
            out = calib.for_central_tendency(segment)
        elif method == "logistic_recalibration":
            out = calib.for_logistic_recalibration(segment)
        elif method == "bayesian_blending":
            out = calib.for_bayesian_blending(segment)
        elif method == "external_blending":
            out = calib.for_external_blending(segment, internal_years=internal_years)
        else:  # pluto_tasche
            out = calib.for_pluto_tasche(segment)
    except ValueError as e:
        raise click.ClickException(str(e))

    click.echo(json.dumps(out.model_dump(), indent=2, default=str))


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
