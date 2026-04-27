"""Report-generation CLI.

One-stop entrypoint for producing the benchmark engine's committee
reports. Each report gets its own subcommand; each subcommand accepts
`--format {docx,html,markdown,all}` so the same invocation style works
across the portfolio.

Usage examples
--------------
    # Generate Report 2 (Environment) in all four file variants
    python scripts/generate_reports.py environment --format all

    # Just the DOCX variant, with a custom output directory
    python scripts/generate_reports.py environment --format docx \
        --output outputs/reports/Env_Q1_2026.docx

    # Override the industry-analysis data-dir (defaults to sibling repo path)
    python scripts/generate_reports.py environment --format all \
        --data-dir /path/to/industry-analysis/data/exports

    # List loaded frames + freshness before rendering
    python scripts/generate_reports.py environment --verify

Default data-dir resolution (environment command):
    1. `--data-dir` flag, if provided.
    2. `EXTERNAL_BENCHMARK_INDUSTRY_ANALYSIS_DIR` environment variable.
    3. `<repo-root>/../../../credit-risk-portfolio_bank/credit risk models
        commercial/industry-analysis/data/exports` (the known-sibling path
        for this portfolio; documented in README).

Design
------
This script is intentionally thin — report composition lives in
`reports/environment_report.py`; rendering lives in
`reports/render_environment_{docx,html,md}.py`. The CLI only maps flags
to file paths and calls the renderer.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional

import click


# Make the project root importable when run as a script.
_THIS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _THIS_DIR.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


DEFAULT_OUTPUT_DIR = Path("outputs/reports")
SIBLING_INDUSTRY_ANALYSIS_CANDIDATES: tuple[Path, ...] = (
    # Walk parents looking for `credit-risk-portfolio_bank/.../industry-analysis/data/exports`.
    # Works whether this repo sits under `portfolio learning/PD Framework/External Benchmark/`
    # or any shallower layout, up to 6 levels above the engine root.
    Path("../../../credit-risk-portfolio_bank/credit risk models commercial/industry-analysis/data/exports"),
    Path("../../../../credit-risk-portfolio_bank/credit risk models commercial/industry-analysis/data/exports"),
    Path("../../../../../credit-risk-portfolio_bank/credit risk models commercial/industry-analysis/data/exports"),
    Path("../../../../../../credit-risk-portfolio_bank/credit risk models commercial/industry-analysis/data/exports"),
)


def _resolve_default_data_dir() -> Optional[Path]:
    """Walk the default-data-dir resolution order; return first hit."""
    env_val = os.environ.get("EXTERNAL_BENCHMARK_INDUSTRY_ANALYSIS_DIR")
    if env_val:
        p = Path(env_val)
        if p.exists():
            return p
    for rel in SIBLING_INDUSTRY_ANALYSIS_CANDIDATES:
        candidate = (_REPO_ROOT / rel).resolve()
        if candidate.exists():
            return candidate
    return None


@click.group(help="Generate committee reports from the benchmark engine.")
def cli() -> None:
    pass


@cli.command("environment",
             help="Report 2 — Environment & Industry Overlay. Sources data "
                  "from the industry-analysis sibling project's parquet "
                  "exports.")
@click.option("--format", "fmt",
              type=click.Choice(["docx", "html", "markdown", "all"]),
              default="all", show_default=True,
              help="Output format. 'all' emits docx + html + markdown "
                   "(board + technical).")
@click.option("--data-dir", type=click.Path(), default=None,
              help="Path to industry-analysis/data/exports/. Defaults to "
                   "$EXTERNAL_BENCHMARK_INDUSTRY_ANALYSIS_DIR, then the "
                   "known-sibling-repo path.")
@click.option("--output", type=click.Path(), default=None,
              help="Output path stem. Defaults to "
                   "outputs/reports/Report_Environment_<period>.<ext>. "
                   "When --format=all, ignored in favour of the stem.")
@click.option("--period-label", default=None,
              help="Override period label (e.g. 'Q1 2026'). Derived from "
                   "macro_regime_flags.as_of_date when omitted.")
@click.option("--stale-days", type=int, default=90, show_default=True,
              help="Freshness threshold; parquet files older than this "
                   "are flagged in the report subtitle.")
@click.option("--verify", is_flag=True, default=False,
              help="Print loaded-frame summary and freshness findings, "
                   "then exit without rendering.")
def environment(
    fmt: str,
    data_dir: Optional[str],
    output: Optional[str],
    period_label: Optional[str],
    stale_days: int,
    verify: bool,
) -> None:
    from ingestion.industry_context import (
        MissingExportError,
        summarise_exports,
    )
    from reports.environment_report import EnvironmentReport

    resolved = Path(data_dir) if data_dir else _resolve_default_data_dir()
    if resolved is None:
        raise click.ClickException(
            "Could not locate industry-analysis exports directory. "
            "Pass --data-dir, set EXTERNAL_BENCHMARK_INDUSTRY_ANALYSIS_DIR, "
            "or place the sibling repo at the expected default path."
        )
    if not resolved.exists():
        raise click.ClickException(f"Data directory does not exist: {resolved}")

    try:
        report = EnvironmentReport.from_data_dir(
            resolved, period_label=period_label, stale_days=stale_days,
        )
    except MissingExportError as e:
        raise click.ClickException(str(e))

    if verify:
        summary = summarise_exports(report._frames)
        click.echo(f"Data directory: {resolved}")
        click.echo(f"Frames loaded  : {len(summary)}")
        for name, s in summary.items():
            click.echo(f"  {name:30s} rows={s['rows']:4d} cols={s['cols']:2d}")
        click.echo("")
        if report._freshness:
            click.echo("Freshness:")
            for f in report._freshness:
                tag = "STALE" if f.is_stale else "fresh"
                click.echo(f"  [{tag:5s}] {f.name:30s} age={f.age_days:7.1f} d")
        return

    period_slug = report._period.replace(" ", "_")
    out_dir = Path(output).parent if output else DEFAULT_OUTPUT_DIR
    stem_name = (
        Path(output).with_suffix("").name if output
        else f"Report_Environment_{period_slug}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = out_dir / stem_name

    to_emit = (["docx", "html", "markdown"] if fmt == "all" else [fmt])
    written: list[Path] = []

    if "docx" in to_emit:
        p = report.to_docx(stem.with_suffix(".docx"))
        written.append(p)
    if "html" in to_emit:
        p = report.to_html(stem.with_suffix(".html"))
        written.append(p)
    if "markdown" in to_emit:
        written.append(report.to_board_markdown(stem.parent / f"{stem.name}_Board.md"))
        written.append(report.to_markdown(stem.parent / f"{stem.name}_Technical.md"))

    for p in written:
        size = p.stat().st_size
        click.echo(f"Written: {p}  ({size:,} bytes)")


if __name__ == "__main__":
    cli()
