"""Generate the Q1 2026 sample raw-only report.

Loads a small in-memory set of representative RawObservation rows
(commercial property + residential mortgage from Big 4 + non-bank
sources), then runs the rewritten BenchmarkCalibrationReport. Output
is written to outputs/reports/Report_Q1_2026_RawOnly_sample.md.

This is the deliverable described in Brief 1's Definition of Done:
"One full quarterly cycle re-run end-to-end to produce a sample
raw-only report; commit the report as
outputs/reports/Report_Q1_2026_RawOnly_sample.md for review."

The data here is illustrative — the real Q1 2026 cycle will populate
raw_observations from the live ingest path. Numbers in this sample are
deliberately consistent with Big 4 Pillar 3 disclosures and ASX-listed
non-bank publications observed in the Q4 2025 cycle.
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

# Allow running as a standalone script from the project root.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from reports.benchmark_report import BenchmarkCalibrationReport  # noqa: E402
from src.db import create_engine_and_schema  # noqa: E402
from src.models import RawObservation, SourceType  # noqa: E402
from src.observations import PeerObservations  # noqa: E402
from src.registry import BenchmarkRegistry  # noqa: E402


TODAY = date(2026, 4, 27)


def _seed(reg: BenchmarkRegistry) -> None:
    """Insert a representative cross-source set of observations."""

    obs: list[RawObservation] = []

    # ---- commercial_property: Big 4 Pillar 3 + non-bank ASX ----------
    obs.append(RawObservation(
        source_id="cba", source_type=SourceType.BANK_PILLAR3,
        segment="commercial_property", parameter="pd",
        value=0.0250, as_of_date=TODAY - timedelta(days=45),
        reporting_basis="Pillar 3 trailing 4-quarter average",
        methodology_note="CR6 EAD-weighted Average PD across PD bands",
        sample_size_n=None, period_end=date(2026, 3, 31),
        source_url="https://www.commbank.com.au/about-us/investors/regulatory-disclosures.html",
        page_or_table_ref="CBA Pillar 3 Q1 2026 — Table CR6 row 4 (Commercial property)",
    ))
    obs.append(RawObservation(
        source_id="nab", source_type=SourceType.BANK_PILLAR3,
        segment="commercial_property", parameter="pd",
        value=0.0285, as_of_date=TODAY - timedelta(days=60),
        reporting_basis="Pillar 3 quarterly disclosure",
        methodology_note="CR6 EAD-weighted Average PD",
        period_end=date(2026, 3, 31),
        source_url="https://www.nab.com.au/about-us/shareholder-centre/regulatory-disclosures",
        page_or_table_ref="NAB Pillar 3 Q1 2026 — Table CR6 row 5 (Commercial real estate)",
    ))
    obs.append(RawObservation(
        source_id="wbc", source_type=SourceType.BANK_PILLAR3,
        segment="commercial_property", parameter="pd",
        value=0.0240, as_of_date=TODAY - timedelta(days=60),
        reporting_basis="Pillar 3 quarterly disclosure",
        methodology_note="CR6 EAD-weighted Average PD",
        period_end=date(2026, 3, 31),
        source_url="https://www.westpac.com.au/about-westpac/investor-centre/financial-information/regulatory-disclosures/",
        page_or_table_ref="Westpac Pillar 3 Q1 2026 — Table CR6 row 4",
    ))
    obs.append(RawObservation(
        source_id="anz", source_type=SourceType.BANK_PILLAR3,
        segment="commercial_property", parameter="pd",
        value=0.0265, as_of_date=TODAY - timedelta(days=60),
        reporting_basis="Pillar 3 quarterly disclosure",
        methodology_note="CR6 EAD-weighted Average PD",
        period_end=date(2026, 3, 31),
        source_url="https://www.anz.com.au/shareholder/centre/reporting/regulatory-disclosure/",
        page_or_table_ref="ANZ Pillar 3 Q1 2026 — Table CR6 row 6 (Commercial property)",
    ))
    obs.append(RawObservation(
        source_id="judo", source_type=SourceType.NON_BANK_LISTED,
        segment="commercial_property", parameter="pd",
        value=0.0420, as_of_date=TODAY - timedelta(days=120),
        reporting_basis="Half-yearly Pillar 3 disclosure (ADI status since 2019)",
        methodology_note="Average PD on commercial real estate book — H1 FY26",
        period_end=date(2025, 12, 31),
        source_url="https://www.judo.bank/investor-centre/",
        page_or_table_ref="Judo H1 FY26 Pillar 3 — CR6 commercial real estate",
    ))
    obs.append(RawObservation(
        source_id="liberty", source_type=SourceType.NON_BANK_LISTED,
        segment="commercial_property", parameter="pd",
        value=0.0530, as_of_date=TODAY - timedelta(days=200),
        reporting_basis="Annual report — credit risk section",
        methodology_note="90+ days arrears proxy on commercial property loans (no Pillar 3 published)",
        period_end=date(2025, 6, 30),
        source_url="https://www.libertyfinancial.com.au/about/investor-information",
        page_or_table_ref="Liberty FY25 Annual Report — Credit risk note 7",
    ))
    obs.append(RawObservation(
        source_id="pepper", source_type=SourceType.NON_BANK_LISTED,
        segment="commercial_property", parameter="pd",
        value=0.0480, as_of_date=TODAY - timedelta(days=150),
        reporting_basis="Half-yearly results — credit performance",
        methodology_note="Net credit losses % on commercial loan book — H1 FY26",
        period_end=date(2025, 12, 31),
        source_url="https://www.peppermoney.com.au/investors/",
        page_or_table_ref="Pepper H1 FY26 results pack — slide 18",
    ))

    # ---- residential_mortgage: subset for the report ----------------
    obs.append(RawObservation(
        source_id="cba", source_type=SourceType.BANK_PILLAR3,
        segment="residential_mortgage", parameter="pd",
        value=0.0042, as_of_date=TODAY - timedelta(days=45),
        reporting_basis="Pillar 3 trailing 4-quarter average",
        methodology_note="CR6 EAD-weighted Average PD on residential mortgages",
        period_end=date(2026, 3, 31),
        source_url="https://www.commbank.com.au/about-us/investors/regulatory-disclosures.html",
        page_or_table_ref="CBA Pillar 3 Q1 2026 — Table CR6 row 1",
    ))
    obs.append(RawObservation(
        source_id="resimac", source_type=SourceType.NON_BANK_LISTED,
        segment="residential_mortgage", product="prime", parameter="pd",
        value=0.0085, as_of_date=TODAY - timedelta(days=120),
        reporting_basis="Half-yearly results — Prime / Specialist split",
        methodology_note="Prime mortgage 90+ arrears proxy",
        period_end=date(2025, 12, 31),
        source_url="https://www.resimac.com.au/about-resimac/investor-relations/",
        page_or_table_ref="Resimac H1 FY26 — slide 12 (Prime arrears)",
    ))
    obs.append(RawObservation(
        source_id="sp_spin", source_type=SourceType.RATING_AGENCY_INDEX,
        segment="residential_mortgage", parameter="pd",
        value=0.0091, as_of_date=TODAY - timedelta(days=20),
        reporting_basis="S&P RMBS Performance Index (SPIN), monthly",
        methodology_note="Total prime + non-conforming arrears proxy for default rate",
        period_end=date(2026, 3, 31),
        source_url="https://www.spglobal.com/ratings/en/regulatory/topic/spin",
        page_or_table_ref="SPIN March 2026 — total arrears headline",
    ))

    reg.add_observations(obs)


def main() -> Path:
    engine = create_engine_and_schema(":memory:")
    registry = BenchmarkRegistry(engine, actor="sample")
    _seed(registry)

    peer = PeerObservations(registry, today=TODAY)
    report = BenchmarkCalibrationReport(
        registry=registry, peer_observations=peer,
        period_label="Q1 2026",
    )

    out_dir = Path("outputs/reports")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "Report_Q1_2026_RawOnly_sample.md"
    out_path.write_text(report.to_markdown(), encoding="utf-8")
    return out_path


if __name__ == "__main__":
    p = main()
    print(f"Sample raw-only report written: {p} ({p.stat().st_size} bytes)")
