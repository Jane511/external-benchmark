"""Macquarie Bank Limited Pillar 3 PDF adapter.

This adapter targets the regulated ADI disclosure for Macquarie Bank
Limited (MBL), not Macquarie Group Limited. MBL is the APRA-authorised
banking entity, so these rows are comparable to the other bank Pillar 3
sources consumed by the benchmark registry.

Macquarie's CR6 table follows APS 330 but differs from the CBA/NAB/WBC/ANZ
text extraction shape:

* percentage cells are often emitted as ``"0.1 %"`` split across two tokens;
* the default band is emitted as ``"100.00 (Non-"`` then ``"Performing)"``;
* a September half-year PDF also includes the preceding March table, so the
  adapter only parses pages/sections matching the requested reporting date.
"""

from __future__ import annotations

import logging
import re
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ingestion.adapters.cba_pillar3_pdf_adapter import (
    CbaPillar3PdfAdapter,
    _match_portfolio,
    _parse_numeric,
)

logger = logging.getLogger(__name__)


_MQG_PD_RANGE_RE = re.compile(
    r"(?P<band>(?:\d+\.\d+ to <\d+\.\d+)|(?:100\.00 \(Non-Performing\)))"
    r"\s+(?P<rest>.*)$"
)

_MQG_DATE_RE = re.compile(
    r"\b(?P<day>30|31)\s+(?P<month>March|September)\s+(?P<year>\d{4})\b",
    re.IGNORECASE,
)


class MqgPillar3PdfAdapter(CbaPillar3PdfAdapter):
    """Extract CR6 PD/LGD and CR10 risk weights from MBL Pillar 3 PDFs."""

    _SOURCE_NAME = "mqg_pillar3"

    # Macquarie Bank's financial year ends 31 March.
    FISCAL_YEAR_END_MONTH = 3

    CR6_HEADER_RE = re.compile(
        r"Table\s+7\s*:\s*CR6\s*-\s*IRB", re.MULTILINE | re.IGNORECASE
    )
    CR10_HEADER_TOKENS = ("Table 9: CR10",)

    PORTFOLIO_PATTERNS = (
        ("specialised lending - ipre", "commercial_property_investment"),
        ("specialised lending- ipre", "commercial_property_investment"),
        ("residential mortgage",      "residential_mortgage"),
        ("sme corporate",             "corporate_sme"),
        ("sme retail",                "retail_sme"),
        ("other retail",              "retail_other"),
        ("financial institution",     "financial_institution"),
        ("corporate",                 "corporate_general"),
        ("sovereign",                 "sovereign"),
    )

    def normalise(
        self,
        file_path: Path,
        *,
        reporting_date: date | str | None = None,
    ) -> pd.DataFrame:
        try:
            import pdfplumber
        except ImportError as exc:  # pragma: no cover
            raise ImportError(
                "pdfplumber is required for MqgPillar3PdfAdapter; install the "
                "'ingestion' extras"
            ) from exc

        reporting = _coerce_reporting_date(reporting_date)
        period_code = _derive_mqg_period_code(reporting)
        target_label = _format_mqg_report_date(reporting)

        records: list[dict[str, Any]] = []
        in_target_cr6 = False

        with pdfplumber.open(file_path) as pdf:
            for page_index, page in enumerate(pdf.pages):
                text = page.extract_text() or ""
                page_num = page_index + 1

                dates_on_page = _report_dates_in_text(text)
                if self.CR6_HEADER_RE.search(text):
                    in_target_cr6 = target_label in dates_on_page
                elif dates_on_page and target_label not in dates_on_page:
                    in_target_cr6 = False

                if in_target_cr6 and _looks_like_mqg_cr6_page(text):
                    records.extend(self._extract_cr6_page(
                        text, page_num, reporting, period_code,
                    ))

                if "Table 9: CR10" in text and target_label in dates_on_page:
                    section = _section_for_report_date(text, target_label)
                    records.extend(self._extract_cr10_page(
                        section, page_num, reporting, period_code,
                    ))

        df = (
            pd.DataFrame.from_records(records)
            if records else pd.DataFrame(columns=self._CANONICAL_COLUMNS)
        )
        self.validate_output(df)
        return df

    def _extract_cr6_page(
        self,
        text: str,
        page_num: int,
        reporting: date,
        period_code: str,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        current_portfolio: str | None = None
        lines = _join_non_performing_band(text.split("\n"))

        for line in lines:
            line = line.strip()
            if not line:
                continue

            m = _MQG_PD_RANGE_RE.search(line)
            if m is None:
                maybe = _match_portfolio(line, self.PORTFOLIO_PATTERNS)
                if maybe:
                    current_portfolio = maybe
                continue

            if current_portfolio is None:
                continue

            band = m.group("band")
            rest_tokens = _combine_percent_tokens(m.group("rest").split())
            pd_pct, lgd_pct = _pick_mqg_cr6_pd_lgd(rest_tokens)

            for metric_name, raw_pct in (("pd", pd_pct), ("lgd", lgd_pct)):
                if raw_pct is None:
                    continue
                value = raw_pct / 100.0
                if not self._is_plausible(metric_name, value):
                    logger.warning(
                        "MQG CR6: dropping implausible %s=%.6f for "
                        "portfolio=%s band=%s (page %d)",
                        metric_name, value, current_portfolio, band, page_num,
                    )
                    continue
                out.append({
                    "asset_class": current_portfolio,
                    "metric_name": metric_name,
                    "value": value,
                    "as_of_date": reporting,
                    "period_code": period_code,
                    "value_basis": self.VALUE_BASIS_CR6,
                    "source_table": "CR6",
                    "source_page": page_num,
                    "pd_band": band,
                })

        return out


def _coerce_reporting_date(value: date | str | None) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return pd.to_datetime(value).date()
    today = date.today()
    year = today.year if today.month >= 10 else today.year - 1
    return date(year, 9, 30)


def _derive_mqg_period_code(d: date) -> str:
    """Macquarie Bank fiscal year ends 31 March."""
    if d.month == 3:
        return f"FY{d.year}"
    if d.month == 9:
        return f"H1FY{d.year + 1}"
    fy = d.year if d.month <= 3 else d.year + 1
    quarter = ((d.month - 4) % 12) // 3 + 1
    return f"Q{quarter}FY{fy}"


def _format_mqg_report_date(d: date) -> str:
    month = "March" if d.month == 3 else "September" if d.month == 9 else d.strftime("%B")
    return f"{d.day} {month} {d.year}"


def _report_dates_in_text(text: str) -> set[str]:
    return {
        f"{m.group('day')} {m.group('month').title()} {m.group('year')}"
        for m in _MQG_DATE_RE.finditer(text)
    }


def _section_for_report_date(text: str, target_label: str) -> str:
    start = text.find(target_label)
    if start < 0:
        return text
    next_date = _MQG_DATE_RE.search(text, start + len(target_label))
    end = next_date.start() if next_date else len(text)
    return text[start:end]


def _looks_like_mqg_cr6_page(text: str) -> bool:
    return "PD Scale" in text or bool(_MQG_PD_RANGE_RE.search(
        "\n".join(_join_non_performing_band(text.split("\n")))
    ))


def _join_non_performing_band(lines: list[str]) -> list[str]:
    out: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line == "100.00 (Non-" and i + 1 < len(lines):
            nxt = lines[i + 1].strip()
            if nxt.startswith("Performing)"):
                out.append(f"100.00 (Non-Performing) {nxt[len('Performing)'):].strip()}")
                i += 2
                continue
        out.append(line)
        i += 1
    return out


def _combine_percent_tokens(tokens: list[str]) -> list[str]:
    out: list[str] = []
    i = 0
    while i < len(tokens):
        if i + 1 < len(tokens) and tokens[i + 1] == "%":
            out.append(f"{tokens[i]}%")
            i += 2
            continue
        out.append(tokens[i])
        i += 1
    return out


def _pick_mqg_cr6_pd_lgd(tokens: list[str]) -> tuple[float | None, float | None]:
    """Return (PD %, LGD %) from Macquarie's CR6 token stream.

    After the PD band, APS 330 columns are:
    original exposure, off-balance exposure, CCF, EAD, average PD,
    number of obligors, average LGD, then maturity/RWA columns.
    """
    def pct_at(index: int) -> float | None:
        if index >= len(tokens):
            return None
        token = tokens[index].rstrip("%")
        return _parse_numeric(token)

    return pct_at(4), pct_at(6)

