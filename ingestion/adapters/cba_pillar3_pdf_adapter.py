"""CBA annual / half-year Pillar 3 PDF adapter.

Extracts two kinds of disclosure from CBA's annual / half-year Pillar 3
PDF (e.g. `CBA_FY2025_Pillar3_Annual.pdf`):

- **CR6** (pages 54-58 in the FY2025 release) — IRB credit-risk exposures
  by portfolio × PD band. Each data row carries an Average PD (%) and an
  Average LGD (%) that the engine consumes directly as PD and LGD
  benchmarks.
- **CR10** (page 48 in the FY2025 release) — specialised-lending
  slotting. **Publishes APRA-prescribed risk weights, not bank-estimated
  PDs.** The adapter therefore emits `risk_weight` entries (Strong 70%,
  Good 90%, Satisfactory 115%, Weak 250%, Default 0%) and never
  fabricates a PD for slotting rows.

Why text-line parsing rather than ``page.extract_tables()``:
`extract_tables` reliably loses the left-most category/portfolio label
on both CR6 and CR10 (it treats them as section headers sitting outside
the table grid). Line-by-line parsing driven by a **PD-range regex** for
CR6 and a **known-category prefix** for CR10 is more robust and makes
the section-header carry-forward explicit.

See ``outputs/cba_pillar3_pdf_structure.md`` for the column map, page
locations, and expected entry counts.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from ingestion.adapters.base import AbstractAdapter

logger = logging.getLogger(__name__)


# Matches "0.00 to <0.15", "10.00 to <100.00", "100.00 (Default)".
# Anchored with a word boundary rather than start-of-string so a portfolio
# prefix on the same line ("Residential mortgage 0.00 to <0.15 …") is
# allowed and separable via ``line[: m.start()]``.
_PD_RANGE_RE = re.compile(
    r"(?P<band>(?:\d+\.\d+ to <\d+\.\d+)|(?:100\.00 \(Default\)))"
    r"\s+(?P<rest>.*)$"
)


# Portfolio labels as they appear at the start of a CR6 block. Listed
# longest-first so "RBNZ regulated entities Non-retail" matches before
# "RBNZ regulated entities".
_CR6_PORTFOLIO_PATTERNS: tuple[tuple[str, str], ...] = (
    ("rbnz regulated entities\nnon-retail", "rbnz_non_retail"),
    ("rbnz regulated entities\nretail",     "rbnz_retail"),
    ("rbnz regulated entities non-retail",  "rbnz_non_retail"),
    ("rbnz regulated entities retail",      "rbnz_retail"),
    ("qualifying revolving retail",         "retail_qrr"),
    ("residential mortgage",                "residential_mortgage"),
    ("sme retail",                          "retail_sme"),
    ("other retail",                        "retail_other"),
    ("corporate - large",                   "corporate_general"),
    ("corporate (incl. sme corporate)",     "corporate_sme"),
    ("corporate",                           "corporate_general"),
    ("sovereign",                           "sovereign"),
    ("financial institution",               "financial_institution"),
)


@dataclass(frozen=True)
class _SlottingGrade:
    label: str              # as it appears in the PDF text
    asset_class: str        # canonical engine label
    risk_weight: float      # APS 113 Attachment B prescribed value (decimal)


_CR10_GRADES: tuple[_SlottingGrade, ...] = (
    _SlottingGrade("Strong",       "development_strong",       0.70),
    _SlottingGrade("Good",         "development_good",         0.90),
    _SlottingGrade("Satisfactory", "development_satisfactory", 1.15),
    _SlottingGrade("Weak",         "development_weak",         2.50),
    _SlottingGrade("Default",      "development_default",      0.00),
)


# Null markers seen in CBA PDFs for blank / NA cells.
_NA_TOKENS = {"-", "–", "—", "n/a", "na", ""}


class CbaPillar3PdfAdapter(AbstractAdapter):
    """Extract CR6 PD/LGD and CR10 risk weights from a CBA Pillar 3 PDF.

    Subclass extension points (per-bank overrides):

    - ``PORTFOLIO_PATTERNS`` — tuple of ``(lowercase_needle, asset_class)``.
      Default covers CBA's portfolio labels. Other Big-4 banks add their
      own variants (``"retail sme"``, ``"rbnz regulated entities -
      retail"``, etc.).
    - ``CR10_GRADES`` — slotting categories + APS 113 risk weights. Same
      across Big-4 by regulation.
    - ``ROW_INDEX_PREFIX_RE`` — optional regex applied to the
      pre-PD-range prefix. ANZ prefixes every CR6 data row with a 1-
      or 2-digit row index that must be stripped before portfolio
      matching.
    - ``FISCAL_YEAR_END_MONTH`` — 6 for CBA (June), 9 for NAB/WBC/ANZ.
      Drives the ``FY`` label in ``period_code``.
    """

    _SOURCE_NAME = "cba_pillar3_annual"
    _CANONICAL_COLUMNS = [
        "asset_class",
        "metric_name",
        "value",
        "as_of_date",
        "period_code",
        "value_basis",
        "source_table",
        "source_page",
        "pd_band",
    ]

    # ---- tunables ---------------------------------------------------------

    # Match ONLY a genuine CR6 table header line (e.g. "CR6: IRB - credit risk
    # exposures by portfolio and probability of default range"), not incidental
    # mentions of "CR6" in surrounding prose (e.g. CR9's cross-reference text
    # "...outlined in CR6: IRB...", which previously caused CR9 rows to be
    # extracted as if they were CR6 rows — producing bogus sub-5% LGD values).
    CR6_HEADER_RE = re.compile(r"^\s*CR6\s*:\s*IRB", re.MULTILINE | re.IGNORECASE)

    # Hard-stop patterns. If any of these appear on the same page as a CR6
    # header, we are on a page where CR6 ends and a different table begins
    # (typically CR7/CR8/CR9). The adapter extracts CR6 rows up to the first
    # match and ignores everything after.
    CR6_BOUNDARY_RE = re.compile(
        r"^\s*CR(?:7|8|9|10)\s*:", re.MULTILINE | re.IGNORECASE
    )
    CR10_HEADER_TOKENS: tuple[str, ...] = ("CR10",)

    # Portfolio labels + slotting grades — exposed as class attributes so
    # subclasses can override with bank-specific variants.
    PORTFOLIO_PATTERNS: tuple[tuple[str, str], ...] = _CR6_PORTFOLIO_PATTERNS
    CR10_GRADES: tuple[_SlottingGrade, ...] = _CR10_GRADES

    # Optional regex stripped from the pre-PD-range prefix before portfolio
    # matching. ANZ subclass overrides to ``r"^\d+\s+"`` to strip the row
    # index; None disables stripping (CBA / NAB / WBC need no strip).
    ROW_INDEX_PREFIX_RE: re.Pattern | None = None

    # Month of fiscal year end. CBA = June; the other Big 4 = September.
    FISCAL_YEAR_END_MONTH: int = 6

    PLAUSIBILITY: dict[str, tuple[float, float]] = {
        "pd":          (0.0, 1.01),
        "lgd":         (0.0, 1.0),
        "risk_weight": (0.0, 3.0),
    }

    VALUE_BASIS_CR6 = "exposure_weighted"          # CBA CR6 is EAD-weighted
    VALUE_BASIS_CR10 = "supervisory_prescribed"    # APS 113 Attachment B

    # ---- AbstractAdapter contract ------------------------------------------

    @property
    def source_name(self) -> str:
        return self._SOURCE_NAME

    @property
    def canonical_columns(self) -> list[str]:
        return list(self._CANONICAL_COLUMNS)

    def normalise(
        self,
        file_path: Path,
        *,
        reporting_date: date | str | None = None,
    ) -> pd.DataFrame:
        try:
            import pdfplumber  # lazy import so fixture tests don't need it
        except ImportError as exc:  # pragma: no cover
            raise ImportError(
                "pdfplumber is required for CbaPillar3PdfAdapter; install the "
                "'ingestion' extras"
            ) from exc

        reporting = _coerce_reporting_date(reporting_date)
        period_code = _derive_cba_period_code(reporting)

        records: list[dict[str, Any]] = []
        with pdfplumber.open(file_path) as pdf:
            for page_index, page in enumerate(pdf.pages):
                text = page.extract_text() or ""

                # Only accept pages with a genuine "CR6: IRB..." header line.
                # Incidental mentions of "CR6" in prose no longer trigger
                # extraction (prevents CR9 cross-reference contamination).
                if self.CR6_HEADER_RE.search(text):
                    # If a subsequent CR-table header appears on the same page,
                    # truncate the text at that boundary so we never parse rows
                    # belonging to CR7/CR8/CR9/CR10.
                    cr6_text = text
                    boundary_match = self.CR6_BOUNDARY_RE.search(cr6_text)
                    if boundary_match:
                        cr6_text = cr6_text[: boundary_match.start()]
                        logger.debug(
                            "CR6: truncating page %d at boundary marker %r",
                            page_index + 1,
                            boundary_match.group(0).strip(),
                        )
                    records.extend(self._extract_cr6_page(
                        cr6_text, page_index + 1, reporting, period_code,
                    ))
                if any(tok in text for tok in self.CR10_HEADER_TOKENS):
                    records.extend(self._extract_cr10_page(
                        text, page_index + 1, reporting, period_code,
                    ))

        if not records:
            df = pd.DataFrame(columns=self._CANONICAL_COLUMNS)
            self.validate_output(df)
            return df

        df = pd.DataFrame.from_records(records)
        self.validate_output(df)
        return df

    # ------------------------------------------------------------------
    # CR6 extraction
    # ------------------------------------------------------------------

    def _extract_cr6_page(
        self,
        text: str,
        page_num: int,
        reporting: date,
        period_code: str,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        current_portfolio: str | None = None

        lines = [ln.strip() for ln in text.split("\n")]

        # Helper — look up the next line's PD-range prefix, if any.
        def _next_prefix(i: int) -> str:
            for j in range(i + 1, min(i + 3, len(lines))):
                if not lines[j]:
                    continue
                m2 = _PD_RANGE_RE.search(lines[j])
                if m2 is None:
                    return lines[j]
                return lines[j][: m2.start()].strip()
            return ""

        previous_prefix: str = ""

        for i, line in enumerate(lines):
            if not line:
                previous_prefix = ""
                continue

            # A PD range may appear at the start of the line OR after a
            # portfolio prefix ("Residential mortgage 0.00 to <0.15 …").
            m = _PD_RANGE_RE.search(line)
            if m is None:
                # Header-only line. If it matches a known portfolio label
                # (pattern A: "RBNZ regulated entities\nNon-retail …") we
                # commit immediately. Otherwise keep the text as context
                # for the next line.
                maybe = _match_portfolio(line, self.PORTFOLIO_PATTERNS)
                if maybe:
                    current_portfolio = maybe
                previous_prefix = line
                continue

            prefix = line[: m.start()].strip()
            # Per-bank hook: strip a row-index prefix (e.g. ANZ's leading
            # "19 " before each CR6 data row).
            if self.ROW_INDEX_PREFIX_RE is not None and prefix:
                prefix = self.ROW_INDEX_PREFIX_RE.sub("", prefix).strip()

            if prefix:
                maybe: str | None = None

                # 1. Look-ahead (Pattern B) — if the NEXT row's prefix
                #    carries a parenthetical continuation
                #    ("(incl. SME corporate)"), prefer that more-specific
                #    combined label over anything else. Done first so the
                #    "corporate" catch-all can't win accidentally.
                next_pref = _next_prefix(i)
                if next_pref and next_pref.startswith("("):
                    maybe = _match_portfolio(
                        f"{prefix} {next_pref}", self.PORTFOLIO_PATTERNS,
                    )

                # 2. Combined "previous + current" (Pattern A) — RBNZ
                #    split header ("RBNZ regulated entities" + "Non-retail").
                if maybe is None and previous_prefix:
                    maybe = _match_portfolio(
                        f"{previous_prefix} {prefix}", self.PORTFOLIO_PATTERNS,
                    )

                # 3. Prefix alone — the common case.
                if maybe is None:
                    maybe = _match_portfolio(prefix, self.PORTFOLIO_PATTERNS)

                if maybe:
                    current_portfolio = maybe

            previous_prefix = prefix

            if current_portfolio is None:
                continue

            band = m.group("band")
            rest_tokens = m.group("rest").split()
            pd_pct, lgd_pct = _pick_cr6_pd_lgd(rest_tokens)

            for metric_name, raw_pct in (("pd", pd_pct), ("lgd", lgd_pct)):
                if raw_pct is None:
                    continue
                value = raw_pct / 100.0
                if not self._is_plausible(metric_name, value):
                    logger.warning(
                        "CR6: dropping implausible %s=%.6f for portfolio=%s band=%s (page %d)",
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

    # ------------------------------------------------------------------
    # CR10 extraction
    # ------------------------------------------------------------------

    def _extract_cr10_page(
        self,
        text: str,
        page_num: int,
        reporting: date,
        period_code: str,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for grade in self.CR10_GRADES:
            # The APRA-prescribed risk weight is deterministic; we only
            # emit a row if the PDF confirms the grade is present on this
            # page (i.e. CBA actually disclosed specialised-lending
            # exposures in that grade this period).
            if grade.label.lower() not in text.lower():
                continue
            value = grade.risk_weight
            if not self._is_plausible("risk_weight", value):
                continue
            out.append({
                "asset_class": grade.asset_class,
                "metric_name": "risk_weight",
                "value": value,
                "as_of_date": reporting,
                "period_code": period_code,
                "value_basis": self.VALUE_BASIS_CR10,
                "source_table": "CR10",
                "source_page": page_num,
                "pd_band": "all",
            })
        return out

    # ------------------------------------------------------------------

    @classmethod
    def _is_plausible(cls, metric_name: str, value: float) -> bool:
        lo, hi = cls.PLAUSIBILITY.get(metric_name, (0.0, 1.0))
        return lo <= value <= hi


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _match_portfolio(
    text: str,
    patterns: tuple[tuple[str, str], ...] = _CR6_PORTFOLIO_PATTERNS,
) -> str | None:
    """Return the asset_class code for the first matching pattern in ``text``.

    ``patterns`` defaults to the CBA list so module-level callers stay
    backwards-compatible; subclass adapters pass their own extended
    tuple via ``self.PORTFOLIO_PATTERNS``.
    """
    lower = text.strip().lower()
    for pattern, code in patterns:
        if pattern in lower:
            return code
    return None


def _parse_numeric(token: str) -> float | None:
    """Parse a CR6 numeric token, returning None for NA markers."""
    t = token.strip().replace(",", "")
    if t.lower() in _NA_TOKENS:
        return None
    try:
        return float(t.rstrip("%"))
    except ValueError:
        return None


def _pick_cr6_pd_lgd(tokens: Iterable[str]) -> tuple[float | None, float | None]:
    """Return (pd_pct, lgd_pct) given the tokens that follow the PD range.

    CR6 columns after the PD range — all numeric, same order on every row:

        1: Original on-balance gross exposure ($M)
        2: Off-balance pre-CCF ($M)
        3: Average CCF (%)
        4: EAD post CRM and post-CCF ($M)
        5: Average PD (%)                    ← target
        6: Number of borrowers
        7: Average LGD (%)                   ← target
        8: Average maturity (Years)          ← non-retail only (skipped on retail)
        9: RWA ($M)
        10: RWA density (%)
        11: Expected Loss ($M)

    PD is always the 5th numeric token. LGD is the 7th.
    """
    numerics: list[float | None] = [_parse_numeric(t) for t in tokens]
    def get(i: int) -> float | None:
        return numerics[i] if i < len(numerics) else None
    return get(4), get(6)


_QUARTER_END_DAY = {3: 31, 6: 30, 9: 30, 12: 31}


def _coerce_reporting_date(value: date | str | None) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return pd.to_datetime(value).date()
    # Default: CBA FY ending 30 June of the latest full year.
    today = date.today()
    year = today.year if today.month >= 7 else today.year - 1
    return date(year, 6, 30)


def _derive_cba_period_code(d: date) -> str:
    """CBA fiscal year ends 30 June. June → FY<year>; December → H1FY<year+1>."""
    if d.month == 6:
        return f"FY{d.year}"
    if d.month == 12:
        return f"H1FY{d.year + 1}"
    # Quarterly: fall back to calendar-quarter slug.
    quarter = (d.month - 1) // 3 + 1
    return f"{d.year}Q{quarter}"
