"""Shared helpers for APRA adapters (Performance + QPEX).

Both APRA workbooks use the same wide-format conventions:

- Dates live as column headers in a specific row (typically index 3).
- Dates arrive either as python ``datetime`` instances (QPEX) or as
  short month strings like "Jun 2022" (Performance).
- We always represent a quarter by its last day (e.g. 2022-03-31).

Extracting these helpers keeps the two adapters from drifting apart on
date parsing / quarter-end conventions.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pandas as pd


_QUARTER_END_DAY: dict[int, int] = {3: 31, 6: 30, 9: 30, 12: 31}


def coerce_quarter_end(value: Any) -> date | None:
    """Return a quarter-end ``date`` if ``value`` parses; else ``None``.

    Accepts python ``datetime``/``date`` instances and strings like
    ``"Jun 2004"`` or ``"December 2025"``. The result is always snapped to
    the last day of the quarter that contains the parsed month so that
    Performance (string headers → 1st of month) and QPEX (native datetime
    2022-06-30) converge on the same representation.
    """
    d: date | None
    if isinstance(value, datetime):
        d = value.date()
    elif isinstance(value, date):
        d = value
    elif isinstance(value, str):
        try:
            d = pd.to_datetime(value.strip(), errors="raise").date()
        except (ValueError, TypeError):
            return None
    else:
        return None

    quarter_month = ((d.month - 1) // 3 + 1) * 3
    day = _QUARTER_END_DAY[quarter_month]
    return date(d.year, quarter_month, day)


def date_to_period_slug(d: date) -> str:
    """Return ``"2025Q4"`` style slug for a quarter-end date."""
    quarter = (d.month - 1) // 3 + 1
    return f"{d.year}Q{quarter}"


def parse_date_row(row: tuple[Any, ...]) -> list[tuple[int, date]]:
    """Pick out all cells in ``row`` that parse as quarter-end dates.

    Column 0 is always skipped — in every APRA sheet that column holds
    metric labels, never a date.
    """
    out: list[tuple[int, date]] = []
    for col_idx, cell in enumerate(row):
        if cell is None or cell == "":
            continue
        if col_idx == 0:
            continue
        d = coerce_quarter_end(cell)
        if d is not None:
            out.append((col_idx, d))
    return out


def find_date_row(
    rows: list[tuple[Any, ...]],
    preferred: int = 3,
    scan_limit: int = 10,
    min_cells: int = 10,
) -> tuple[int, list[tuple[int, date]]]:
    """Locate the row holding quarterly date headers.

    Tries ``preferred`` first; if that row has fewer than ``min_cells``
    parseable dates, scans the first ``scan_limit`` rows. Raises
    ``ValueError`` if no candidate clears the threshold.
    """
    candidates = [preferred] + [
        i for i in range(min(scan_limit, len(rows))) if i != preferred
    ]
    for idx in candidates:
        if idx >= len(rows):
            continue
        parsed = parse_date_row(rows[idx])
        if len(parsed) >= min_cells:
            return idx, parsed
    raise ValueError(
        f"could not locate a date header row (tried rows {candidates[:6]})"
    )
