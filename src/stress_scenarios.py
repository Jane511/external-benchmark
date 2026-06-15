"""Stress scenario parameter set for the stress-testing model inputs.

Loads ``config/stress_scenarios.yaml`` into a small typed library. The
multipliers are illustrative reality-check overlays (not calibrated
regulatory parameters); see the YAML header and the report's stress
governance section. Mirrors the loader style of :mod:`src.reality_check`.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml


@dataclass(frozen=True)
class StressScenario:
    name: str
    label: str
    pd_multiplier: float
    lgd_multiplier: float
    ccf_multiplier: float
    apply_reality_check_floor: bool
    macro_path: str


@dataclass(frozen=True)
class StressScenarioLibrary:
    scenarios: list[StressScenario]
    no_diversification: bool
    last_review_date: str
    next_review_due: str
    validation_note: str

    def ordered(self) -> list[StressScenario]:
        return list(self.scenarios)


def _flatten(text: object) -> str:
    """Collapse a YAML folded-scalar block into a single clean line."""
    return " ".join(str(text or "").split())


def load_stress_scenarios(
    yaml_path: Optional[Path] = None,
) -> StressScenarioLibrary:
    """Load stress scenarios. Default path: config/stress_scenarios.yaml."""
    if yaml_path is None:
        yaml_path = (
            Path(__file__).resolve().parent.parent
            / "config" / "stress_scenarios.yaml"
        )
    with yaml_path.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    scenarios: list[StressScenario] = []
    for name, body in (raw.get("scenarios") or {}).items():
        scenarios.append(StressScenario(
            name=name,
            label=body.get("label", name),
            pd_multiplier=float(body["pd_multiplier"]),
            lgd_multiplier=float(body["lgd_multiplier"]),
            ccf_multiplier=float(body.get("ccf_multiplier", 1.0)),
            apply_reality_check_floor=bool(
                body.get("apply_reality_check_floor", False)
            ),
            macro_path=_flatten(body.get("macro_path", "")),
        ))

    last = raw.get("last_review") or {}
    return StressScenarioLibrary(
        scenarios=scenarios,
        no_diversification=bool(raw.get("no_diversification", True)),
        last_review_date=last.get("date", "unknown"),
        next_review_due=last.get("next_review_due", "unknown"),
        validation_note=_flatten(last.get("validation_note", "")),
    )
