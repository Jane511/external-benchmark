"""
reports/make_figures.py — regenerate the README charts for this repo.

Every figure is built from the committed model-input CSVs in output/data/
(source-published, aggregated benchmark values only), so the charts regenerate
reproducibly with:

    python reports/make_figures.py

Outputs PNGs into reports/figures/.
"""
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "output" / "data"
FIG = ROOT / "reports" / "figures"
FIG.mkdir(parents=True, exist_ok=True)

plt.rcParams.update({
    "figure.dpi": 130, "savefig.dpi": 130,
    "font.size": 12.5, "axes.titlesize": 15, "axes.titleweight": "bold",
    "axes.labelsize": 12.5, "axes.grid": True, "grid.alpha": 0.25,
    "axes.spines.top": False, "axes.spines.right": False,
})
BASE, STRESS, ACCENT = "#2166ac", "#b2182b", "#4d4d4d"


def save(fig, name):
    fig.tight_layout()
    fig.savefig(FIG / name)
    plt.close(fig)
    print("wrote", FIG / name)


# 1. Expected-loss rate by segment (basis points) ----------------------------
el = pd.read_csv(DATA / "expected_loss_inputs.csv").sort_values("expected_loss_rate_bps")
fig, ax = plt.subplots(figsize=(7.8, 5.0))
bars = ax.barh(el.segment_label, el.expected_loss_rate_bps, color=BASE, edgecolor="white")
for y, v in enumerate(el.expected_loss_rate_bps):
    ax.text(v, y, f" {v:.0f}", va="center", fontsize=10)
ax.set_xlabel("expected-loss rate (basis points, PD × LGD)")
ax.set_title("Expected-loss rate by segment (from disclosed PD × LGD)")
ax.grid(axis="y", alpha=0)
save(fig, "el_rate_by_segment_bps.png")

# 2. Base vs stressed EL rate by segment (severe scenario) -------------------
st = pd.read_csv(DATA / "stress_testing_inputs.csv")
# One row per segment x scenario now; show the headline severe scenario.
st = st[st.scenario == "severe"].sort_values("stressed_expected_loss_rate_decimal")
fig, ax = plt.subplots(figsize=(8.4, 5.0))
y = range(len(st))
ax.barh([i + 0.2 for i in y], st.base_expected_loss_rate_decimal * 1e4, height=0.4,
        label="base", color=BASE)
ax.barh([i - 0.2 for i in y], st.stressed_expected_loss_rate_decimal * 1e4, height=0.4,
        label="stressed (severe)", color=STRESS)
ax.set_yticks(list(y))
ax.set_yticklabels(st.segment_label)
ax.set_xlabel("expected-loss rate (basis points)")
ax.set_title("Base vs severe-stressed expected loss by segment")
ax.legend(frameon=False, loc="lower right")
ax.grid(axis="y", alpha=0)
save(fig, "base_vs_stressed_el_by_segment.png")

# 3. Residential-mortgage PD across disclosing banks (the anchoring story) ----
pd_in = pd.read_csv(DATA / "pd_inputs.csv")
res = pd_in[pd_in.segment == "residential_mortgage"].copy()
res["bank"] = res.source_id.str.split("_").str[0].str.replace("MACQUARIE", "MQG")
res = res.sort_values("pd_decimal")
median_pd = res.pd_decimal.median()
fig, ax = plt.subplots(figsize=(7.2, 4.6))
bars = ax.bar(res.bank, res.pd_decimal * 100, color=BASE, width=0.6, edgecolor="white")
for b, v in zip(bars, res.pd_decimal * 100):
    ax.text(b.get_x() + b.get_width() / 2, v, f"{v:.2f}%", ha="center", va="bottom", fontsize=10)
ax.axhline(median_pd * 100, color=STRESS, linestyle="--", linewidth=1.6,
           label=f"median used as benchmark ({median_pd*100:.2f}%)")
ax.set_ylabel("disclosed 12-month PD (%)")
ax.set_title("Residential-mortgage PD, anchored to Pillar 3 disclosures")
ax.legend(frameon=False)
save(fig, "residential_pd_by_bank.png")

print("\nAll figures written to", FIG)
