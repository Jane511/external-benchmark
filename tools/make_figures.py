"""
tools/make_figures.py — regenerate the README charts for this repo.

Every figure is built from the committed model-input CSVs in outputs/data/
(source-published, aggregated benchmark values only), so the charts regenerate
reproducibly with:

    python tools/make_figures.py

Outputs PNGs into outputs/charts/.
"""
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "outputs" / "data"
FIG = ROOT / "outputs" / "charts"
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


el = pd.read_csv(DATA / "expected_loss_inputs.csv")

# 1. PD by segment ------------------------------------------------------------
pds = el.sort_values("pd_decimal")
fig, ax = plt.subplots(figsize=(7.8, 5.0))
ax.barh(pds.segment_label, pds.pd_decimal * 100, color=BASE, edgecolor="white")
for y, v in enumerate(pds.pd_decimal * 100):
    ax.text(v, y, f" {v:.2f}%", va="center", fontsize=10)
ax.set_xlabel("benchmark PD (%, median across disclosing sources)")
ax.set_title("Probability of default (PD) by segment")
ax.grid(axis="y", alpha=0)
save(fig, "pd_by_segment.png")

# 2. LGD by segment -----------------------------------------------------------
lgds = el.sort_values("lgd_decimal")
fig, ax = plt.subplots(figsize=(7.8, 5.0))
ax.barh(lgds.segment_label, lgds.lgd_decimal * 100, color=ACCENT, edgecolor="white")
for y, v in enumerate(lgds.lgd_decimal * 100):
    ax.text(v, y, f" {v:.0f}%", va="center", fontsize=10)
ax.set_xlabel("benchmark LGD (%, median across disclosing sources)")
ax.set_title("Loss given default (LGD) by segment")
ax.grid(axis="y", alpha=0)
save(fig, "lgd_by_segment.png")

# 3. Expected-loss rate by segment (basis points) ----------------------------
els = el.sort_values("expected_loss_rate_bps")
fig, ax = plt.subplots(figsize=(7.8, 5.0))
ax.barh(els.segment_label, els.expected_loss_rate_bps, color="#762a83", edgecolor="white")
for y, v in enumerate(els.expected_loss_rate_bps):
    ax.text(v, y, f" {v:.0f}", va="center", fontsize=10)
ax.set_xlabel("expected-loss rate (basis points, = PD × LGD)")
ax.set_title("Expected-loss rate by segment (from disclosed PD × LGD)")
ax.grid(axis="y", alpha=0)
save(fig, "el_rate_by_segment_bps.png")

# 4. Per-segment PD / LGD anchored to each disclosing source -----------------
# An "anchor" chart shows every disclosing source for a segment around the
# median the engine uses as the benchmark. It is only meaningful where >= 2
# sources disclose the parameter — bank Pillar 3 reports publish PD/LGD by Basel
# asset class, so only residential and commercial property have a multi-source
# PD spread (the rest are single-source and are NOT anchored, by design).
pd_in = pd.read_csv(DATA / "pd_inputs.csv")
lgd_in = pd.read_csv(DATA / "lgd_inputs.csv")


def _source_label(row):
    if row.source_type == "apra_performance":
        return "APRA\nfloor"
    head = row.source_id.split("_")[0]
    return "MQG" if head == "MACQUARIE" else head


def anchor_chart(src_df, value_col, segment, ylabel, title, fname, decimals=2):
    sub = src_df[src_df.segment == segment].copy()
    if sub[value_col].notna().sum() < 2:
        print("skip", fname, "- fewer than 2 disclosing sources")
        return
    sub["lab"] = sub.apply(_source_label, axis=1)
    sub = sub.sort_values(value_col)
    median = sub[value_col].median()
    colours = [ACCENT if t == "apra_performance" else BASE for t in sub.source_type]
    fig, ax = plt.subplots(figsize=(7.2, 4.6))
    bars = ax.bar(sub.lab, sub[value_col] * 100, color=colours, width=0.6, edgecolor="white")
    for b, v in zip(bars, sub[value_col] * 100):
        ax.text(b.get_x() + b.get_width() / 2, v, f"{v:.{decimals}f}%",
                ha="center", va="bottom", fontsize=9.5)
    ax.axhline(median * 100, color=STRESS, linestyle="--", linewidth=1.6,
               label=f"median used as benchmark ({median*100:.{decimals}f}%)")
    handles = [plt.Line2D([], [], color=STRESS, ls="--", lw=1.6,
                          label=f"median benchmark ({median*100:.{decimals}f}%)"),
               Patch(color=BASE, label="bank Pillar 3 disclosure")]
    if (sub.source_type == "apra_performance").any():
        handles.append(Patch(color=ACCENT, label="APRA regulatory floor"))
    ax.legend(handles=handles, frameon=False, fontsize=9)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    save(fig, fname)


anchor_chart(pd_in, "pd_decimal", "residential_mortgage", "disclosed 12-month PD (%)",
             "Residential property — PD anchored to disclosures", "residential_pd_by_bank.png", 2)
anchor_chart(lgd_in, "lgd_decimal", "residential_mortgage", "disclosed LGD (%)",
             "Residential property — LGD anchored to disclosures", "residential_lgd_by_bank.png", 1)
anchor_chart(pd_in, "pd_decimal", "commercial_property", "disclosed 12-month PD (%)",
             "Commercial property — PD anchored to disclosures", "commercial_property_pd_by_bank.png", 2)
anchor_chart(lgd_in, "lgd_decimal", "commercial_property", "disclosed LGD (%)",
             "Commercial property — LGD anchored to disclosures", "commercial_property_lgd_by_bank.png", 1)

print("\nAll figures written to", FIG)
