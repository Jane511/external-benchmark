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

# 3. Big 4 + Macquarie PD / LGD vs the APRA floor (per-source "anchor") -------
# Shows every disclosing source for a segment (each bank, plus the APRA floor in
# grey) around the median the engine uses as the benchmark. Only meaningful where
# >= 2 sources disclose the parameter — bank Pillar 3 publishes PD/LGD by Basel
# asset class, so residential and commercial property have a multi-bank spread.
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
    ax.axhline(median * 100, color=STRESS, linestyle="--", linewidth=1.6)
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
             "Residential property — PD: each bank vs APRA floor", "residential_pd_by_bank.png", 2)
anchor_chart(lgd_in, "lgd_decimal", "residential_mortgage", "disclosed LGD (%)",
             "Residential property — LGD: each bank vs APRA floor", "residential_lgd_by_bank.png", 1)
anchor_chart(pd_in, "pd_decimal", "commercial_property", "disclosed 12-month PD (%)",
             "Commercial property — PD: each bank vs APRA floor", "commercial_property_pd_by_bank.png", 2)
anchor_chart(lgd_in, "lgd_decimal", "commercial_property", "disclosed LGD (%)",
             "Commercial property — LGD: each bank vs APRA floor", "commercial_property_lgd_by_bank.png", 1)

# Cohort-tagged raw observations (audit-trail export) drive the bank/non-bank
# charts. Banks = Big4 + Macquarie (Pillar 3); non-banks disclose no Basel
# PD/LGD, only arrears / impaired / realised-loss rates.
label_map = dict(zip(el.segment, el.segment_label))
raw = pd.read_csv(DATA / "raw_observations.csv")
raw["is_bank"] = raw.cohort.isin(["peer_big4", "peer_other_major_bank"])

# 4. Big 4 + Macquarie — average PD and LGD by segment -----------------------
bank = raw[raw.is_bank & raw.parameter.isin(["pd", "lgd"])]
piv = bank.pivot_table(index="segment", columns="parameter", values="value", aggfunc="mean")
piv = piv.dropna(subset=["pd", "lgd"]).sort_values("pd")
piv["label"] = [label_map.get(s, s) for s in piv.index]
fig, (axp, axl) = plt.subplots(1, 2, figsize=(13.5, 6.0), sharey=True)
axp.barh(piv.label, piv.pd * 100, color=BASE, edgecolor="white")
for y, v in enumerate(piv.pd * 100):
    axp.text(v, y, f" {v:.2f}%", va="center", fontsize=9.5)
axp.set_xlabel("average PD (%)")
axp.set_title("Average PD")
axp.grid(axis="y", alpha=0)
axl.barh(piv.label, piv.lgd * 100, color=ACCENT, edgecolor="white")
for y, v in enumerate(piv.lgd * 100):
    axl.text(v, y, f" {v:.0f}%", va="center", fontsize=9.5)
axl.set_xlabel("average LGD (%)")
axl.set_title("Average LGD")
axl.grid(axis="y", alpha=0)
fig.suptitle("Big 4 + Macquarie — average PD and LGD by segment (Pillar 3)",
             fontsize=15, fontweight="bold")
save(fig, "bank_avg_pd_lgd_by_segment.png")

# 5. Non-bank lenders — disclosed arrears / impaired / loss rates ------------
# Non-banks publish NO Basel PD/LGD; these are the risk indicators they do report.
nb = raw[(raw.cohort == "peer_non_bank") & raw.value.notna()
         & raw.parameter.isin(["arrears", "impaired", "lgd"])].copy()
nb["lender"] = nb.source_id.str.split("_").str[0]
nb = (nb.sort_values("as_of_date")
        .groupby(["lender", "segment", "data_definition_class"], as_index=False).last())
metric_name = {"arrears_90_plus_days": "90+ arrears",
               "impaired_loans_ratio": "impaired", "realised_loss_rate": "realised loss"}
metric_col = {"arrears_90_plus_days": "#dd8452",
              "impaired_loans_ratio": "#c44e52", "realised_loss_rate": BASE}
seg_short = {"residential_mortgage_specialist": "spec. resi",
             "consumer_secured": "consumer secured", "corporate_sme": "corporate SME",
             "bridging_residential": "bridging", "residential_mortgage": "residential"}
nb["metric"] = nb.data_definition_class.map(metric_name)
nb["lab"] = (nb.lender + " · " + nb.segment.map(lambda s: seg_short.get(s, s))
             + " (" + nb.metric + ")")
nb = nb.sort_values("value")
colours = [metric_col[d] for d in nb.data_definition_class]
fig, ax = plt.subplots(figsize=(8.8, 6.0))
ax.barh(nb.lab, nb.value * 100, color=colours, edgecolor="white")
for y, v in enumerate(nb.value * 100):
    ax.text(v, y, f" {v:.2f}%", va="center", fontsize=9)
ax.set_xlabel("disclosed rate (% of portfolio)")
ax.set_title("Non-bank lenders — disclosed risk rates\n"
             "(arrears / impaired / loss — no Basel PD/LGD)")
ax.grid(axis="y", alpha=0)
handles = [Patch(color=c, label=n) for n, c in
           [("90+ arrears", "#dd8452"), ("impaired", "#c44e52"), ("realised loss", BASE)]]
ax.legend(handles=handles, frameon=False, fontsize=9, loc="lower right")
save(fig, "nonbank_arrears_loss.png")

print("\nAll figures written to", FIG)
