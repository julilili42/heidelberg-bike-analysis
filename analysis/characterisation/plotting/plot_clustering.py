import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from analysis.characterisation.helpers import entropy
import polars as pl


def plot_cluster_probabilities_ci(
    cluster_probs_ci,
    station_col,
    prob_col,
    lo_col,
    hi_col,
    title = "Dominant cluster assignment with confidence intervals",
    figsize_scale = 0.35,
):
    df = (
        cluster_probs_ci
        .sort(prob_col, descending=True)
        .group_by(station_col)
        .head(1)
    )

    stations = df[station_col].to_list()
    p = df[prob_col].to_numpy()
    lo = df[lo_col].to_numpy()
    hi = df[hi_col].to_numpy()

    y = np.arange(len(stations))

    xerr_lo = np.clip(p - lo, 0, None)
    xerr_hi = np.clip(hi - p, 0, None)

    plt.figure(figsize=(6, max(3, figsize_scale * len(stations))))

    plt.errorbar(
        p,
        y,
        xerr=[xerr_lo, xerr_hi],
        fmt="o",
        color="black",
        ecolor="gray",
        elinewidth=1.5,
        capsize=3,
    )

    plt.yticks(y, stations)
    plt.xlabel("Cluster probability")
    plt.title(title)
    plt.xlim(0, 1)
    plt.grid(axis="x", alpha=0.3)
    plt.tight_layout()
    plt.show()




def plot_usage_entropy(
    entropy_df,
    entropy_col="entropy",
    usage_col="usage_type",
    figsize=(6, 4),
    title="Usage entropy by dominant station type",
):
    plt.figure(figsize=figsize)

    sns.boxplot(
        data=entropy_df,
        x=usage_col,
        y=entropy_col,
        showfliers=False,
        width=0.5,
    )

    sns.stripplot(
        data=entropy_df,
        x=usage_col,
        y=entropy_col,
        color="black",
        size=5,
        jitter=0.2,
        alpha=0.8,
    )

    plt.xlabel("Dominant usage type")
    plt.ylabel("Usage entropy")
    plt.title(title)
    plt.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.show()




def plot_usage_probabilities_paper(
    usage_probs: pl.DataFrame,
    figsize=(6, 3),
    savepath=None,
):
    plt.rcParams.update({
        "font.size": 12,
        "legend.fontsize": 12,
    })

    pivot = (
        usage_probs
        .pivot(index="station", columns="usage_type", values="probability")
        .fill_null(0)
    )

    for col in ["recreational", "mixed", "utilitarian"]:
        if col not in pivot.columns:
            pivot = pivot.with_columns(pl.lit(0).alias(col))

    pivot = pivot.select(["station", "recreational", "mixed", "utilitarian"])

    ent = entropy(usage_probs).select(["station", "entropy"])

    pivot = (
        pivot
        .join(ent, on="station", how="left")
        .sort("entropy")
    )

    x = np.arange(pivot.height)

    colors = {
        "recreational": "#55A868",
        "mixed": "#DD8452",
        "utilitarian": "#4C72B0",
    }

    fig, ax = plt.subplots(figsize=figsize)

    bottom = np.zeros(pivot.height)
    for key in ["recreational", "mixed", "utilitarian"]:
        vals = pivot[key].to_numpy()
        ax.bar(
            x, vals, bottom=bottom,
            color=colors[key],
            width=0.8,
            label=key.capitalize(),
            edgecolor="none",
        )
        bottom += vals

    ax.set_ylim(0, 1)
    ax.set_yticks([0, 0.5, 1.0])
    ax.set_ylabel("Assignment probability")

    ax.set_xticks(x)
    ax.set_xticklabels([f"S{i+1}" for i in x], rotation=0)
    ax.set_xlabel("Station (ordered by increasing entropy)")

    ax.legend(
        frameon=False,
        ncol=3,
        loc="upper center",
        bbox_to_anchor=(0.5, 1.25),
    )

    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)

    plt.tight_layout()

    if savepath is not None:
        fig.savefig(savepath, dpi=300, bbox_inches="tight") 
        plt.show() 
        plt.close(fig)
    else:
        plt.show()

