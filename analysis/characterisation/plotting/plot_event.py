import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import polars as pl
import numpy as np
from analysis.characterisation.event import compute_event_deltas
from analysis.characterisation.helpers import fit_optimal_spline

def plot_holiday_impact(delta_labeled, color_map=None, show_centers=True, savepath=None):
    plt.rcParams.update({
        "font.size": 13,
        "axes.labelsize": 13,
        "xtick.labelsize": 12,
        "ytick.labelsize": 12,
        "legend.fontsize": 13,
    })


    if color_map is None:
        color_map = {
            "recreational": "#55A868",
            "mixed": "#DD8452",
            "utilitarian": "#4C72B0",
        }

    has_dpi = "DPI_delta" in delta_labeled.columns
    has_wsd = "WSD_delta" in delta_labeled.columns

    if not (has_dpi or has_wsd):
        raise ValueError("No holiday delta features available for plotting")

    plt.figure(figsize=(6, 5))

    for (utype,), df_u in delta_labeled.group_by("usage_type"):
        x = df_u["DPI_delta"] if has_dpi else np.zeros(df_u.height)
        y = df_u["WSD_delta"] if has_wsd else np.zeros(df_u.height)

        plt.scatter(
            x, y,
            color=color_map.get(utype, "gray"),
            alpha=0.8,
            edgecolors="black",
            s=80,
        )

    if show_centers:
        centers = (
            delta_labeled
            .group_by("usage_type")
            .agg([
                pl.median("DPI_delta").alias("x") if has_dpi else pl.lit(0.0).alias("x"),
                pl.median("WSD_delta").alias("y") if has_wsd else pl.lit(0.0).alias("y"),
            ])
        )

        for row in centers.iter_rows(named=True):
            plt.scatter(
                row["x"], row["y"],
                marker="X", s=200,
                color=color_map.get(row["usage_type"], "gray"),
                edgecolors="black",
                linewidths=1.5,
                zorder=5,
            )


    plt.xlabel("ΔDPI" if has_dpi else "")
    plt.ylabel("ΔWSD" if has_wsd else "")

    median_handle = Line2D(
        [0], [0], marker="X", color="w",
        label="Group median",
        markerfacecolor="gray",
        markeredgecolor="black",
        markersize=13, linewidth=0
    )
    plt.legend(handles=[median_handle], frameon=False, fontsize=12)

    plt.grid(alpha=0.2)
    plt.tight_layout()

    if savepath is not None:
        plt.savefig(savepath, dpi=300, bbox_inches="tight")

    plt.show()



def plot_weather_response(
    df_resp,
    *,
    xlabel,
    title,
    y_label="Relative change (vs baseline)",
):
    plt.figure(figsize=(6,4))

    for usage in ["recreational", "mixed", "utilitarian"]:
        d = df_resp.filter(pl.col("usage_type") == usage)
        plt.plot(d["var_bin"], d["mean_delta"], marker="o", label=usage)

    plt.xlabel(xlabel)
    plt.ylabel(y_label)
    plt.title(title)
    plt.legend(frameon=False)
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.show()




def plot_utilitarian_spline(
    x,
    y,
    x_fit,
    y_fit,
    title
):
    plt.figure(figsize=(6, 4))
    plt.scatter(x, y, alpha=0.8)
    plt.plot(x_fit, y_fit, color="black", lw=2, label="Optimal spline")

    plt.axhline(0, color="gray", ls="--", alpha=0.6)
    plt.xlabel("Baseline utilitarian score")
    plt.ylabel("Utilitarian score change")
    plt.title(title)
    plt.legend(frameon=False)
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.show()
    


def plot_event_utilitarian_spline(
    loader,
    intervals,
    title,
    k=2,
):
    delta_raw = compute_event_deltas(
        loader=loader,
        intervals=intervals
    )

    if delta_raw.is_empty():
        print(f"No valid stations for event: {title}")
        return None

    delta_df = (
        delta_raw
        .select(["station", "U_base", "U_event", "U_delta"])
        .drop_nulls()
    )

    x = delta_df["U_base"].to_numpy()
    y = delta_df["U_delta"].to_numpy()

    spline, s_opt, mse_opt = fit_optimal_spline(x, y, k=k)
    x_fit = np.linspace(x.min(), x.max(), 200)
    y_fit = spline(x_fit)

    plot_utilitarian_spline(
        x=x,
        y=y,
        x_fit=x_fit,
        y_fit=y_fit,
        title=title,
    )

    return {
        "n_stations": delta_df.height,
        "s_opt": s_opt,
        "mse": mse_opt,
    }


