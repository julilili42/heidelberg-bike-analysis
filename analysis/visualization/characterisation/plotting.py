import matplotlib.pyplot as plt
from analysis.visualization.characterisation.features import (warm_cold_drop, double_peak_index, weekend_shape_diff)
from analysis.visualization.characterisation.indices import (hourly_index, daily_index, monthly_index)
import polars as pl

def plot_hourly_profiles(
    loader,
    station_name,
    channel = "channels_all",
    interval=None,
    figsize=(10, 4),
    ylim=None
):
    Ih_weekday = hourly_index(
        loader, station_name, channel=channel, interval=interval, weekday=True
    )
    
    Ih_weekend = hourly_index(
        loader, station_name, channel=channel, interval=interval, weekday=False
    )
    
    print("DPI", double_peak_index(Ih_weekday))
    print("Diff", weekend_shape_diff(Ih_weekday, Ih_weekend))
    fig, axes = plt.subplots(1, 2, figsize=figsize, sharey=True)

    # weekday
    axes[0].plot(
        Ih_weekday["hour"],
        Ih_weekday["I_h"],
        linewidth=2
    )
    axes[0].set_title("Weekday")
    axes[0].set_xlabel("Hour")
    axes[0].set_ylabel("Hourly Index $I_h$")
    axes[0].grid(alpha=0.3)

    # weekend
    axes[1].plot(
        Ih_weekend["hour"],
        Ih_weekend["I_h"],
        linewidth=2
    )
    axes[1].set_title("Weekend")
    axes[1].set_xlabel("Hour")
    axes[1].grid(alpha=0.3)

    if ylim is not None:
        axes[0].set_ylim(ylim)

    fig.suptitle(f"Hourly Profiles – {station_name}", fontsize=14)
    plt.tight_layout()
    plt.show()


def plot_daily_profile(loader, station_name, channel = "channels_all", interval=None, ylim=None):
    Id = daily_index(loader, station_name, channel, interval=interval)

    plt.figure(figsize=(6, 3))
    plt.plot(
        Id["weekday"],
        Id["I_d"],
        marker="o",
        linewidth=2
    )

    plt.xticks(
        ticks=range(1,8),
        labels=["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    )

    plt.title(f"Daily Profile – {station_name}")
    plt.ylabel("Daily Index $I_d$")
    plt.grid(alpha=0.3)

    if ylim is not None:
        plt.ylim(ylim)

    plt.tight_layout()
    plt.show()




def plot_monthly_profile(loader, station_name, channel = "channels_all", interval=None, ylim=None):
    Im = monthly_index(loader, station_name, channel, interval=interval)

    print("Warm/Cold Drop:", warm_cold_drop(Im))
    plt.figure(figsize=(6, 3))
    plt.plot(
        Im["month"],
        Im["I_m"],
        marker="o",
        linewidth=2
    )

    plt.xticks(
        ticks=range(1, 13),
        labels=["Jan","Feb","Mar","Apr","May","Jun",
                "Jul","Aug","Sep","Oct","Nov","Dec"]
    )

    plt.title(f"Monthly Profile – {station_name}")
    plt.ylabel("Monthly Index $I_m$")
    plt.grid(alpha=0.3)

    if ylim is not None:
        plt.ylim(ylim)

    plt.tight_layout()
    plt.show()




def plot_pca_clusters(
    df,
    pc1="PC1",
    pc2="PC2",
    cluster_col="cluster_k",
    label_col="station",
    annotate=True,
    figsize=(6, 5),
    alpha=0.85,
    point_size=80
):
    
    label_map = {
        0: "Cluster 0",
        1: "Cluster 1",
        2: "Cluster 2"
    }
    
    plt.figure(figsize=figsize)

    clusters = sorted(df[cluster_col].unique())

    for c in clusters:
        df_c = df.filter(pl.col(cluster_col) == c)

        plt.scatter(
            df_c[pc1].to_numpy(),
            df_c[pc2].to_numpy(),
            s=point_size,
            alpha=alpha,
            label=label_map.get(c, f"Cluster {c}"), 
            edgecolors="black",
            linewidths=0.6
        )

    if annotate:
        for row in df.iter_rows(named=True):
            plt.text(
                row[pc1] + 0.01,
                row[pc2] + 0.01,
                row[label_col],
                fontsize=8,
                alpha=0.8
            )

    plt.xlabel(pc1)
    plt.ylabel(pc2)
    plt.title("PCA of station features")
    plt.legend(frameon=True)
    plt.grid(alpha=0.25)
    plt.tight_layout()
    plt.show()


