import matplotlib.pyplot as plt
from analysis.visualization.characterisation.features import (warm_cold_drop, double_peak_index, weekend_shape_diff)
from analysis.visualization.characterisation.indices import (hourly_index, daily_index, monthly_index)
import polars as pl

"""
PLOT INDICES
"""
def plot_hourly_indices(
    loader,
    station_name,
    channel="channels_all",
    interval=None,
    figsize=(10, 4),
    ylim=(0, 0.2),
    show_metrics=True
):
    Ih_wd = hourly_index(
        loader, station_name, channel=channel, interval=interval, weekday=True
    )
    Ih_we = hourly_index(
        loader, station_name, channel=channel, interval=interval, weekday=False
    )

    plt.figure(figsize=figsize)

    plt.plot(
        Ih_wd["hour"], Ih_wd["I_h"],
        color="tab:blue", linewidth=2, label="Weekday"
    )
    plt.plot(
        Ih_we["hour"], Ih_we["I_h"],
        color="tab:orange", linewidth=2, label="Weekend"
    )

    plt.xlabel("Hour of day")
    plt.ylabel("Hourly index $I_h$")
    plt.ylim(ylim)
    plt.grid(alpha=0.3)
    plt.legend(frameon=False)

    if show_metrics:
        dpi = double_peak_index(Ih_wd)
        diff = weekend_shape_diff(Ih_wd, Ih_we)

        plt.title(
            f"Hourly traffic index – {station_name}\n"
            f"DPI = {dpi:.2f}, Weekend shape diff = {diff:.2f}"
        )
    else:
        plt.title(f"Hourly traffic index – {station_name}")

    plt.tight_layout()
    plt.show()




def plot_daily_indices(
    loader,
    station_name,
    channel="channels_all",
    interval=None,
    figsize=(6, 3),
    ylim=(0.5, 1.5),
    show_metric=True
):
    Id = daily_index(loader, station_name, channel, interval=interval)

    plt.figure(figsize=figsize)

    plt.plot(
        Id["weekday"], Id["I_d"],
        marker="o", linewidth=2
    )

    plt.xticks(
        ticks=range(1, 8),
        labels=["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    )

    plt.xlabel("Day of week")
    plt.ylabel("Daily index $I_d$")
    plt.ylim(ylim)
    plt.grid(alpha=0.3)

    if show_metric:
        weekend_drop = Id.filter(pl.col("weekday") >= 6)["I_d"].mean()
        weekday_mean = Id.filter(pl.col("weekday") <= 5)["I_d"].mean()
        diff = weekday_mean - weekend_drop

        plt.title(
            f"Daily traffic index – {station_name}\n"
            f"Weekday–Weekend diff = {diff:.2f}"
        )
    else:
        plt.title(f"Daily traffic index – {station_name}")

    plt.tight_layout()
    plt.show()




def plot_monthly_indices(loader, station_name, channel = "channels_all", interval=None, ylim=None):
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



def plot_hourly_indices_all(
    loader,
    channel="channels_all",
    interval=None,
    figsize=(10, 4),
    ylim=(0, 0.2)
):
    plt.figure(figsize=figsize)

    Ih_wd_all = []
    Ih_we_all = []

    for station in loader.get_bicyle_stations():
        Ih_wd = hourly_index(loader, station, channel, interval, weekday=True)
        Ih_we = hourly_index(loader, station, channel, interval, weekday=False)

        Ih_wd_all.append(Ih_wd)
        Ih_we_all.append(Ih_we)

        # individual stations (background)
        plt.plot(
            Ih_wd["hour"], Ih_wd["I_h"],
            color="tab:blue", alpha=0.15, linewidth=1
        )
        plt.plot(
            Ih_we["hour"], Ih_we["I_h"],
            color="tab:orange", alpha=0.15, linewidth=1
        )

    # mean profiles (foreground)
    wd_mean = (
        pl.concat(Ih_wd_all)
        .group_by("hour")
        .agg(pl.mean("I_h").alias("mean"))
        .sort("hour")
    )
    we_mean = (
        pl.concat(Ih_we_all)
        .group_by("hour")
        .agg(pl.mean("I_h").alias("mean"))
        .sort("hour")
    )

    plt.plot(
        wd_mean["hour"], wd_mean["mean"],
        color="tab:blue", linewidth=2.5, label="Weekday (mean)"
    )
    plt.plot(
        we_mean["hour"], we_mean["mean"],
        color="tab:orange", linewidth=2.5, label="Weekend (mean)"
    )

    plt.xlabel("Hour of day")
    plt.ylabel("Hourly index $I_h$")
    plt.title("Hourly traffic indices across all stations")
    plt.ylim(ylim)
    plt.legend(frameon=False)
    plt.grid(alpha=0.3)

    plt.tight_layout()
    plt.show()

def plot_daily_indices_all(
    loader,
    channel="channels_all",
    interval=None,
    figsize=(8, 4),
    ylim=(0.5, 1.5)
):
    plt.figure(figsize=figsize)

    Id_all = []

    for station in loader.get_bicyle_stations():
        Id = (
            daily_index(loader, station, channel, interval=interval)
            .sort("weekday")
        )
        Id_all.append(Id)

        x = Id["weekday"].to_numpy()
        y = Id["I_d"].to_numpy()

        # Mon–Fri
        plt.plot(
            x[:5], y[:5],
            color="tab:blue", alpha=0.25, linewidth=1
        )
        # Fri–Sat
        plt.plot(
            x[4:6], y[4:6],
            color="tab:blue", alpha=0.25, linewidth=1
        )
        # Sat–Sun
        plt.plot(
            x[5:], y[5:],
            color="tab:orange", alpha=0.25, linewidth=1
        )

    # mean profile
    Id_mean = (
        pl.concat(Id_all)
        .group_by("weekday")
        .agg(pl.mean("I_d").alias("mean"))
        .sort("weekday")
    )

    x = Id_mean["weekday"].to_numpy()
    y = Id_mean["mean"].to_numpy()

    plt.plot(x[:5], y[:5], color="tab:blue", linewidth=3, label="Weekday (mean)")
    plt.plot(x[4:6], y[4:6], color="tab:blue", linewidth=3)
    plt.plot(x[5:], y[5:], color="tab:orange", linewidth=3, label="Weekend (mean)")

    plt.xticks(
        ticks=range(1, 8),
        labels=["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    )

    plt.xlabel("Day of week")
    plt.ylabel("Daily index $I_d$")
    plt.title("Daily traffic indices across all stations")
    plt.ylim(ylim)
    plt.legend(frameon=False)
    plt.grid(alpha=0.3)

    plt.tight_layout()
    plt.show()


"""
PLOT PCA
"""

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


