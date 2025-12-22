import matplotlib.pyplot as plt
from analysis.visualization.characterisation.features import (weekend_shape_diff_index, double_peak_index, seasonal_drop_index)
from analysis.visualization.characterisation.indices import (hourly_index, daily_index, monthly_index)
import polars as pl
import seaborn as sns





"""
PLOT INDICES
"""

WD_COLOR = "tab:blue"
WE_COLOR = "tab:orange"

COLD_COLOR = "tab:brown"
WARM_COLOR = "tab:purple"

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
        color=WD_COLOR, linewidth=2, label="Weekday"
    )
    plt.plot(
        Ih_we["hour"], Ih_we["I_h"],
        color=WE_COLOR, linewidth=2, label="Weekend"
    )

    plt.xlabel("Hour of day")
    plt.ylabel("Hourly index $I_h$")
    plt.ylim(ylim)
    plt.grid(alpha=0.3)
    plt.legend(frameon=False)

    if show_metrics:
        dpi = double_peak_index(Ih_wd)
        diff = weekend_shape_diff_index(Ih_wd, Ih_we)

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
    Id = (
        daily_index(loader, station_name, channel, interval=interval)
        .sort("weekday")
    )

    x = Id["weekday"].to_numpy()
    y = Id["I_d"].to_numpy()

    plt.figure(figsize=figsize)

    # Mon–Fri
    plt.plot(
        x[:5], y[:5],
        marker="o", linewidth=2, color=WD_COLOR, label="Weekday"
    )
    # Fri–Sat 
    plt.plot(
        x[4:6], y[4:6],
        marker="o", linewidth=2, color=WD_COLOR
    )
    # Sat–Sun
    plt.plot(
        x[5:], y[5:],
        marker="o", linewidth=2, color=WE_COLOR, label="Weekend"
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
        weekend_mean = Id.filter(pl.col("weekday") >= 6)["I_d"].mean()
        weekday_mean = Id.filter(pl.col("weekday") <= 5)["I_d"].mean()
        diff = weekday_mean - weekend_mean

        plt.title(
            f"Daily traffic index – {station_name}\n"
            f"Weekday–Weekend diff = {diff:.2f}"
        )
    else:
        plt.title(f"Daily traffic index – {station_name}")

    plt.legend(frameon=False)
    plt.tight_layout()
    plt.show()


def plot_monthly_indices(
    loader,
    station_name,
    channel="channels_all",
    interval=None,
    figsize=(6, 3),
    ylim=None,
    show_metric=True
):
    Im = (
        monthly_index(loader, station_name, channel, interval=interval)
        .sort("month")
    )

    x = Im["month"].to_numpy()
    y = Im["I_m"].to_numpy()

    plt.figure(figsize=figsize)

    # Cold season: Jan–Mar
    plt.plot(
        x[:3], y[:3],
        color=COLD_COLOR,
        marker="o",
        linewidth=2,
        label="Cold season"
    )

    # Transition Mar–Apr (keeps line continuous)
    plt.plot(
        x[2:4], y[2:4],
        color=WARM_COLOR,
        marker="o",
        linewidth=2
    )

    # Warm season: Apr–Oct
    plt.plot(
        x[3:10], y[3:10],
        color=WARM_COLOR,
        marker="o",
        linewidth=2,
        label="Warm season"
    )

    # Transition Oct–Nov
    plt.plot(
        x[9:11], y[9:11],
        color=COLD_COLOR,
        marker="o",
        linewidth=2
    )

    # Cold season: Nov–Dec
    plt.plot(
        x[10:], y[10:],
        color=COLD_COLOR,
        marker="o",
        linewidth=2
    )

    plt.xticks(
        ticks=range(1, 13),
        labels=["Jan","Feb","Mar","Apr","May","Jun",
                "Jul","Aug","Sep","Oct","Nov","Dec"]
    )

    plt.xlabel("Month")
    plt.ylabel("Monthly index $I_m$")
    plt.grid(alpha=0.3)

    if ylim is not None:
        plt.ylim(ylim)

    if show_metric:
        drop = seasonal_drop_index(Im)
        plt.title(
            f"Monthly profile – {station_name}\n"
            f"Warm–Cold drop = {drop:.2f}"
        )
    else:
        plt.title(f"Monthly profile – {station_name}")

    plt.legend(frameon=False)
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
            color=WD_COLOR, alpha=0.15, linewidth=1
        )
        plt.plot(
            Ih_we["hour"], Ih_we["I_h"],
            color=WE_COLOR, alpha=0.15, linewidth=1
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
        color=WD_COLOR, linewidth=2.5, label="Weekday (mean)"
    )
    plt.plot(
        we_mean["hour"], we_mean["mean"],
        color=WE_COLOR, linewidth=2.5, label="Weekend (mean)"
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
    ylim=(0.3, 1.5)
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
            color=WD_COLOR, alpha=0.25, linewidth=1
        )
        # Fri–Sat
        plt.plot(
            x[4:6], y[4:6],
            color=WD_COLOR, alpha=0.25, linewidth=1
        )
        # Sat–Sun
        plt.plot(
            x[5:], y[5:],
            color=WE_COLOR, alpha=0.25, linewidth=1
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

    plt.plot(x[:5], y[:5], color=WD_COLOR, linewidth=3, label="Weekday (mean)")
    plt.plot(x[4:6], y[4:6], color=WD_COLOR, linewidth=3)
    plt.plot(x[5:], y[5:], color=WE_COLOR, linewidth=3, label="Weekend (mean)")

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


def plot_monthly_indices_all(
    loader,
    channel="channels_all",
    interval=None,
    figsize=(8, 4),
    ylim=None
):
    plt.figure(figsize=figsize)

    Im_all = []

    # background: individual stations
    for station in loader.get_bicyle_stations():
        Im = (
            monthly_index(loader, station, channel, interval=interval)
            .sort("month")
        )
        Im_all.append(Im)

        x = Im["month"].to_numpy()
        y = Im["I_m"].to_numpy()

        # Cold season: Jan–Mar
        plt.plot(
            x[:3], y[:3],
            color=COLD_COLOR, alpha=0.25, linewidth=1
        )
        # Transition Mar–Apr
        plt.plot(
            x[2:4], y[2:4],
            color=WARM_COLOR, alpha=0.25, linewidth=1
        )
        # Warm season: Apr–Oct
        plt.plot(
            x[3:10], y[3:10],
            color=WARM_COLOR, alpha=0.25, linewidth=1
        )
        # Transition Oct–Nov
        plt.plot(
            x[9:11], y[9:11],
            color=COLD_COLOR, alpha=0.25, linewidth=1
        )
        # Cold season: Nov–Dec
        plt.plot(
            x[10:], y[10:],
            color=COLD_COLOR, alpha=0.25, linewidth=1
        )

    # mean profile (foreground)
    Im_mean = (
        pl.concat(Im_all)
        .group_by("month")
        .agg(pl.mean("I_m").alias("mean"))
        .sort("month")
    )

    x = Im_mean["month"].to_numpy()
    y = Im_mean["mean"].to_numpy()

    # Cold season mean
    plt.plot(
        x[:3], y[:3],
        color=COLD_COLOR, linewidth=3, label="Cold season (mean)"
    )
    # Transition Mar–Apr
    plt.plot(
        x[2:4], y[2:4],
        color=WARM_COLOR, linewidth=3
    )
    # Warm season mean
    plt.plot(
        x[3:10], y[3:10],
        color=WARM_COLOR, linewidth=3, label="Warm season (mean)"
    )
    # Transition Oct–Nov
    plt.plot(
        x[9:11], y[9:11],
        color=COLD_COLOR, linewidth=3
    )
    # Cold season mean
    plt.plot(
        x[10:], y[10:],
        color=COLD_COLOR, linewidth=3
    )

    plt.xticks(
        ticks=range(1, 13),
        labels=["Jan","Feb","Mar","Apr","May","Jun",
                "Jul","Aug","Sep","Oct","Nov","Dec"]
    )

    plt.xlabel("Month")
    plt.ylabel("Monthly index $I_m$")
    plt.title("Monthly traffic indices across all stations")
    plt.grid(alpha=0.3)

    if ylim is not None:
        plt.ylim(ylim)

    plt.legend(frameon=False)
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



def plot_feature_boxplots(
    df,
    features,
    cluster_col="cluster",
    clusters=(0, 1),
    figsize=(4, 3)
):
    df_filt = df.filter(pl.col(cluster_col).is_in(clusters))

    for feat in features:
        plt.figure(figsize=figsize)
        sns.boxplot(data=df_filt, x=cluster_col, y=feat)
        plt.title(feat)
        plt.tight_layout()
        plt.show()
