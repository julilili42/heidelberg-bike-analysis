import matplotlib.pyplot as plt
from analysis.visualization.characterisation.features import (
    weekend_shape_diff_index,
    double_peak_index,
    seasonal_drop_index,
)
from analysis.visualization.characterisation.indices import (
    hourly_index,
    daily_index,
    monthly_index,
)
import polars as pl
import seaborn as sns
import numpy as np
from matplotlib.lines import Line2D

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
    show_metrics=True,
    ax=None,
    # this is for holiday filtering
    extra_title=None,
    filter_dates=None,
    neg_dates=False,
):
    created_fig = False
    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)
        created_fig = True

    Ih_wd = hourly_index(
        loader,
        station_name,
        channel=channel,
        interval=interval,
        weekday=True,
        filter_dates=filter_dates,
        neg_dates=neg_dates,
    )
    Ih_we = hourly_index(
        loader,
        station_name,
        channel=channel,
        interval=interval,
        weekday=False,
        filter_dates=filter_dates,
        neg_dates=neg_dates,
    )

    ax.plot(Ih_wd["hour"], Ih_wd["I_h"], color=WD_COLOR, linewidth=2, label="Weekday")
    ax.plot(Ih_we["hour"], Ih_we["I_h"], color=WE_COLOR, linewidth=2, label="Weekend")

    ax.set_xlabel("Hour of day")
    ax.set_ylabel("Hourly index $I_h$")
    ax.set_ylim(ylim)
    ax.grid(alpha=0.3)
    ax.legend(frameon=False)

    if show_metrics:
        dpi = double_peak_index(Ih_wd)
        diff = weekend_shape_diff_index(Ih_wd, Ih_we)

        ax.set_title(
            f"Daily traffic index – {station_name}{" " + extra_title if extra_title is not None else ""}\n"
            f"DPI = {dpi:.2f}, Weekend shape diff = {diff:.2f}"
        )
    else:
        ax.set_title(
            f"Hourly traffic index – {station_name}{" " + extra_title if extra_title is not None else ""}"
        )

    if created_fig:
        plt.tight_layout()
        plt.show()


def plot_hourly_indices_subplots(
    loader,
    station_name,
    channel="channels_all",
    interval=None,
    figsize=(18, 5),
    ylim=(0, 0.2),
    show_metrics=True,
    title_1=None,
    title_2=None,
    filter_dates=None,
    neg_dates=False,
):
    fig, axes = plt.subplots(1, 2, figsize=figsize, sharey=True)

    plot_hourly_indices(
        loader,
        station_name,
        channel=channel,
        interval=interval,
        ylim=ylim,
        show_metrics=show_metrics,
        ax=axes[0],
        extra_title=title_1,
        neg_dates=neg_dates,
        filter_dates=filter_dates if neg_dates is True else None,
    )
    plot_hourly_indices(
        loader,
        station_name,
        channel=channel,
        interval=interval,
        ylim=ylim,
        show_metrics=show_metrics,
        ax=axes[1],
        extra_title=title_2,
        filter_dates=filter_dates,
    )

    plt.tight_layout()
    plt.show()


def plot_hourly_indices_all(
    loader,
    channel="channels_all",
    interval=None,
    figsize=(10, 4),
    ylim=(0, 0.2),
    ax=None,
    # this is for holidays
    title=None,
    filter_dates=None,
    neg_dates=False,
):
    created_fig = False
    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)
        created_fig = True

    Ih_wd_all = []
    Ih_we_all = []

    for station in loader.get_bicyle_stations():
        Ih_wd = hourly_index(
            loader,
            station,
            channel,
            interval,
            weekday=True,
            filter_dates=filter_dates,
            neg_dates=neg_dates,
        )
        Ih_we = hourly_index(
            loader,
            station,
            channel,
            interval,
            weekday=False,
            filter_dates=filter_dates,
            neg_dates=neg_dates,
        )

        Ih_wd_all.append(Ih_wd)
        Ih_we_all.append(Ih_we)

        # individual stations (background)
        ax.plot(Ih_wd["hour"], Ih_wd["I_h"], color=WD_COLOR, alpha=0.15, linewidth=1)
        ax.plot(Ih_we["hour"], Ih_we["I_h"], color=WE_COLOR, alpha=0.15, linewidth=1)

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

    ax.plot(
        wd_mean["hour"],
        wd_mean["mean"],
        color=WD_COLOR,
        linewidth=2.5,
        label="Weekday (mean)",
    )
    ax.plot(
        we_mean["hour"],
        we_mean["mean"],
        color=WE_COLOR,
        linewidth=2.5,
        label="Weekend (mean)",
    )

    ax.set_xlabel("Hour of day")
    ax.set_ylabel("Hourly index $I_h$")
    ax.set_title(
        "Hourly traffic indices across all stations" if title is None else title
    )
    ax.set_ylim(ylim)
    ax.legend(frameon=False)
    ax.grid(alpha=0.3)

    if created_fig:
        plt.tight_layout()
        plt.show()


def plot_hourly_indices_all_subplots(
    loader,
    channel="channels_all",
    interval=None,
    figsize=(18, 5),
    ylim=(0, 0.2),
    title_1=None,
    title_2=None,
    filter_dates=None,
    neg_dates=False,
):

    fig, axes = plt.subplots(1, 2, figsize=figsize, sharey=True)

    plot_hourly_indices_all(
        loader,
        channel=channel,
        interval=interval,
        figsize=(figsize[0] // 2, figsize[1]),
        ylim=ylim,
        ax=axes[0],
        title=title_1,
        neg_dates=neg_dates,
        filter_dates=filter_dates if neg_dates is True else None,
    )
    plot_hourly_indices_all(
        loader,
        channel=channel,
        interval=interval,
        figsize=(figsize[0] // 2, figsize[1]),
        ylim=ylim,
        ax=axes[1],
        title=title_2,
        filter_dates=filter_dates,
    )

    plt.tight_layout()
    plt.show()


def plot_daily_indices(
    loader,
    station_name,
    channel="channels_all",
    interval=None,
    figsize=(6, 3),
    ylim=(0.5, 1.5),
    show_metric=True,
    ax=None,
    # this part is for holiday analysis
    extra_title=None,
    filter_dates=None,
    neg_dates=False,
):
    Id = daily_index(
        loader,
        station_name,
        channel,
        interval=interval,
        filter_dates=filter_dates,
        neg_dates=neg_dates,
    ).sort("weekday")

    x = Id["weekday"].to_numpy()
    y = Id["I_d"].to_numpy()

    created_fig = False
    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)
        created_fig = True

    # Mon–Fri
    ax.plot(x[:5], y[:5], marker="o", linewidth=2, color=WD_COLOR, label="Weekday")
    # Fri–Sat
    ax.plot(x[4:6], y[4:6], marker="o", linewidth=2, color=WD_COLOR)
    # Sat–Sun
    ax.plot(x[5:], y[5:], marker="o", linewidth=2, color=WE_COLOR, label="Weekend")

    ax.set_xticks(
        ticks=range(1, 8), labels=["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    )

    ax.set_xlabel("Day of week")
    ax.set_ylabel("Daily index $I_d$")
    ax.set_ylim(ylim)
    ax.grid(alpha=0.3)

    if show_metric:
        weekend_mean = Id.filter(pl.col("weekday") >= 6)["I_d"].mean()
        weekday_mean = Id.filter(pl.col("weekday") <= 5)["I_d"].mean()
        if weekend_mean is not None and weekday_mean is not None:
            diff = weekday_mean - weekend_mean

            ax.set_title(
                f"Daily traffic index – {station_name}{" " + extra_title if extra_title is not None else ""}\n"
                f"Weekday–Weekend diff = {diff:.2f}"
            )
    else:
        ax.set_title(
            f"Daily traffic index – {station_name}{" " + extra_title if extra_title is not None else ""}"
        )

    ax.legend(frameon=False)

    if created_fig:
        plt.tight_layout()
        plt.show()


def plot_daily_indices_subplots(
    loader,
    station_name,
    channel="channels_all",
    interval=None,
    figsize=(12, 4),
    ylim=(0.5, 1.5),
    show_metric=True,
    title_1=None,
    title_2=None,
    filter_dates=None,
    neg_dates=False,
):
    fig, axes = plt.subplots(1, 2, figsize=figsize, sharey=True)

    plot_daily_indices(
        loader,
        station_name,
        channel=channel,
        interval=interval,
        ylim=ylim,
        show_metric=show_metric,
        ax=axes[0],
        extra_title=title_1,
        neg_dates=neg_dates,
        filter_dates=filter_dates if neg_dates is True else None,
    )
    plot_daily_indices(
        loader,
        station_name,
        channel=channel,
        interval=interval,
        ylim=ylim,
        show_metric=show_metric,
        ax=axes[1],
        extra_title=title_2,
        filter_dates=filter_dates,
    )

    plt.tight_layout()
    plt.show()


def plot_daily_indices_all(
    loader,
    channel="channels_all",
    interval=None,
    figsize=(8, 4),
    ylim=(0.3, 1.5),
    ax=None,
    # this is for holidays
    title=None,
    filter_dates=None,
    neg_dates=False,
):
    created_fig = False
    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)
        created_fig = True

    Id_all = []

    for station in loader.get_bicyle_stations():
        Id = daily_index(
            loader,
            station,
            channel,
            interval=interval,
            filter_dates=filter_dates,
            neg_dates=neg_dates,
        ).sort("weekday")
        Id_all.append(Id)

        x = Id["weekday"].to_numpy()
        y = Id["I_d"].to_numpy()

        # Mon–Fri
        ax.plot(x[:5], y[:5], color=WD_COLOR, alpha=0.25, linewidth=1)
        # Fri–Sat
        ax.plot(x[4:6], y[4:6], color=WD_COLOR, alpha=0.25, linewidth=1)
        # Sat–Sun
        ax.plot(x[5:], y[5:], color=WE_COLOR, alpha=0.25, linewidth=1)

    # mean profile
    Id_mean = (
        pl.concat(Id_all)
        .group_by("weekday")
        .agg(pl.mean("I_d").alias("mean"))
        .sort("weekday")
    )

    x = Id_mean["weekday"].to_numpy()
    y = Id_mean["mean"].to_numpy()

    ax.plot(x[:5], y[:5], color=WD_COLOR, linewidth=3, label="Weekday (mean)")
    ax.plot(x[4:6], y[4:6], color=WD_COLOR, linewidth=3)
    ax.plot(x[5:], y[5:], color=WE_COLOR, linewidth=3, label="Weekend (mean)")

    ax.set_xticks(
        ticks=range(1, 8), labels=["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    )

    ax.set_xlabel("Day of week")
    ax.set_ylabel("Daily index $I_d$")
    ax.set_title(
        "Daily traffic indices across all stations" if title is None else title
    )
    ax.set_ylim(ylim)
    ax.legend(frameon=False)
    ax.grid(alpha=0.3)

    if created_fig:
        plt.tight_layout()
        plt.show()


def plot_daily_indices_all_subplots(
    loader,
    channel="channels_all",
    interval=None,
    figsize=(18, 5),
    ylim=(0.3, 1.5),
    title_1=None,
    title_2=None,
    filter_dates=None,
    neg_dates=False,
):
    fig, axes = plt.subplots(1, 2, figsize=figsize, sharey=True)

    plot_daily_indices_all(
        loader,
        channel=channel,
        interval=interval,
        figsize=(figsize[0] // 2, figsize[1]),
        ylim=ylim,
        ax=axes[0],
        title=title_1,
        neg_dates=neg_dates,
        filter_dates=filter_dates if neg_dates is True else None,
    )
    plot_daily_indices_all(
        loader,
        channel=channel,
        interval=interval,
        figsize=(figsize[0] // 2, figsize[1]),
        ylim=ylim,
        ax=axes[1],
        title=title_2,
        filter_dates=filter_dates,
    )

    plt.tight_layout()
    plt.show()


def plot_monthly_indices(
    loader,
    station_name,
    channel="channels_all",
    interval=None,
    figsize=(6, 3),
    ylim=None,
    show_metric=True,
    ax=None,
):
    Im = monthly_index(loader, station_name, channel, interval=interval).sort("month")

    x = Im["month"].to_numpy()
    y = Im["I_m"].to_numpy()

    created_fig = False
    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)
        created_fig = True

    # Cold season: Jan–Mar
    ax.plot(
        x[:3], y[:3], color=COLD_COLOR, marker="o", linewidth=2, label="Cold season"
    )

    # Transition Mar–Apr (keeps line continuous)
    ax.plot(x[2:4], y[2:4], color=WARM_COLOR, marker="o", linewidth=2)

    # Warm season: Apr–Oct
    ax.plot(
        x[3:10], y[3:10], color=WARM_COLOR, marker="o", linewidth=2, label="Warm season"
    )

    # Transition Oct–Nov
    ax.plot(x[9:11], y[9:11], color=COLD_COLOR, marker="o", linewidth=2)

    # Cold season: Nov–Dec
    ax.plot(x[10:], y[10:], color=COLD_COLOR, marker="o", linewidth=2)

    ax.set_xticks(
        ticks=range(1, 13),
        labels=[
            "Jan",
            "Feb",
            "Mar",
            "Apr",
            "May",
            "Jun",
            "Jul",
            "Aug",
            "Sep",
            "Oct",
            "Nov",
            "Dec",
        ],
    )

    ax.set_xlabel("Month")
    ax.set_ylabel("Monthly index $I_m$")
    ax.grid(alpha=0.3)

    if ylim is not None:
        ax.set_ylim(ylim)

    if show_metric:
        drop = seasonal_drop_index(Im)
        ax.set_title(
            f"Monthly profile – {station_name}\n" f"Warm–Cold drop = {drop:.2f}"
        )
    else:
        ax.set_title(f"Monthly profile – {station_name}")

    ax.legend(frameon=False)

    if created_fig:
        plt.tight_layout()
        plt.show()


def plot_monthly_indices_all(
    loader, channel="channels_all", interval=None, figsize=(8, 4), ylim=None, ax=None
):
    created_fig = False
    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)
        created_fig = True

    Im_all = []

    
    for station in loader.get_bicyle_stations():
        Im = monthly_index(loader, station, channel, interval=interval).sort("month")
        Im_all.append(Im)

        x = Im["month"].to_numpy()
        y = Im["I_m"].to_numpy()

        # Cold season: Jan–Mar
        ax.plot(x[:3], y[:3], color=COLD_COLOR, alpha=0.25, linewidth=1)
        # Transition Mar–Apr
        ax.plot(x[2:4], y[2:4], color=WARM_COLOR, alpha=0.25, linewidth=1)
        # Warm season: Apr–Oct
        ax.plot(x[3:10], y[3:10], color=WARM_COLOR, alpha=0.25, linewidth=1)
        # Transition Oct–Nov
        ax.plot(x[9:11], y[9:11], color=COLD_COLOR, alpha=0.25, linewidth=1)
        # Cold season: Nov–Dec
        ax.plot(x[10:], y[10:], color=COLD_COLOR, alpha=0.25, linewidth=1)

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
    ax.plot(x[:3], y[:3], color=COLD_COLOR, linewidth=3, label="Cold season (mean)")
    # Transition Mar–Apr
    ax.plot(x[2:4], y[2:4], color=WARM_COLOR, linewidth=3)
    # Warm season mean
    ax.plot(x[3:10], y[3:10], color=WARM_COLOR, linewidth=3, label="Warm season (mean)")
    # Transition Oct–Nov
    ax.plot(x[9:11], y[9:11], color=COLD_COLOR, linewidth=3)
    # Cold season mean
    ax.plot(x[10:], y[10:], color=COLD_COLOR, linewidth=3)

    ax.set_xticks(
        ticks=range(1, 13),
        labels=[
            "Jan",
            "Feb",
            "Mar",
            "Apr",
            "May",
            "Jun",
            "Jul",
            "Aug",
            "Sep",
            "Oct",
            "Nov",
            "Dec",
        ],
    )

    ax.set_xlabel("Month")
    ax.set_ylabel("Monthly index $I_m$")
    ax.set_title("Monthly traffic indices across all stations")
    ax.grid(alpha=0.3)

    if ylim is not None:
        ax.set_ylim(ylim)

    ax.legend(frameon=False)

    if created_fig:
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
    point_size=80,
):

    label_map = {0: "Cluster 0", 1: "Cluster 1", 2: "Cluster 2"}

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
            linewidths=0.6,
        )

    if annotate:
        for row in df.iter_rows(named=True):
            plt.text(
                row[pc1] + 0.01, row[pc2] + 0.01, row[label_col], fontsize=8, alpha=0.8
            )

    plt.xlabel(pc1)
    plt.ylabel(pc2)
    plt.title("PCA of station features")
    plt.legend(frameon=True)
    plt.grid(alpha=0.25)
    plt.tight_layout()
    plt.show()


def plot_feature_boxplots(
    df, features, cluster_col="cluster", clusters=(0, 1), figsize=(4, 3)
):
    df_filt = df.filter(pl.col(cluster_col).is_in(clusters))

    for feat in features:
        plt.figure(figsize=figsize)
        sns.boxplot(data=df_filt, x=cluster_col, y=feat)
        plt.title(feat)
        plt.tight_layout()
        plt.show()


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

    plt.figure(figsize=(6, max(3, figsize_scale * len(stations))))

    plt.errorbar(
        p,
        y,
        xerr=[p - lo, hi - p],
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



def plot_holiday_impact(delta_labeled, color_map=None, show_centers=True):
    if color_map is None:
        color_map = {
            "utilitarian": "red",
            "mixed": "orange",
            "recreational": "green",
        }

    has_dpi = "ΔDPI" in delta_labeled.columns
    has_wsd = "ΔWSD" in delta_labeled.columns

    if not (has_dpi or has_wsd):
        raise ValueError("No holiday delta features available for plotting")

    plt.figure(figsize=(6, 5))

    for (utype,), df_u in delta_labeled.group_by("usage_type"):
        x = df_u["ΔDPI"] if has_dpi else np.zeros(df_u.height)
        y = df_u["ΔWSD"] if has_wsd else np.zeros(df_u.height)

        plt.scatter(
            x,
            y,
            label=utype,
            color=color_map.get(utype, "gray"),
            alpha=0.8,
            edgecolors="black",
            s=80,
        )

    if show_centers:
        agg_exprs = []
        if has_dpi:
            agg_exprs.append(pl.median("ΔDPI").alias("x"))
        else:
            agg_exprs.append(pl.lit(0.0).alias("x"))

        if has_wsd:
            agg_exprs.append(pl.median("ΔWSD").alias("y"))
        else:
            agg_exprs.append(pl.lit(0.0).alias("y"))

        centers = (
            delta_labeled
            .group_by("usage_type")
            .agg(agg_exprs)
        )

        for row in centers.iter_rows(named=True):
            utype = row["usage_type"]
            plt.scatter(
                row["x"],
                row["y"],
                marker="X",
                s=200,
                color=color_map.get(utype, "gray"),
                edgecolors="black",
                linewidths=1.5,
                zorder=5,
            )

    xlabel = "ΔDPI (change in weekday double-peak structure)" if has_dpi else ""
    ylabel = "ΔWSD (change in weekday–weekend difference)" if has_wsd else ""

    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title("Impact of public holidays on station dominant usage patterns")

    handles, labels = plt.gca().get_legend_handles_labels()
    handles.append(Line2D(
        [0], [0],
        marker="X",
        color="w",
        label="Median center",
        markerfacecolor="gray",
        markeredgecolor="black",
        markersize=10,
        linewidth=0
    ))
    plt.legend(handles=handles, frameon=False)

    plt.grid(alpha=0.2)
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