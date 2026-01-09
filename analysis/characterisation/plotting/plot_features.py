import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestClassifier
import polars as pl
import seaborn as sns

def plot_feature_boxplots(
    df,
    features,
    cluster_col="cluster",
    clusters=(0, 1),
    figsize=(12, 3),
):
    df_filt = df.filter(pl.col(cluster_col).is_in(clusters))

    n = len(features)
    fig, axes = plt.subplots(1, n, figsize=figsize, sharey=False)

    if n == 1:
        axes = [axes]

    for ax, feat in zip(axes, features):
        sns.boxplot(data=df_filt, x=cluster_col, y=feat, ax=ax)
        ax.set_title(feat)
        ax.set_xlabel("")
        ax.grid(True, axis="y", alpha=0.3)

    plt.tight_layout()
    plt.show()

def plot_feature_space(
    X,
    features=("DPI", "WSD", "SDI"),
    cluster_col=None,
    color_map= {
        0: "#4C72B0",
        1: "#DD8452",
        2: "#55A868",
    },
    figsize=(6, 8),
    point_size=60,
    alpha=0.9,
    show_centroids=False,
):
    df = X.to_pandas() if hasattr(X, "to_pandas") else X.copy()

    if cluster_col is not None:
        if color_map is None:
            raise ValueError("color_map must be provided when cluster_col is used")
        colors = df[cluster_col].map(color_map)
    else:
        colors = "tab:blue"

    fig = plt.figure(figsize=figsize)
    ax = fig.add_subplot(111, projection="3d")

    ax.scatter(
        df[features[0]],
        df[features[1]],
        df[features[2]],
        c=colors,
        s=point_size,
        alpha=alpha,
    )

    if show_centroids and cluster_col is not None:
        centroids = (
            df.groupby(cluster_col)[list(features)]
            .mean()
        )
        ax.scatter(
            centroids[features[0]],
            centroids[features[1]],
            centroids[features[2]],
            c="black",
            s=220,
            marker="X",
            label="Centroids",
        )

    LABELPAD = 10

    ax.set_xlabel(features[0], labelpad=LABELPAD)
    ax.set_ylabel(features[1], labelpad=LABELPAD)
    ax.set_zlabel(features[2], labelpad=LABELPAD)

    ax.set_box_aspect(None, zoom=0.85)

    if cluster_col is not None:
        for k, col in color_map.items():
            ax.scatter([], [], [], c=col, label=f"Cluster {k}")
        if show_centroids:
            ax.legend(title="Cluster", frameon=False)
        else:
            ax.legend(title="Cluster", frameon=False)

    plt.tight_layout()
    plt.show()



def plot_rf_importance(
    df,
    features,
    target_col="cluster",
    n_estimators=500,
    random_state=0,
    class_weight="balanced",
    figsize=(6, 4),
):
    df_pd = df.filter(pl.col(target_col).is_not_null()).to_pandas()

    X = df_pd[features].values
    y = df_pd[target_col].values

    rf = RandomForestClassifier(
        n_estimators=n_estimators,
        random_state=random_state,
        class_weight=class_weight,
    )
    rf.fit(X, y)

    importances = rf.feature_importances_

    plt.figure(figsize=figsize)
    plt.bar(features, importances)
    plt.ylabel("Feature importance")
    plt.tight_layout()
    plt.show()