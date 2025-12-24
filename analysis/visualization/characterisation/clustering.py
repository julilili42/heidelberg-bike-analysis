import numpy as np
import polars as pl
from datetime import date
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from analysis.visualization.characterisation.features import build_feature_df
from sklearn.metrics import adjusted_rand_score
from dateutil.relativedelta import relativedelta
from data_io.loader.data_loader import DataLoader
from analysis.visualization.characterisation.helpers import wilson_ci


dl = DataLoader()

def kmeans_core(features_valid, k):
    features = features_valid.drop(["station", "valid"]).to_numpy()

    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features)
    
    km = KMeans(n_clusters=k, random_state=0, n_init=20)

    labels = km.fit_predict(features_scaled)

    return labels, km.cluster_centers_



def kmeans_clustering(features, k):
    features_valid = features.filter(pl.col("valid") == True)

    labels, _ = kmeans_core(features_valid, k)

    features_valid = features_valid.with_columns(pl.Series("cluster", labels))

    return features.join(
        features_valid.select(["station", "cluster"]),
        on="station",
        how="left"
    )


def cluster_until_with_centroids(loader, k, start, end, mode, window_months, min_stations=5):
    interval = make_interval(
        start=start,
        end=end,
        mode=mode,
        window_months=window_months,
    )

    features = build_feature_df(loader, interval)
    features_valid = features.filter(pl.col("valid") == True)

    if features_valid.height < max(k, min_stations):
        return None

    labels, centroids = kmeans_core(features_valid, k)

    df = features_valid.with_columns(
        pl.Series("cluster", labels, dtype=pl.Int32)
    )

    return df, centroids



def cluster_timeseries_usage(loader, k, start, end, mode, window_months, features):
    dates = monthly_dates(start=start, end=end)
    rows = []

    for d in dates:
        out = cluster_until_with_centroids(
            loader=loader,
            k=k,
            mode=mode,
            window_months=window_months,
            start=start,
            end=d
        )
        if out is None:
            continue

        df, _ = out

        cluster_means = compute_cluster_means(
            df=df,
            features=features
        )
        cluster_means = zscore_columns(df=cluster_means, features=features)
        cluster_means = compute_utilitarian_score(df=cluster_means, features=features)

        cluster_labels = label_clusters_by_score(df=cluster_means)

        df_labeled = df.with_columns([
            pl.col("cluster")
              .map_elements(lambda c: cluster_labels.get(c))
              .alias("usage_type"),
            pl.lit(d).alias("date")
        ])

        rows.append(df_labeled.select(["station", "date", "usage_type"]))

    return pl.concat(rows) if rows else pl.DataFrame()



def monthly_dates(start, end):
    return pl.date_range(
        start=date.fromisoformat(start),
        end=date.fromisoformat(end),
        interval="1mo",
        eager=True
    )


def make_interval(start, end, mode, window_months):
    if mode == "cumulative":
        return (start, end.isoformat())
    elif mode == "sliding":
        start = end - relativedelta(months=window_months)
        return (start.isoformat(), end.isoformat())
    else:
        raise ValueError("mode must be 'cumulative' or 'sliding'")
    


def usage_probabilities(df_usage, alpha=0.05):
    return (
        df_usage
        .group_by(["station", "usage_type"])
        .agg(pl.len().alias("k"))
        .with_columns(
            pl.col("k").sum().over("station").alias("N")
        )
        .with_columns(
            (pl.col("k") / pl.col("N")).alias("probability")
        )
        .with_columns([
            pl.struct(["k", "N"])
              .map_elements(lambda x: wilson_ci(x["k"], x["N"], alpha)[0])
              .alias("ci_low"),
            pl.struct(["k", "N"])
              .map_elements(lambda x: wilson_ci(x["k"], x["N"], alpha)[1])
              .alias("ci_high"),
        ])
        .sort(["station", "usage_type"])
    )



def dominant_usage(df_probs):
    return (
        df_probs
        .sort("probability", descending=True)
        .group_by("station")
        .head(1)
    )



def compute_cluster_means(df, features, cluster_col="cluster"):
    return (
        df.group_by(cluster_col)
          .agg([pl.mean(f).alias(f) for f in features])
    )



def zscore_columns(df, features):
    return df.with_columns([
        (pl.col(c) - pl.col(c).mean()) / pl.col(c).std()
        for c in features
    ])



def compute_utilitarian_score(df, features, out_col="utilitarian_score"):
    expr = 0
    if "DPI" in features:
        expr += pl.col("DPI")
    if "WSD" in features:
        expr += pl.col("WSD")
    if "SDI" in features:
        expr -= pl.col("SDI")

    return df.with_columns(expr.alias(out_col))


def label_clusters_by_score(
    df,
    cluster_col="cluster",
    score_col="utilitarian_score"
):
    df = df.sort(score_col)

    k = df.height
    clusters = df[cluster_col].to_list()

    if k == 2:
        return {
            clusters[0]: "recreational",
            clusters[1]: "utilitarian",
        }
    
    if k == 3:
        return {
            clusters[0]: "recreational",
            clusters[1]: "mixed",
            clusters[2]: "utilitarian",
        }

    if k == 4:
        return {
            clusters[0]: "recreational",
            clusters[1]: "mixed recreational",
            clusters[2]: "mixed utilitarian",
            clusters[3]: "utilitarian",
        }

    raise ValueError(f"Unsupported k={k}")



def label_cluster_probabilities(cluster_probs, cluster_labels):
    return (
        cluster_probs
        .with_columns(
            pl.col("cluster")
              .map_elements(lambda c: cluster_labels.get(c))
              .alias("usage_type")
        )
    )



def add_utilitarian_score(
    df,
    dpi_col="DPI",
    shape_col="WSD",
    season_col="SDI",
    out_col="utilitarian_score"
):
    return df.with_columns(
        (
            pl.col(dpi_col)
            + pl.col(shape_col)
            - pl.col(season_col)
        ).alias(out_col)
    )




def select_top_stations_per_usage(
    cluster_probs_labeled,
    station_scores,
    n=2
):
    return (
        cluster_probs_labeled
        .join(
            station_scores.select(["station", "utilitarian_score"]),
            on="station",
            how="left"
        )
        .with_columns(
            pl.when(pl.col("usage_type") == "utilitarian")
              .then(pl.col("utilitarian_score"))
            .when(pl.col("usage_type") == "recreational")
              .then(-pl.col("utilitarian_score"))
            .when(pl.col("usage_type").str.starts_with("mixed"))
              .then(-pl.col("utilitarian_score").abs())
            .otherwise(0.0)
            .alias("ranking_score")
        )
        .sort(
            ["usage_type", "probability", "ranking_score"],
            descending=[False, True, True]
        )
        .group_by("usage_type")
        .head(n)
    )


def cluster_ari(df_a, df_b):
    joined = (
        df_a.select(["station", "cluster"])
        .join(df_b.select(["station", "cluster"]), on="station")
        .drop_nulls()
    )
    return adjusted_rand_score(
        joined["cluster"].to_numpy(),
        joined["cluster_right"].to_numpy()
    )