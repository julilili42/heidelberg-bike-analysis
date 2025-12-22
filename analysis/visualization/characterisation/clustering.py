import numpy as np
import polars as pl
from datetime import date
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from scipy.optimize import linear_sum_assignment  
from analysis.visualization.characterisation.features import build_feature_df
from sklearn.metrics import adjusted_rand_score
from dateutil.relativedelta import relativedelta


DATASET_START = "2016-01-01"
DATASET_END = "2024-01-01"
TIME_SERIES_MODE = "cumulative"


def make_interval(end_date, mode, window_months, dataset_start):
    if mode == "cumulative":
        return (dataset_start, end_date.isoformat())
    elif mode == "sliding":
        start = end_date - relativedelta(months=window_months)
        return (start.isoformat(), end_date.isoformat())
    else:
        raise ValueError("mode must be 'cumulative' or 'sliding'")
    

def cluster_until_with_centroids(loader, end_date, k=3, min_stations=5, mode=TIME_SERIES_MODE, window_months=24, dataset_start=DATASET_START):
    interval = make_interval(
        end_date,
        mode,
        window_months,
        dataset_start
    )

    X = build_feature_df(loader, interval)
    X_valid = X.filter(pl.col("valid") == True)

    if X_valid.height < max(k, min_stations):
        return None

    X_feat = X_valid.drop(["station", "valid"]).to_numpy()
    X_scaled = StandardScaler().fit_transform(X_feat)

    km = KMeans(n_clusters=k, random_state=0, n_init=20)
    labels = km.fit_predict(X_scaled)

    centroids = km.cluster_centers_

    df = X_valid.with_columns(
        pl.Series("cluster", labels, dtype=pl.Int32)
    )

    return df, centroids


def monthly_dates(start=DATASET_START, end=DATASET_END):
    return pl.date_range(
        start=date.fromisoformat(start),
        end=date.fromisoformat(end),
        interval="1mo",
        eager=True
    )


def match_labels(prev_centroids, cur_centroids):
    cost = np.linalg.norm(
        prev_centroids[:, None, :] - cur_centroids[None, :, :],
        axis=2
    )
    row_ind, col_ind = linear_sum_assignment(cost)
    return {cur: prev for prev, cur in zip(row_ind, col_ind)}


def cluster_timeseries_aligned(loader, k=3, start=DATASET_START, end=DATASET_END, mode=TIME_SERIES_MODE, window_months=24):
    dates = monthly_dates(start=start, end=end)

    rows = []
    prev_centroids = None

    for d in dates:
        out = cluster_until_with_centroids(
            loader,
            d,
            k=k,
            mode=mode,
            window_months=window_months
        )
        if out is None:
            continue

        df, centroids = out

        if prev_centroids is None:
            aligned = df.with_columns(pl.lit(d).alias("date"))
            prev_centroids = centroids
        else:
            mapping = match_labels(prev_centroids, centroids)

            aligned = df.with_columns([
                pl.col("cluster")
                  .map_elements(lambda x: mapping.get(x, x), return_dtype=pl.Int32)
                  .alias("cluster"),
                pl.lit(d).alias("date")
            ])

            new_centroids = np.zeros_like(centroids)
            for cur, prev in mapping.items():
                new_centroids[prev] = centroids[cur]
            prev_centroids = new_centroids

        rows.append(aligned.select(["station", "date", "cluster"]))

    if not rows:
        return pl.DataFrame(
            schema={
                "station": pl.Utf8,
                "date": pl.Date,
                "cluster": pl.Int32
            }
        )

    return pl.concat(rows).sort(["station", "date"])


def compute_cluster_means(df, features, cluster_col="cluster"):
    return (
        df.filter(pl.col("valid") == True)
          .group_by(cluster_col)
          .agg([pl.mean(f).alias(f) for f in features])
    )



def zscore_columns(df, cols):
    return df.with_columns([
        (pl.col(c) - pl.col(c).mean()) / pl.col(c).std()
        for c in cols
    ])



def compute_utilitarian_score(
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
