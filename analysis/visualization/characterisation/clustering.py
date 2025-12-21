import numpy as np
import polars as pl
from datetime import date
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from scipy.optimize import linear_sum_assignment  
from analysis.visualization.characterisation.features import build_feature_df


DATASET_START = "2016-01-01"

def cluster_until_with_centroids(loader, end_date, k=3, min_stations=5):
    interval = (DATASET_START, end_date.isoformat())

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


def monthly_dates(start="2016-01-01", end="2024-01-01"):
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


def cumulative_cluster_timeseries_aligned(
    loader, k=3, start="2016-01-01", end="2024-01-01"
):
    dates = monthly_dates(start=start, end=end)

    rows = []
    prev_centroids = None

    for d in dates:
        out = cluster_until_with_centroids(loader, d, k=k)
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

            # reorder centroids to aligned label space
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
