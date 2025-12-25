import polars as pl
import numpy as np
from scipy.stats import norm

def find_peak(Ih, hour_min, hour_max):
    w = Ih.filter((pl.col("hour") >= hour_min) & (pl.col("hour") < hour_max))
    if w.height == 0:
        return None, None
    row = w.sort("I_h", descending=True).row(0)
    return row[w.columns.index("hour")], row[w.columns.index("I_h")]


def wilson_ci(k, n, alpha=0.05):
    z = norm.ppf(1 - alpha / 2)
    p = k / n

    denom = 1 + z**2 / n
    center = (p + z**2 / (2 * n)) / denom
    margin = z * np.sqrt((p * (1 - p) + z**2 / (4 * n)) / n) / denom

    return center - margin, center + margin


def impact_by_usage(delta_labeled):
    agg_exprs = []

    if "ΔDPI" in delta_labeled.columns:
        agg_exprs.append(
            pl.median("ΔDPI").alias("ΔDPI_median")
            #pl.quantile("DPI", 0.25).alias("ΔDPI_q25"),
            #pl.quantile("DPI", 0.75).alias("ΔDPI_q75"),
        )

    if "ΔWSD" in delta_labeled.columns:
        agg_exprs.append(
            pl.median("ΔWSD").alias("ΔWSD_median")
            #pl.quantile("WSD", 0.25).alias("ΔWSD_q25"),
            #pl.quantile("WSD", 0.75).alias("ΔWSD_q75"),
        )

    if not agg_exprs:
        return pl.DataFrame()

    return (
        delta_labeled
        .group_by("usage_type")
        .agg(agg_exprs)
    )



def dominant_usage_per_station(usage_probs):
    return (
        usage_probs
        .sort("probability", descending=True)
        .group_by("station")
        .head(1)
        .select(["station", "usage_type", "probability"])
    )


def label_deltas_with_usage(delta_df, usage_probs):
    dom = dominant_usage_per_station(usage_probs)

    return (
        delta_df
        .join(dom, on="station", how="left")
    )



def entropy(usage_probs):
    return (
        usage_probs
        .with_columns(
            (-pl.col("probability") * pl.col("probability").log())
            .alias("entropy_term")
        )
        .group_by("station")
        .agg(pl.sum("entropy_term").alias("entropy"))
        .sort("entropy")
    )
