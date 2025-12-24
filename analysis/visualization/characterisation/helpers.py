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
