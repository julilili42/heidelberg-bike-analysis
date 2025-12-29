import polars as pl
import numpy as np
from scipy.stats import norm
from scipy.interpolate import UnivariateSpline
from sklearn.model_selection import KFold
from sklearn.metrics import mean_squared_error

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


def fit_optimal_spline(
    x,
    y,
    s_values=None,
    k=3,
    n_splits=5,
):
    order = np.argsort(x)
    x = x[order]
    y = y[order]

    if s_values is None:
        s_values = np.logspace(-4, 1, 40) * len(x)

    best_s = None
    best_mse = np.inf

    kf = KFold(n_splits=n_splits, shuffle=True, random_state=0)

    for s in s_values:
        mses = []

        for train, test in kf.split(x):
            spline = UnivariateSpline(x[train], y[train], s=s, k=k)
            y_pred = spline(x[test])
            mses.append(mean_squared_error(y[test], y_pred))

        mse = np.mean(mses)

        if mse < best_mse:
            best_mse = mse
            best_s = s

    spline = UnivariateSpline(x, y, s=best_s, k=k)

    return spline, best_s, best_mse

# get the absolute and relative failure rate with start and end date 
# (to validate the output with Martin's Schaubild)
def station_outage_rate(dl):
    rows = []

    for station in dl.get_bicyle_stations():
        df = dl.get_bicycle(station_name=station,sample_rate="1h").df

        if df.is_empty():
            continue
        
        dt = (df.select(pl.col("datetime")).unique().sort("datetime"))
        start = dt.select(pl.col("datetime").min()).item()
        end   = dt.select(pl.col("datetime").max()).item()
        
        expected = pl.datetime_range(start=start,end=end,interval="1h",eager=True)
        
        missing_hours = (pl.DataFrame({"datetime": expected}).join(dt, on="datetime", how="anti").height)
        
        total_expected = len(expected)
        rows.append({
            "station": station,
            "start": start.year,
            "end": end.year,
            "expected_hours": total_expected,
            "missing_hours": missing_hours,
            "outage_rate": missing_hours / total_expected,
        })

    return pl.DataFrame(rows).sort("outage_rate", descending=True)