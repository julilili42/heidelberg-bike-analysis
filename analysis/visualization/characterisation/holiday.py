import polars as pl
from analysis.visualization.characterisation.features import calc_feature_vector

def compute_holiday_deltas(loader):
    intervals = loader.get_all_holiday_intervals(school_vacation=False)

    rows = []

    for station in loader.get_bicyle_stations():
        baseline = calc_feature_vector(
            loader=loader,
            station_name=station,
            neg_dates=True
        )
        holiday = calc_feature_vector(
            loader=loader,
            station_name=station,
            filter_dates=intervals
        )

        if baseline is None or holiday is None:
            continue

        rows.append({
            "station": station,
            "ΔDPI": holiday["DPI"] - baseline["DPI"],
            "ΔWSD": holiday["WSD"] - baseline["WSD"],
        })

    return pl.DataFrame(rows)

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



def holiday_impact_by_usage(delta_labeled):
    return (
        delta_labeled
        .group_by("usage_type")
        .agg([
            pl.median("ΔDPI").alias("ΔDPI_median"),
            #pl.quantile("DPI", 0.25).alias("ΔDPI_q25"),
            #pl.quantile("DPI", 0.75).alias("ΔDPI_q75"),
            pl.median("ΔWSD").alias("ΔWSD_median"),
            #pl.quantile("WSD", 0.25).alias("ΔWSD_q25"),
            #pl.quantile("WSD", 0.75).alias("ΔWSD_q75"),
        ])
    )

