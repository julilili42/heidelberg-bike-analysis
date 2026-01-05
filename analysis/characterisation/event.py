import polars as pl
from analysis.characterisation.features import calc_feature_vector

# event = {holiday, weather}


def get_baseline_event_features(
    loader,
    station,
    event_intervals=None,
    baseline_negate=True,
):
    base = calc_feature_vector(
        loader=loader,
        station_name=station,
        neg_dates=baseline_negate,
        filter_dates=event_intervals,
    )
    event = calc_feature_vector(
        loader=loader,
        station_name=station,
        filter_dates=event_intervals,
    )

    if base is None or event is None:
        return None, None

    return base, event


def compute_event_deltas(loader, intervals):
    rows = []

    for station in loader.get_bicyle_stations():
        base, event = get_baseline_event_features(
            loader,
            station,
            event_intervals=intervals,
        )

        if base is None or event is None:
            continue

        U_base = utilitarian_score(base)
        U_event  = utilitarian_score(event)

        rows.append({
            "station": station,
            
            "U_base": U_base,
            "U_event": U_event,
            "U_delta": U_event - U_base,

            "DPI_delta": event["DPI"] - base["DPI"],
            "WSD_delta": event["WSD"] - base["WSD"],
            "SDI_delta": event["SDI"] - base["SDI"],  
        })

    return pl.DataFrame(rows)




def event_effect_table(
    df,
    range_col,
    baseline_label="L",
    group_cols=("station", "usage_type"),
    agg_cols=("usage_type",),
):
    station_means = (
        df
        .group_by([*group_cols, range_col])
        .agg(pl.mean("count").alias("mean_count"))
    )

    baseline = (
        station_means
        .filter(pl.col(range_col) == baseline_label)
        .select([
            *group_cols,
            pl.col("mean_count").alias("baseline_count")
        ])
    )

    station_rel = (
        station_means
        .join(baseline, on=list(group_cols), how="inner")
        .with_columns(
            ((pl.col("mean_count") - pl.col("baseline_count"))
             / pl.col("baseline_count"))
            .alias("rel_diff")
        )
    )

    final_agg = (
        station_rel
        .group_by([*agg_cols, range_col])
        .agg([
            pl.median("mean_count").alias("mean_count"),
            pl.median("rel_diff").alias("rel_diff"),
        ])
    )

    final_table = (
        final_agg
        .pivot(
            index=list(agg_cols),
            columns=range_col,
            values=["mean_count", "rel_diff"]
        )
        .sort(list(agg_cols))
    )

    final_table = final_table.with_columns([
        (pl.col(c) * 100).round(2).alias(c)
        for c in final_table.columns
        if c.startswith("rel_diff_")
    ])

    return final_table


def utilitarian_score(feats):
    return feats["DPI"] + feats["WSD"] - feats["SDI"]