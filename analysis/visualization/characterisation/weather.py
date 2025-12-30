import polars as pl

def weather_response_df(
    loader,
    sample_rate="1h",          
    channel="channels_all",
    min_obs=200,
):
    rows = []

    for station in loader.get_bicyle_stations():
        df = loader.get_bicycle_with_weather(
            station_name=station,
            sample_rate=sample_rate
        )

        if df.height < min_obs:
            continue

        df_day = (
            df
            .with_columns(pl.col("datetime").dt.date().alias("date"))
            .group_by("date")
            .agg([
                pl.col(channel).mean().alias("count"),            
                pl.col("temperature_2m").max().alias("temp_max"),     
                pl.col("precipitation").sum().alias("precip_sum"),
                pl.col("wind_speed_10m").max().alias("wind_max"),
            ])
            .filter(pl.col("count").is_not_null() & (pl.col("count") > 0))
            .with_columns(pl.col("date").cast(pl.Datetime).alias("datetime"))
            .select([
                pl.lit(station).alias("station"),
                "datetime",
                "count",
                pl.col("temp_max"),
                "precip_sum",
                "wind_max",
            ])
        )

        rows.append(df_day)

    return pl.concat(rows)



def reorder_lmh_columns(df):
    fixed = ["usage_type", "day_type"]

    mean_cols = [c for c in df.columns if c.startswith("mean_count_")]
    diff_cols = [c for c in df.columns if c.startswith("rel_diff_")]

    order = ["L", "M", "H"]

    mean_cols = sorted(mean_cols, key=lambda c: order.index(c.split("_")[-1]))
    diff_cols = sorted(diff_cols, key=lambda c: order.index(c.split("_")[-1]))

    return df.select(fixed + mean_cols + diff_cols)


def weather_effect_table(
    df,
    range_col,
    baseline_label="L",
):
    # mean count per station
    station_means = (
        df
        .group_by(["station", "usage_type", "day_type", range_col])
        .agg(pl.mean("count").alias("mean_count"))
    )

    # baseline per station
    baseline = (
        station_means
        .filter(pl.col(range_col) == baseline_label)
        .select([
            "station",
            "usage_type",
            "day_type",
            pl.col("mean_count").alias("baseline_count")
        ])
    )

    # relative change per station
    station_rel = (
        station_means
        .join(
            baseline,
            on=["station", "usage_type", "day_type"],
            how="inner"
        )
        .with_columns(
            ((pl.col("mean_count") - pl.col("baseline_count"))
             / pl.col("baseline_count"))
            .alias("rel_diff")
        )
    )

    # aggregate across stations
    final_agg = (
        station_rel
        .group_by(["usage_type", "day_type", range_col])
        .agg([
            pl.median("mean_count").alias("mean_count"),
            pl.median("rel_diff").alias("rel_diff"),
        ])
    )

    # pivot to final table
    final_table = (
        final_agg
        .pivot(
            index=["usage_type", "day_type"],
            columns=range_col,
            values=["mean_count", "rel_diff"]
        )
        .sort(["usage_type", "day_type"])
    )

    # rel_diff → %
    final_table = final_table.with_columns([
        (pl.col(c) * 100).round(2).alias(c)
        for c in final_table.columns
        if c.startswith("rel_diff_")
    ])

    final_table = reorder_lmh_columns(final_table)

    return final_table