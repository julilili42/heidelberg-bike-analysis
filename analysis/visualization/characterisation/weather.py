import polars as pl

def compute_weather_deltas(
    loader,
    event_intervals,
    weekday=None,
    hours=None,
    sample_rate="1h",
    min_obs=24,
    channel="channels_all",
):
    rows = []

    for station in loader.get_bicyle_stations():
        bd_event = (
            loader.get_bicycle(station, sample_rate=sample_rate)
            .filter_intervals(event_intervals)
        )

        bd_base = (
            loader.get_bicycle(station, sample_rate=sample_rate)
            .filter_intervals(event_intervals, negate=True)
        )

        if weekday is not None or hours is not None:
            bd_event = bd_event.filter_time(weekday=weekday, time_frame=hours)
            bd_base  = bd_base.filter_time(weekday=weekday, time_frame=hours)

        df_event = bd_event.df
        df_base  = bd_base.df

        if df_event.height < min_obs or df_base.height < min_obs:
            continue

        mean_event = df_event[channel].mean()
        mean_base  = df_base[channel].mean()

        rows.append({
            "station": station,
            "delta_relative": (mean_event - mean_base) / mean_base,
            "delta_absolute": mean_event - mean_base,
        })

    return pl.DataFrame(rows)





def weather_response_df(
    loader,
    sample_rate="1h",
    channel="channels_all",
    min_obs=500,
):
    rows = []

    for station in loader.get_bicyle_stations():
        df = loader.get_bicycle_with_weather(
            station_name=station,
            sample_rate=sample_rate
        )

        if df.height < min_obs:
            continue

        rows.append(
            df.select([
                pl.lit(station).alias("station"),
                "datetime",
                pl.col(channel).alias("count"),
                "temperature_2m",
                "precipitation",
                "wind_speed_10m",
            ])
        )

    return pl.concat(rows)


def weather_response_by_usage(
    df,
    var,                # "temperature_2m", "wind_speed_10m", "precipitation"
    bin_width,
    baseline_cond,      
    min_var=None,
    max_var=None,
    min_obs=100,
):
    if min_var is not None:
        df = df.filter(pl.col(var) >= min_var)
    if max_var is not None:
        df = df.filter(pl.col(var) <= max_var)

    # bins
    df = df.with_columns(
        ((pl.col(var) / bin_width).floor() * bin_width)
        .alias("var_bin")
    )

    baseline = (
        df
        .filter(baseline_cond)
        .group_by("station")
        .agg(pl.mean("count").alias("baseline_count"))
    )

    df = (
        df
        .join(baseline, on="station", how="left")
        .with_columns(
            (
                (pl.col("count") - pl.col("baseline_count"))
                / pl.col("baseline_count")
            ).alias("delta_rel")
        )
        .drop_nulls()
    )

    return (
        df
        .group_by(["usage_type", "var_bin"])
        .agg([
            pl.mean("delta_rel").alias("mean_delta"),
            pl.len().alias("n_obs"),
        ])
        .filter(pl.col("n_obs") >= min_obs)
        .sort(["usage_type", "var_bin"])
    )
