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
                pl.col(channel).sum().alias("count"),            
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


