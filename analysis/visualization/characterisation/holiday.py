import polars as pl
from datetime import date
from analysis.visualization.characterisation.helpers import dominant_usage_per_station

def holiday_count_df(loader, usage_probs):
    rows = []

    intervals = loader.get_all_holiday_intervals(school_vacation=False)

    holiday_dates = []
    for start, end in intervals:
        holiday_dates.extend(
            pl.date_range(
                date.fromisoformat(start),
                date.fromisoformat(end),
                interval="1d",
                eager=True,
            ).to_list()
        )

    holiday_df = (
        pl.DataFrame({"date": holiday_dates})
        .unique()
        .with_columns(pl.lit("H").alias("holiday_flag"))
    )

    for station in loader.get_bicyle_stations():
        df = loader.get_bicycle(station, sample_rate="1d").df
        df = df.select(["datetime", "channels_all"]).with_columns(
            pl.lit(station).alias("station"),
            pl.col("datetime").dt.date().alias("date"),
        )
        rows.append(df)

    df = pl.concat(rows)

    df = df.join(
        dominant_usage_per_station(usage_probs),
        on="station",
        how="left"
    )

    df = (
        df.join(holiday_df, on="date", how="left")
          .with_columns(pl.col("holiday_flag").fill_null("L"))
          .rename({"channels_all": "count"})
    )

    return df
