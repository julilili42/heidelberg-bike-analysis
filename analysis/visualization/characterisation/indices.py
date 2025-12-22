import polars as pl

def daily_mean_count(loader, station_name, interval=None):
    df = loader.get_bicycle(
        station_name,
        interval=interval,
        sample_rate="1d"
    ).df.mean()

    return df["channels_all"].item()


def hourly_index(loader, station_name, channel="channels_all", interval=None, weekday=None, filter_dates=None, neg_dates=False):
    df = loader.get_bicycle(
        station_name, interval=interval, sample_rate="1h"
    ).filter_time(weekday=weekday).filter_intervals(intervals=filter_dates, negate=neg_dates).df

    mean_C_24h = daily_mean_count(loader, station_name, interval)

    Ih = (
        df
        .with_columns([
            pl.col("datetime").dt.hour().alias("hour"),
        ])
        .group_by("hour")
        .agg(pl.mean(channel).alias("mean_C_1h"))
        .with_columns((pl.col("mean_C_1h") / mean_C_24h).alias("I_h"))
        .sort("hour")
    )
    return Ih


def daily_index(loader, station_name, channel="channels_all", interval=None, weekday=None, filter_dates=None, neg_dates=False):
    df = loader.get_bicycle(
        station_name,
        interval=interval,
        sample_rate="1d"
    ).filter_time(weekday=weekday).filter_intervals(intervals=filter_dates, negate=neg_dates).df

    mean_C_24h = daily_mean_count(loader, station_name, interval)

    Id = (
        df.with_columns([
            pl.col("datetime").dt.weekday().alias("weekday"),
        ])
        .group_by("weekday")
        .agg(pl.mean(channel).alias("mean_C_1d"))
        .with_columns((pl.col("mean_C_1d") / mean_C_24h).alias("I_d"))
        .sort("weekday")
    )
    return Id



def monthly_index(loader, station_name, channel="channels_all", interval=None, weekday=None):
    df = (
        loader.get_bicycle(
            station_name,
            interval=interval,
            sample_rate="1d"
        )
        .filter_time(weekday=weekday)
        .df
    )


    mean_C_24h = daily_mean_count(loader, station_name, interval)

    Im = (
        df
        .with_columns([
            pl.col("datetime").dt.month().alias("month"),
        ])
        .group_by("month")
        .agg(pl.mean(channel).alias("mean_C_1d"))
        .with_columns((pl.col("mean_C_1d") / mean_C_24h).alias("I_m"))
        .sort("month")
    )

    return Im
