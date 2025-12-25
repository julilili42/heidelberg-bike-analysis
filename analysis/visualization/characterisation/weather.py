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
