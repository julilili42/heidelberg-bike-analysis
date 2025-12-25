from data_io.loader.base import BaseData
import polars as pl

class WeatherData(BaseData):
    def new(self, df):
        return WeatherData(df)
    
    def resample(self, rate: str):
        df = (
            self.df.sort("datetime")
            .group_by_dynamic("datetime", every=rate)
            .agg([
                pl.col("temperature_2m").mean(),
                pl.col("relative_humidity_2m").mean(),
                pl.col("precipitation").sum(),
                pl.col("rain").sum(),
                pl.col("snowfall").sum(),
                pl.col("cloud_cover").mean(),
                pl.col("wind_speed_10m").mean(),
                pl.col("wind_direction_10m").mean(),
                pl.col("wind_gusts_10m").mean(),
            ])
        )
        return WeatherData(df)
    

    def get_intervals(self, condition):
        df = (
            self.df
            .with_columns(pl.col("datetime").dt.date().alias("date"))
            .filter(condition)
            .select("date")
            .unique()
            .sort("date")
        )

        dates = df["date"].to_list()
        if not dates:
            return []

        intervals = []
        start = prev = dates[0]

        for d in dates[1:]:
            if (d - prev).days == 1:
                prev = d
            else:
                intervals.append((start.isoformat(), prev.isoformat()))
                start = prev = d

        intervals.append((start.isoformat(), prev.isoformat()))
        return intervals
    