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