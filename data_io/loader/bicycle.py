from data_io.loader.base import BaseData
import polars as pl

class BicycleData(BaseData):
    def __init__(self, df: pl.DataFrame, station_name: str):
        super().__init__(df)
        self.station = station_name

    def new(self, df):
        return BicycleData(df, self.station)
    
    def resample(self, rate: str):
        df = (
            self.df.sort("datetime")
            .group_by_dynamic("datetime", every=rate)
            .agg([
                pl.col("channels_in").sum(),
                pl.col("channels_out").sum(),
                pl.col("channels_all").sum(),
                pl.col("channels_unknown").sum(),
                pl.col("site_temperature").mean(),
                pl.col("site_rain_accumulation").sum(),
                pl.col("site_snow_accumulation").sum(),
            ])
        )
        return BicycleData(df, self.station)    
    
    def min_count(self, column="channels_all"):
        return self.df.select(pl.col(column).min()).item()

    def max_count(self, column="channels_all"):
        return self.df.select(pl.col(column).max()).item()

    def count_range(self, column="channels_all"):
        return self.min_count(column), self.max_count(column)


