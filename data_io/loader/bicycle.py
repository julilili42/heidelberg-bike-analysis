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
    
    def drop(self, columns: list[str]):
        df = self.df.drop(columns)
        return BicycleData(df, self.station)
    
    def min_date(self):
        return self.df.select(pl.col("datetime").min()).item()

    def max_date(self):
        return self.df.select(pl.col("datetime").max()).item()

    def date_range(self, readable=False):
        if readable:
            return self.min_date().strftime("%Y-%m-%d"), self.max_date().strftime("%Y-%m-%d")
        return self.min_date(), self.max_date()

    def min_count(self, column="channels_all"):
        return self.df.select(pl.col(column).min()).item()

    def max_count(self, column="channels_all"):
        return self.df.select(pl.col(column).max()).item()

    def count_range(self, column="channels_all"):
        return self.min_count(column), self.max_count(column)

    def filter_time(
        self,
        weekday = None,   # True=Mo–Fr, False=Sa–So
        time_frame = (0, 24)
    ):
        df = self.df
        hour_min, hour_max = time_frame

        if weekday is not None:
            if weekday:
                df = df.filter(pl.col("datetime").dt.weekday() < 5)
            else:
                df = df.filter(pl.col("datetime").dt.weekday() >= 5)

        df = df.filter(
            (pl.col("datetime").dt.hour() >= hour_min) &
            (pl.col("datetime").dt.hour() < hour_max)
        )

        return self.new(df)
    
    def filter_intervals(self, intervals, negate=False):
        expr = None
        
        for start, end in intervals:
            cond = (
                (pl.col("datetime").dt.date() >= pl.lit(start).cast(pl.Date)) &
                (pl.col("datetime").dt.date() <= pl.lit(end).cast(pl.Date))
            )
            expr = cond if expr is None else expr | cond
        
        if expr is None:
            expr = pl.lit(False)
            
        if negate:
            expr = ~expr
        
        df = self.df.filter(expr)
        return self.new(df)
        
    
