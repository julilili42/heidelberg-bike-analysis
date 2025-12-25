import polars as pl
from datetime import datetime, timezone

class BaseData:
    def __init__(self, df: pl.DataFrame):
        self.df = df

    def new(self, df):
        return self.__class__(df)

    def drop(self, columns: list[str]):
        df = self.df.drop(columns)
        return self.new(df)

    def interval(self, start: str, end: str):
        start_dt = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        df = self.df.filter(
            (pl.col("datetime") >= start_dt) &
            (pl.col("datetime") < end_dt)
        )
        return self.new(df)
    

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
        df = self.df

        if not intervals:
            return self.new(df)

        expr = None
        for start, end in intervals:
            cond = (
                (pl.col("datetime").dt.date() >= pl.lit(start).cast(pl.Date)) &
                (pl.col("datetime").dt.date() <= pl.lit(end).cast(pl.Date))
            )
            expr = cond if expr is None else expr | cond

        if negate:
            expr = ~expr

        return self.new(df.filter(expr))
    

    def min_date(self):
        return self.df.select(pl.col("datetime").min()).item()

    def max_date(self):
        return self.df.select(pl.col("datetime").max()).item()

    def date_range(self, readable=False):
        if readable:
            return self.min_date().strftime("%Y-%m-%d"), self.max_date().strftime("%Y-%m-%d")
        return self.min_date(), self.max_date()
    

    def to_pandas(self):
        return self.df.to_pandas()

    def resample(self, rate: str):
        raise NotImplementedError("resample must be implemented in subclasses")