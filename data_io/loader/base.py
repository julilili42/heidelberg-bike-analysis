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

    def to_pandas(self):
        return self.df.to_pandas()

    def resample(self, rate: str):
        raise NotImplementedError("resample must be implemented in subclasses")