from data_io.loader.base import BaseData
import polars as pl
from datetime import datetime, timezone


class HolidaysData(BaseData):
    def __init__(self, df: pl.DataFrame):
        super().__init__(df)

    def new(self, df):
        return HolidaysData(df)

    # end_date is ALWAYS included, 03.10 lasts from 03.10 to 03.10 !!!
    def interval(self, start: str, end: str):
        start_dt = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        df = self.df.filter(
            (pl.col("end_date") >= start_dt) & (pl.col("start_date") <= end_dt)
        ).sort("start_date", descending=False)
        return self.new(df)

    def drop(self, columns: list[str]):
        df = self.df.drop(columns)
        return HolidaysData(df)

    def resample(self, rate: str):
        raise NotImplementedError("Makes no sense for holidays")
