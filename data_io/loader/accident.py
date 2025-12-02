from data_io.loader.base import BaseData
import polars as pl

class AccidentData(BaseData):
    def new(self, df):
        return AccidentData(df)

    def interval(self, start, end):
        raise NotImplementedError("AccidentData has no real datetime")

    def resample(self, rate: str):
        raise NotImplementedError("AccidentData has no real datetime")
    
    def per_month(self):
        df = self.df.group_by(["year", "month"]).agg([
            pl.len().alias("num_accidents")
        ])
        return df

    def per_hour(self):
        df = self.df.group_by("hour").agg([
            pl.len().alias("num_accidents")
        ])
        return df

    def per_weekday(self):
        df = self.df.group_by("weekday").agg([
            pl.len().alias("num_accidents")
        ])
        return df
    
    def bicycle_only(self):
        return AccidentData(self.df.filter(pl.col("is_bicycle") == 1))

    def filter_region(self, state=None, region=None, district=None):
        df = self.df

        if state is not None:
            df = df.filter(pl.col("state") == state)

        if region is not None:
            df = df.filter(pl.col("region") == region)

        if district is not None:
            df = df.filter(pl.col("district") == district)

        return AccidentData(df)
    
    def map_points(self):
        rows = self.df.select(["latitude", "longitude", "year", "month"]).iter_rows()
        return rows