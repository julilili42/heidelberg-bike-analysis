import os
import polars as pl
from datetime import datetime, timezone

DATA_FORMAT = {
    "operator_name": pl.String,
    "domain_name": pl.String,
    "domain_id": pl.Int32,
    "counter_site": pl.String,
    "counter_site_id": pl.Int32,
    "counter_serial": pl.String,
    "longitude": pl.Float64,
    "latitude": pl.Float64,
    "timezone": pl.String,
    "iso_timestamp": pl.String,
    "channels_in": pl.Int32,
    "channels_out": pl.Int32,
    "channels_all": pl.Int32,
    "channels_unknown": pl.Int32,
    "site_temperature": pl.Float64,
    "site_rain_accumulation": pl.Float64,
    "site_snow_accumulation": pl.Float64,
}


class DataLoader:
    def __init__(self, city="Stadt_Heidelberg"):
        self.__city = city
        self.__folder = f"./data/processed/cycle_counter/{city}/"

        # station_name : file_name
        self.__station_names = {}

        # This will trigger loading data
        self.__load()

    def __load(self):
        files = os.listdir(self.__folder)
        self.stations = [os.path.splitext(f)[0] for f in files if f.endswith(".csv")]

        self.__station_data = {}

        for station in self.stations:
            df = pl.read_csv(
                source=f"{self.__folder}/{station}.csv", schema=DATA_FORMAT
            )
            df = df.with_columns(
                pl.col("iso_timestamp")
                .str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%z", strict=False)
                .alias("datetime")
            )

            station_name = df["counter_site"][0]

            self.__station_names[station_name] = station
            self.__station_data[station_name] = df

    def __get_file_names(self):
        """
        Returns a list of csv file names used
        """
        return self.stations

    def get_stations(self):
        """
        Returns a list of readable station names
        """
        return self.__station_names.keys()

    def get_location(self, station_name):
        """
        Returns a tuple (lat, long)
        """
        return self.__station_data[station_name][["latitude", "longitude"]].row(0)

    def get_data(self, station_name, interval=None, sample_rate=None):
        """
        Returns the data for a specific station, between the given interval and resampled to the given sample rate.
        interval: tuple of (start_date, end_date) in "YYYY-MM-DD" format
        sample_rate: string representing the sample rate, e.g. "1h", "1d", "1w" "1mo", "1y" etc.

        Example calls:
        data = loader.get_data("Mannheimer Straße", interval=("2022-01-01", "2022-01-02"), sample_rate="1h")
        data = loader.get_data("Mannheimer Straße", interval=("2022-01-01", "2023-01-01"), sample_rate="1y")

        Returns.
        A polars DataFrame with the data for the specified station.
        """
        df = self.__station_data[station_name].drop(
            [
                "operator_name",
                "domain_name",
                "domain_id",
                "counter_site_id",
                "counter_site",
                "counter_serial",
                "timezone",
                "iso_timestamp",
                "longitude",
                "latitude",
            ]
        )

        if interval is not None:
            start_date, end_date = interval

            start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )

            if start_dt == end_dt:
                raise ValueError(
                    "start_date must be before end_date, if you want data for a single day, set end_date to the next day."
                )

            df = df.filter(
                (pl.col("datetime") >= start_dt) & (pl.col("datetime") < end_dt)
            )

        if sample_rate is not None:
            df = df.sort("datetime")
            df = df.group_by_dynamic(
                index_column="datetime",
                every=sample_rate,
                closed="left",
                include_boundaries=False,
            ).agg(
                [
                    pl.col("channels_in").sum().alias("channels_in"),
                    pl.col("channels_out").sum().alias("channels_out"),
                    pl.col("channels_all").sum().alias("channels_all"),
                    pl.col("channels_unknown").sum().alias("channels_unknown"),
                    pl.col("site_temperature").mean().alias("site_temperature"),
                    pl.col("site_rain_accumulation")
                    .sum()
                    .alias("site_rain_accumulation"),
                    pl.col("site_snow_accumulation")
                    .sum()
                    .alias("site_snow_accumulation"),
                ]
            )

        # put datetime in front again
        df = df[["datetime"] + [col for col in df.columns if col != "datetime"]]

        return df


# Example usage:
# loader = DataLoader()

# print(loader.get_stations())
# print(loader.get_data("Mannheimer Straße", interval=("2022-01-01", "2023-01-01"), sample_rate="1y"))
# print(loader.get_location("Mannheimer Straße"))
