import os
import polars as pl
from data_io.formats.formats import (
    ACCIDENT_FORMAT,
    WEATHER_FORMAT,
    BICYCLE_FORMAT,
    HOLIDAYS_FORMAT,
)
from data_io.loader.bicycle import BicycleData
from data_io.loader.weather import WeatherData
from data_io.loader.accident import AccidentData
from data_io.loader.holidays import HolidaysData


class DataLoader:
    def __init__(self, city="Stadt_Heidelberg"):
        self.city = city

        self.bicycle_folder = f"./data/processed/cycle_counter/{city}/"
        self.weather_folder = f"./data/processed/weather/"
        self.accident_folder = f"./data/processed/accidents/"
        self.holidays_folder = f"./data/processed/holidays/"

        self.bicycle_data = {}
        self.weather_data = None
        self.accident_data = None
        self.holidays_data = None

        # This will trigger data loading
        self._load_bicycle()
        self._load_weather()
        self._load_accidents()
        self._load_holidays()

    def _load_weather(self):
        if not os.path.exists(self.weather_folder):
            print(f"Weather folder not found: {self.weather_folder}")
            return

        files = sorted(f for f in os.listdir(self.weather_folder) if f.endswith(".csv"))

        if not files:
            print("No weather csv files found")
            return

        dfs = []

        for file in files:
            path = os.path.join(self.weather_folder, file)
            df = pl.read_csv(path, schema=WEATHER_FORMAT)

            if df.schema["datetime"] == pl.String:
                df = df.with_columns(
                    pl.col("datetime")
                    .str.strptime(pl.Datetime, strict=False)
                    .dt.replace_time_zone("UTC")
                )

            dfs.append(df)

        # combine all weather files
        full_df = pl.concat(dfs).sort("datetime")

        self.weather_data = WeatherData(full_df)

    def get_weather(self, interval=None, sample_rate=None):
        wd = self.weather_data

        # datetime already includes all of this information
        wd = wd.drop(
            ["timestamp", "year", "month", "day", "hour", "weekday", "day_of_year"]
        )

        if interval:
            wd = wd.interval(interval[0], interval[1])
        if sample_rate:
            wd = wd.resample(sample_rate)
        return wd

    def get_weather_pandas(self, interval=None, sample_rate=None):
        df = self.get_weather(interval, sample_rate)
        return df.to_pandas()

    def get_bicycle_with_weather(self, station_name, interval=None, sample_rate="1h"):
        """
        Returns a combined bicycle + weather data joined on datetime.
        Join based on weather data, since it is more comprehensive.
        """
        bike = self.get_bicycle(
            station_name, interval=interval, sample_rate=sample_rate
        ).df

        weather = self.get_weather(interval=interval, sample_rate=sample_rate).df

        merged = weather.join(bike, on="datetime", how="left")

        return merged

    def _load_bicycle(self):
        files = os.listdir(self.bicycle_folder)
        self.csv_files = [f for f in files if f.endswith(".csv")]

        for file in self.csv_files:
            path = os.path.join(self.bicycle_folder, file)

            df = pl.read_csv(path, schema=BICYCLE_FORMAT)

            df = df.with_columns(
                pl.col("iso_timestamp")
                .str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%z", strict=False)
                .alias("datetime")
            )

            station_name = df["counter_site"][0]

            # BicycleData Objekt speichern
            self.bicycle_data[station_name] = BicycleData(df, station_name)

    def get_bicyle_stations(self):
        """
        Returns a list of readable station names
        """
        return list(self.bicycle_data.keys())

    def get_bicycle_location(self, station_name):
        bd = self.bicycle_data[station_name]
        lat, lon = bd.df.select(["latitude", "longitude"]).row(0)
        return lat, lon

    def get_bicycle(self, station_name, interval=None, sample_rate=None):
        bd = self.bicycle_data[station_name]

        bd = bd.drop(
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
            bd = bd.interval(interval[0], interval[1])

        if sample_rate is not None:
            bd = bd.resample(sample_rate)

        return bd

    def get_bicycle_pandas(self, station_name, interval=None, sample_rate=None):
        """
        Same as get_data but returns a pandas DataFrame
        """
        df = self.get_bicycle(station_name, interval, sample_rate)
        return df.to_pandas()

    def _load_accidents(self):
        if not os.path.exists(self.accident_folder):
            print(f"Accident folder not found: {self.accident_folder}")
            return

        files = sorted(
            f for f in os.listdir(self.accident_folder) if f.endswith(".csv")
        )

        if not files:
            print("No accident CSV files found.")
            return

        dfs = []

        for file in files:
            path = os.path.join(self.accident_folder, file)
            df = pl.read_csv(path, schema=ACCIDENT_FORMAT)

            # The dataset does not specify which exact day of the month => day is unknown
            # Set the day to 1 for each entry
            df = df.with_columns(
                [
                    pl.datetime(
                        pl.col("year"),
                        pl.col("month"),
                        1,  # Day unknown => set to 1
                        pl.col("hour"),
                    ).alias("datetime")
                ]
            )

            dfs.append(df)

        full_df = pl.concat(dfs).sort("datetime")
        self.accident_data = AccidentData(full_df)

    def get_accidents(self, interval=None, sample_rate=None):
        ad = self.accident_data

        if interval:
            ad = ad.interval(interval[0], interval[1])

        if sample_rate:
            ad = ad.resample(sample_rate)

        return ad

    def get_accidents_pandas(self, interval=None, sample_rate=None):
        df = self.get_accidents(interval, sample_rate)
        return df.to_pandas()

    def _load_holidays(self):
        if not os.path.exists(self.holidays_folder):
            print(f"Holidays folder not found: {self.holidays_folder}")
            return

        files = sorted(
            f for f in os.listdir(self.holidays_folder) if f.endswith(".csv")
        )

        if not files:
            print("No holidays CSV files found.")
            return

        # We only load: 'schulferien_holidays_bw'
        for file in files:
            print("Ja", file)
            if file.startswith("schulferien_holidays_bw"):
                path = os.path.join(self.holidays_folder, file)
                df = pl.read_csv(path, schema=HOLIDAYS_FORMAT).drop(["id", "type"])
                df = df.with_columns(
                    [
                        pl.col("start_date")
                        .str.strptime(pl.Date, "%Y-%m-%d")
                        .alias("start_date"),
                        pl.col("end_date")
                        .str.strptime(pl.Date, "%Y-%m-%d")
                        .alias("end_date"),
                    ]
                )

                self.holidays_data = HolidaysData(df)
                break

    # get_school_holidays('2023-08-01', '2023-08-20')
    def get_school_holidays(self, start, end):
        data = self.holidays_data.interval(start, end)
        data = data.df.filter(pl.col("is_school_vacation") == True)
        return data

    def get_public_holidays(self, start, end):
        data = self.holidays_data.interval(start, end)
        data = data.df.filter(pl.col("is_public_holiday") == True)
        return data

    # For single days
    def get_public_holiday(self, date):
        data = self.holidays_data.interval(date, date)
        data = data.df.filter(pl.col("is_public_holiday") == True)
        return data


# Example usage:
loader = DataLoader()

# print(loader.get_bicyle_stations())
# print(loader.get_bicycle("Mannheimer Straße", interval=("2022-01-01", "2023-01-01"), sample_rate="1y"))
# print(loader.get_bicycle_location("Mannheimer Straße"))

# print(loader.get_public_holiday("2023-10-03"))
# print(loader.get_school_holidays("2023-08-01", "2023-08-22"))
