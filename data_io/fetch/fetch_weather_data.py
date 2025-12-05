import os
import requests
from pathlib import Path
import argparse
from tqdm import tqdm
import polars as pl
from datetime import date, timedelta
import time


def fetch_weather_data_for_year(year: int, start_month: int = 1, end_month: int = 12, start_day = 1, end_day = 31, latitude: float = 49.4093, longitude: float = 8.6942) -> pl.DataFrame:

    start_date = date(year, start_month, start_day)
    try:
        end_date = date(year, end_month, end_day)
    except ValueError:
        next_month = end_month % 12 + 1
        next_year = year + (end_month // 12)
        end_date = date(next_year, next_month, 1) - timedelta(days=1)

    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "rain",
            "snowfall",
            "weather_code",
            "cloud_cover",
            "wind_speed_10m",
            "wind_direction_10m",
            "wind_gusts_10m"
        ],
        "timezone": "Europe/Berlin"
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        hourly = data.get("hourly", {})

        df = pl.DataFrame({
            "timestamp": hourly.get("time", []),
            "temperature_2m": hourly.get("temperature_2m", []),
            "relative_humidity_2m": hourly.get("relative_humidity_2m", []),
            "precipitation": hourly.get("precipitation", []),
            "rain": hourly.get("rain", []),
            "snowfall": hourly.get("snowfall", []),
            "weather_code": hourly.get("weather_code", []),
            "cloud_cover": hourly.get("cloud_cover", []),
            "wind_speed_10m": hourly.get("wind_speed_10m", []),
            "wind_direction_10m": hourly.get("wind_direction_10m", []),
            "wind_gusts_10m": hourly.get("wind_gusts_10m", [])
        })

        return df

    except Exception as e:
        print(f"Error fetching data for {year}: {e}")
        return None



def decode_weather_code(code: int) -> str:
    weather_codes = {
        0: "Clear sky",
        1: "Mainly clear",
        2: "Partly cloudy",
        3: "Overcast",
        45: "Fog",
        48: "Depositing rime fog",
        51: "Drizzle: Light",
        53: "Drizzle: Moderate",
        55: "Drizzle: Dense",
        56: "Freezing Drizzle: Light",
        57: "Freezing Drizzle: Dense",
        61: "Rain: Slight",
        63: "Rain: Moderate",
        65: "Rain: Heavy",
        66: "Freezing Rain: Light",
        67: "Freezing Rain: Heavy",
        71: "Snowfall: Slight",
        73: "Snowfall: Moderate",
        75: "Snowfall: Heavy",
        77: "Snow grains",
        80: "Rain showers: Slight",
        81: "Rain showers: Moderate",
        82: "Rain showers: Violent",
        85: "Snow showers: Slight",
        86: "Snow showers: Heavy",
        95: "Thunderstorm: Moderate",
        96: "Thunderstorm: Light hail",
        99: "Thunderstorm: Heavy hail"
    }
    return weather_codes.get(code, f"Unknown ({code})")



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_year", default=2013, type=int)
    parser.add_argument("--end_year", default=2025, type=int)
    parser.add_argument("--folder", default="data/", type=str)

    args = parser.parse_args()

    out_folder = os.path.join(args.folder, "processed", "weather")
    Path(out_folder).mkdir(parents=True, exist_ok=True)

    for year in tqdm(range(args.start_year, args.end_year), desc="Downloading weather data"):
        df = fetch_weather_data_for_year(year)

        if df is None or len(df) == 0:
            print(f"No data for {year}")
            continue

        df = df.with_columns([
            pl.col("timestamp").str.to_datetime().alias("datetime"),
        ])

        df = df.with_columns([
            pl.col("datetime").dt.year().alias("year"),
            pl.col("datetime").dt.month().alias("month"),
            pl.col("datetime").dt.day().alias("day"),
            pl.col("datetime").dt.hour().alias("hour"),
            pl.col("datetime").dt.weekday().alias("weekday"),
            pl.col("datetime").dt.ordinal_day().alias("day_of_year"),
        ])

        df = df.with_columns([
            pl.col("weather_code").map_elements(decode_weather_code, return_dtype=pl.String).alias("weather_description")
        ])

        outfile = os.path.join(out_folder, f"weather_{year}.csv")
        df.write_csv(outfile)
