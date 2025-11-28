"""
This script downloads historical weather data for Heidelberg from Open-Meteo API.
Open-Meteo provides free historical weather data without requiring an API key.

Data includes:
- Temperature (2m above ground)
- Precipitation (rain)
- Wind speed
- Weather codes (clear, rain, snow, etc.)
- Cloud cover
- Relative humidity

Coordinates for Heidelberg: 49.4093°N, 8.6942°E
"""

import os
import requests
from pathlib import Path
import argparse
from tqdm import tqdm
import polars as pl
from datetime import datetime, timedelta
import time


def fetch_weather_data_for_year(year: int, latitude: float = 49.4093, longitude: float = 8.6942) -> pl.DataFrame:
    """
    Fetch hourly weather data for a specific year from Open-Meteo API.
    
    Args:
        year: The year to fetch data for
        latitude: Latitude of location (default: Heidelberg)
        longitude: Longitude of location (default: Heidelberg)
    
    Returns:
        Polars DataFrame with weather data
    """
    # Define date range for the year
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    
    # Open-Meteo Historical Weather API endpoint
    url = "https://archive-api.open-meteo.com/v1/archive"
    
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
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
        
        # Extract hourly data
        hourly = data.get("hourly", {})
        
        # Create DataFrame
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
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {year}: {e}")
        return None


def decode_weather_code(code: int) -> str:
    """
    Decode WMO Weather interpretation codes.
    
    Source: https://open-meteo.com/en/docs
    """
    weather_codes = {
        0: "Clear sky",
        1: "Mainly clear",
        2: "Partly cloudy",
        3: "Overcast",
        45: "Fog",
        48: "Depositing rime fog",
        51: "Drizzle: Light",
        53: "Drizzle: Moderate",
        55: "Drizzle: Dense intensity",
        56: "Freezing Drizzle: Light",
        57: "Freezing Drizzle: Dense",
        61: "Rain: Slight",
        63: "Rain: Moderate",
        65: "Rain: Heavy intensity",
        66: "Freezing Rain: Light",
        67: "Freezing Rain: Heavy",
        71: "Snow fall: Slight",
        73: "Snow fall: Moderate",
        75: "Snow fall: Heavy intensity",
        77: "Snow grains",
        80: "Rain showers: Slight",
        81: "Rain showers: Moderate",
        82: "Rain showers: Violent",
        85: "Snow showers: Slight",
        86: "Snow showers: Heavy",
        95: "Thunderstorm: Slight or moderate",
        96: "Thunderstorm with slight hail",
        99: "Thunderstorm with heavy hail"
    }
    return weather_codes.get(code, f"Unknown ({code})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download historical weather data for Heidelberg from Open-Meteo API."
    )
    parser.add_argument(
        "--start_year", 
        default=2013, 
        type=int, 
        help="Start year for data download (inclusive, available from 1940)."
    )
    parser.add_argument(
        "--end_year", 
        default=2025, 
        type=int, 
        help="End year for data download (inclusive)."
    )
    parser.add_argument(
        "--folder", 
        default="data/", 
        type=str, 
        help="Folder to save downloaded data."
    )
    parser.add_argument(
        "--latitude",
        default=49.4093,
        type=float,
        help="Latitude of location (default: Heidelberg)."
    )
    parser.add_argument(
        "--longitude",
        default=8.6942,
        type=float,
        help="Longitude of location (default: Heidelberg)."
    )
    parser.add_argument(
        "--combine",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Combine all years into a single file."
    )
    
    args = parser.parse_args()
    
    # Create output directories
    raw_folder = os.path.join(args.folder, "raw", "weather")
    processed_folder = os.path.join(args.folder, "processed", "weather")
    Path(raw_folder).mkdir(parents=True, exist_ok=True)
    Path(processed_folder).mkdir(parents=True, exist_ok=True)
    
    print(f"Downloading weather data for Heidelberg ({args.latitude}°N, {args.longitude}°E)")
    print(f"Period: {args.start_year} - {args.end_year}")
    print("Source: Open-Meteo Historical Weather API (https://open-meteo.com)")
    print()
    
    all_dataframes = []
    
    # Download data year by year
    for year in tqdm(range(args.start_year, args.end_year + 1), desc="Downloading weather data"):
        # Check if current year - only download up to yesterday
        if year == datetime.now().year:
            print(f"\nNote: {year} is the current year, downloading data up to yesterday...")
        
        df = fetch_weather_data_for_year(year, args.latitude, args.longitude)
        
        if df is not None and len(df) > 0:
            # Save individual year file
            output_file = os.path.join(raw_folder, f"weather_heidelberg_{year}.csv")
            df.write_csv(output_file)
            all_dataframes.append(df)
            
            # Be nice to the API - small delay between requests
            time.sleep(0.5)
        else:
            print(f"Warning: No data retrieved for {year}")
    
    print(f"\nDownloaded {len(all_dataframes)} years of data.")
    
    # Combine all data into one file
    if args.combine and all_dataframes:
        print("\nCombining all years into single dataset...")
        combined_df = pl.concat(all_dataframes)
        
        # Add additional derived columns for easier analysis
        combined_df = combined_df.with_columns([
            # Parse timestamp
            pl.col("timestamp").str.to_datetime().alias("datetime"),
        ])
        
        # Extract temporal features
        combined_df = combined_df.with_columns([
            pl.col("datetime").dt.year().alias("year"),
            pl.col("datetime").dt.month().alias("month"),
            pl.col("datetime").dt.day().alias("day"),
            pl.col("datetime").dt.hour().alias("hour"),
            pl.col("datetime").dt.weekday().alias("weekday"),
            pl.col("datetime").dt.ordinal_day().alias("day_of_year"),
        ])
        
        # Add weather description
        combined_df = combined_df.with_columns([
            pl.col("weather_code").map_elements(
                decode_weather_code, 
                return_dtype=pl.String
            ).alias("weather_description")
        ])
        
        # Save combined file
        combined_file = os.path.join(
            processed_folder, 
            f"weather_heidelberg_{args.start_year}_{args.end_year}_combined.csv"
        )
        combined_df.write_csv(combined_file)
        
        print(f"Combined data saved to: {combined_file}")
        print(f"Total records: {len(combined_df):,}")
        print(f"Date range: {combined_df['timestamp'].min()} to {combined_df['timestamp'].max()}")
        
        # Display basic statistics
        print("\n" + "="*60)
        print("WEATHER DATA STATISTICS")
        print("="*60)
        
        stats_df = combined_df.select([
            pl.col("temperature_2m").mean().alias("avg_temp"),
            pl.col("temperature_2m").min().alias("min_temp"),
            pl.col("temperature_2m").max().alias("max_temp"),
            pl.col("precipitation").sum().alias("total_precipitation_mm"),
            pl.col("precipitation").mean().alias("avg_hourly_precipitation"),
            pl.col("wind_speed_10m").mean().alias("avg_wind_speed_kmh"),
            pl.col("wind_speed_10m").max().alias("max_wind_speed_kmh"),
            pl.col("relative_humidity_2m").mean().alias("avg_humidity_percent"),
        ])
        
        print("\nTemperature (°C):")
        print(f"  Average: {stats_df['avg_temp'][0]:.1f}°C")
        print(f"  Min: {stats_df['min_temp'][0]:.1f}°C")
        print(f"  Max: {stats_df['max_temp'][0]:.1f}°C")
        
        print(f"\nPrecipitation:")
        print(f"  Total: {stats_df['total_precipitation_mm'][0]:.1f} mm")
        print(f"  Average hourly: {stats_df['avg_hourly_precipitation'][0]:.3f} mm")
        
        print(f"\nWind:")
        print(f"  Average speed: {stats_df['avg_wind_speed_kmh'][0]:.1f} km/h")
        print(f"  Max speed: {stats_df['max_wind_speed_kmh'][0]:.1f} km/h")
        
        print(f"\nHumidity:")
        print(f"  Average: {stats_df['avg_humidity_percent'][0]:.1f}%")
        
        # Most common weather conditions
        print(f"\nMost common weather conditions:")
        weather_counts = (
            combined_df
            .group_by("weather_description")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .head(10)
        )
        for row in weather_counts.iter_rows():
            description, count = row
            percentage = (count / len(combined_df)) * 100
            print(f"  {description}: {count:,} hours ({percentage:.1f}%)")
        
        print("\n" + "="*60)
    
    print("\n✓ Weather data download completed successfully!")
    print(f"\nRaw data location: {raw_folder}")
    if args.combine:
        print(f"Processed data location: {processed_folder}")
    
    print("\nYou can now use this data to analyze:")
    print("  • How weather conditions affect bicycle traffic volume")
    print("  • Correlation between precipitation and accidents")
    print("  • Temperature effects on cycling patterns")
    print("  • Seasonal weather variations")
