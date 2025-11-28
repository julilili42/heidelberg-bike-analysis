# Old file, consider deleting it

import polars as pl


def get_counter_data_schema():
    return {
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


def get_weather_data_schema():
    """
    Schema for weather data from Open-Meteo API.
    Ensures consistent data types across all weather datasets.
    """
    return {
        "timestamp": pl.String,
        "temperature_2m": pl.Float64,
        "relative_humidity_2m": pl.Float64,
        "precipitation": pl.Float64,
        "rain": pl.Float64,
        "snowfall": pl.Float64,
        "weather_code": pl.Int32,
        "cloud_cover": pl.Float64,
        "wind_speed_10m": pl.Float64,
        "wind_direction_10m": pl.Float64,
        "wind_gusts_10m": pl.Float64,
    }


def decode_weather_code(code: int) -> str:
    """
    Decode WMO Weather interpretation codes to human-readable descriptions.

    Args:
        code: WMO weather code (0-99)

    Returns:
        String description of weather condition

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
        99: "Thunderstorm with heavy hail",
    }
    return weather_codes.get(code, f"Unknown ({code})")


def categorize_weather(code: int) -> str:
    """
    Categorize weather codes into broader categories for analysis.

    Args:
        code: WMO weather code

    Returns:
        Category string: 'Clear', 'Cloudy', 'Rain', 'Snow', 'Fog', 'Thunderstorm'
    """
    if code == 0:
        return "Clear"
    elif code in [1, 2]:
        return "Partly Cloudy"
    elif code == 3:
        return "Cloudy"
    elif code in [45, 48]:
        return "Fog"
    elif code in [51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82]:
        return "Rain"
    elif code in [71, 73, 75, 77, 85, 86]:
        return "Snow"
    elif code in [95, 96, 99]:
        return "Thunderstorm"
    else:
        return "Other"


def get_accident_data_schema():
    """
    Schema for accident data from Unfallatlas.
    Ensures consistent data types across all accident datasets.
    """
    return {
        "state": pl.Int32,
        "region": pl.Int32,
        "district": pl.Int32,
        "municipality": pl.Int32,
        "year": pl.Int32,
        "month": pl.Int32,
        "weekday": pl.Int32,
        "hour": pl.Int32,
        "accident_type": pl.Int32,
        "injury_severity": pl.Int32,
        "light_condition": pl.Int32,
        "road_condition": pl.Int32,
        "is_bicycle": pl.Int32,
        "is_car": pl.Int32,
        "is_pedestrian": pl.Int32,
        "is_motorcycle": pl.Int32,
        "is_other": pl.Int32,
        "latitude": pl.Float64,
        "longitude": pl.Float64,
    }
