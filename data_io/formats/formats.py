import polars as pl


WEATHER_FORMAT = {
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
    "datetime": pl.String,
    "year": pl.Int32,
    "month": pl.Int32,
    "day": pl.Int32,
    "hour": pl.Int32,
    "weekday": pl.Int32,
    "day_of_year": pl.Int32,
    "weather_description": pl.String,
}


BICYCLE_FORMAT = {
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


ACCIDENT_FORMAT = {
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
    "LINREFX": pl.Float64,
    "LINREFY": pl.Float64,
    "latitude": pl.Float64,
    "longitude": pl.Float64
}

