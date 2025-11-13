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