# This is a script to automate the data downloading process from https://mobidata-bw.de/fahrradzaehldaten/v2/ where we download bicycle counting data for specified years and months.

import os
import requests
from pathlib import Path
import argparse
import gzip
from tqdm import tqdm
import polars as pl
import shutil


if __name__ == "__main__":
    # command line interface for start and end year
    parser = argparse.ArgumentParser(description="Download bicycle counting data.")
    parser.add_argument("--start_year", default=2013, type=int, help="Start year for data download (inclusive).")
    parser.add_argument("--end_year", default=2025, type=int, help="End year for data download (inclusive).")
    # add the folder argument
    parser.add_argument("--folder", default="data/", type=str, help="Folder to save downloaded data.")
    parser.add_argument("--keep-raw", default=False, action='store_true', help="Whether to keep raw downloaded zip files.")
    args = parser.parse_args()

    BASE_URL = "https://mobidata-bw.de/fahrradzaehldaten/v2/"

    start_year = args.start_year
    end_year = args.end_year
    folder = args.folder

    raw_folder = os.path.join(folder, "raw", "cycle_counter")

    # create the folder if it does not exist
    Path(raw_folder).mkdir(parents=True, exist_ok=True)

    # The request looks like https://mobidata-bw.de/fahrradzaehldaten/v2/fahrradzaehler_stundenwerten_202101.csv.gz where 202101 is year and month
    for year in tqdm(range(start_year, end_year + 1), desc="Downloading..."):
        for month in range(1, 13):
            date_str = f"{year}{month:02d}"
            file_name = f"fahrradzaehler_stundenwerten_{date_str}.csv.gz"
            save_name = f"bicycle_counts_{date_str}.csv"
            url = BASE_URL + file_name
            response = requests.get(url)

            if response.status_code == 200:
                file_path = os.path.join(raw_folder, file_name)
                write_path = os.path.join(raw_folder, save_name)
                
                with open(file_path, "wb") as f:
                    f.write(response.content)


                # also unpack the gz file and delete the original gz file
                with gzip.open(file_path, 'rb') as f_in:
                    with open(write_path, 'wb') as f_out:
                        f_out.write(f_in.read())

                os.remove(file_path)
            else:
                print(f"Failed to download: {file_name} (Status code: {response.status_code}) (possibly no data for this month)")

    print("Data download completed.")

    # Now we want to process the downloaded data further, currently one month contains the monthly data for several cities and stations.
    CITY_INDEX =  "domain_name"
    STATION_INDEX = "counter_site_id"
    TIMESTAMP_INDEX = "iso_timestamp"


    entries = {}

    all_files = [f for f in os.listdir(raw_folder) if f.endswith('.csv')]

    print(f"Preprocessing {len(all_files)} downloaded files...")
    
    # Go through all downloaded files and aggregate them by city and station, safe data by station in one large dataframe
    for file in tqdm(all_files, desc="Processing files..."):
        file_path = os.path.join(raw_folder, file)
        df = pl.read_csv(file_path, separator=',' , n_threads=4, ignore_errors=True,
            schema={
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
        )
        
        # Fix channels:
        # Override channels all only if channels_in and channels_out are not null, else:
        # Some cities only implement channels_unknown and leave the others empty, in this case, put the data into channels_all
        df = df.with_columns([
            pl.when(
                (pl.col("channels_in").is_not_null()) & (pl.col("channels_out").is_not_null())
            )
            .then(pl.col("channels_in") + pl.col("channels_out"))
            .otherwise(pl.col("channels_unknown"))
            .alias("channels_all")
        ])

        # go through all unique city and station combinations
        for (city, station), group in df.group_by([CITY_INDEX, STATION_INDEX]):
            if city not in entries:
                entries[city] = {}

            if station not in entries[city]:
                entries[city][station] = []

            entries[city][station].append(group)


    # go through the entries and concatenate the dataframes for each city and station
    for city in entries:
        for station in entries[city]:
            
            try:
                entries[city][station] = pl.concat(entries[city][station], how="diagonal")
            except Exception as e:
                print(f"\n❌ Error concatenating {city} - Station {station}: {e}")


    proc_folder = os.path.join(folder, "processed", "cycle_counter")
    Path(proc_folder).mkdir(parents=True, exist_ok=True)

    # save each city and station combination into a seperate file
    for city in tqdm(entries, desc="Saving processed data..."):
        city_folder = os.path.join(proc_folder, city.replace(" ", "_"))
        Path(city_folder).mkdir(parents=True, exist_ok=True)

        for station in entries[city]:
            station_file = os.path.join(city_folder, f"station_{station}.csv")
            entries[city][station].write_csv(station_file)

    # now save each city and station combination into a seperate file
    print("Saving processed data...")


    if not args.keep_raw:
        print("Removing raw downloaded files...")
        # remove raw folder and its contents
        shutil.rmtree(raw_folder)

    print("Data processing completed.")
