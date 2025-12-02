import os
import requests
from pathlib import Path
import argparse
import zipfile
from tqdm import tqdm
import polars as pl
import shutil
from io import StringIO


if __name__ == "__main__":
    # command line interface for start and end year
    parser = argparse.ArgumentParser(description="Download accident data from Unfallatlas.")
    parser.add_argument("--start_year", default=2016, type=int, help="Start year for data download (inclusive, available from 2016).")
    parser.add_argument("--end_year", default=2024, type=int, help="End year for data download (inclusive).")
    # add the folder argument
    parser.add_argument("--folder", default="data/", type=str, help="Folder to save downloaded data.")
    parser.add_argument("--keep-raw", default=False, action='store_true', help="Whether to keep raw downloaded zip files.")
    args = parser.parse_args()

    BASE_URL = "https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/"

    start_year = args.start_year
    end_year = args.end_year
    folder = args.folder

    raw_folder = os.path.join(folder, "raw/accidents/")
    output_folder = os.path.join(folder, "processed/accidents/")

    # create the folder if it does not exist
    Path(raw_folder).mkdir(parents=True, exist_ok=True)
    Path(output_folder).mkdir(parents=True, exist_ok=True)

    # The URL pattern is: https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/Unfallorte2024_EPSG25832_CSV.zip
    for year in tqdm(range(start_year, end_year + 1), desc="Downloading..."):
        file_name = f"Unfallorte{year}_EPSG25832_CSV.zip"
        url = BASE_URL + file_name
        
        response = requests.get(url, stream=True)

        if response.status_code == 200:
            zip_path = os.path.join(raw_folder, file_name)
            with open(zip_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            # grab only the text file from the zip and save it as a csv
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                for file in zip_ref.namelist():
                    if file.endswith('.txt') or file.endswith('.csv'):
                        extracted_path = zip_ref.extract(file, raw_folder)
                        # replace , with . in the extracted file to fix decimal format
                        with open(extracted_path, 'r', encoding='utf-8') as f:
                            content = f.read().replace(',', '.')

                        #transform the content into a polars dataframe
                        content = StringIO(content)
                        
                        df = pl.read_csv(content, separator=';', ignore_errors=True, n_threads=4)

                        # as we live in germany we have the multiple data formats which we need to unify...
                        # rename colums if needed
                        new_names = {
                            "ULAND": "state",
                            "UREGBEZ": "region",
                            "UKREIS": "district",
                            "UGEMEINDE": "municipality",
                            "UJAHR": "year",
                            "UMONAT": "month",
                            "UWOCHENTAG": "weekday",
                            "USTUNDE": "hour",
                            "UTYP1": "accident_type",
                            "UKATEGORIE": "injury_severity",
                            "ULICHTVERH": "light_condition",
                            "LICHT" : "light_condition",
                            "IstStrasse": "road_condition",
                            "STRZUSTAND": "road_condition",
                            "IstStrassenzustand": "road_condition",
                            "IstRad": "is_bicycle",
                            "IstPKW": "is_car",
                            "IstFuss": "is_pedestrian",
                            "IstKrad": "is_motorcycle",
                            "IstSonstige": "is_other",
                            "IstSonstig": "is_other",
                            "YGCSWGS84": "latitude",
                            "XGCSWGS84": "longitude"
                        }

                        # iterate over the new_names and rename if the old name exists in the dataframe
                        rename_dict = {}
                        for old_name, new_name in new_names.items():
                            if old_name in df.columns:
                                rename_dict[old_name] = new_name
                        if rename_dict:
                            df = df.rename(rename_dict)

                        # if "IstGkfz" exists we want to combine itz with "is_other" by OR operation
                        if "IstGkfz" in df.columns:
                            df = df.with_columns((pl.col("is_other") | pl.col("IstGkfz")).alias("is_other"))
                            df = df.drop("IstGkfz") 

                        # use only relevant columns
                        relevant_columns = [
                            "state", "region", "district", "municipality", "year", "month", "weekday", "hour",
                            "accident_type", "injury_severity", "light_condition", "road_condition",
                            "is_bicycle", "is_car", "is_pedestrian", "is_motorcycle", "is_other",
                            "LINREFX", "LINREFY", "latitude", "longitude"
                        ]

                        df = df.select([col for col in relevant_columns if col in df.columns])

                        csv_save_path = os.path.join(output_folder, f"accidents_{year}.csv")
                        df.write_csv(csv_save_path)
                        os.remove(extracted_path)  # remove the extracted txt file
        else:
            print(f"Failed to download: {file_name} (Status code: {response.status_code})")


    if not args.keep_raw:
        print("Removing raw downloaded files...")
        shutil.rmtree(raw_folder)

    print("Data processing completed.")