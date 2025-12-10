import argparse
import requests
import pandas as pd
import os


# Fetches public holidays and schulferien
def fetch_holidays(start_year, end_year, output_dir):
    base_url = "https://www.mehr-schulferien.de/api/v2.1/federal-states/baden-wuerttemberg/periods"

    start_date = f"{start_year}-01-01"
    end_date = f"{end_year}-12-31"

    params = {"start_date": start_date, "end_date": end_date}

    print(f"Fetching holidays from {start_date} to {end_date}...")
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        json_response = response.json()
        data = json_response.get("data", [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return

    if not data:
        print("No data found for the given period.")
        return

    # We will extract relevant fields.
    holidays = []
    for item in data:
        holiday = {
            "id": item.get("id"),
            "name": item.get("name"),
            "type": item.get("type"),
            "start_date": item.get("starts_on"),
            "end_date": item.get("ends_on"),
            "is_public_holiday": item.get("is_public_holiday"),
            "is_school_vacation": item.get("is_school_vacation"),
        }
        holidays.append(holiday)

    # Fastnachtsferien
    manual_vacations = [
        {
            "name": "Fastnachtsferien",
            "start_date": "2020-02-24",
            "end_date": "2020-02-28",
        },
        {
            "name": "Fastnachtsferien",
            "start_date": "2021-02-15",
            "end_date": "2021-02-19",
        },
        {
            "name": "Fastnachtsferien",
            "start_date": "2022-02-28",
            "end_date": "2022-03-04",
        },
        {
            "name": "Fastnachtsferien",
            "start_date": "2023-02-20",
            "end_date": "2023-02-24",
        },
        {
            "name": "Fastnachtsferien",
            "start_date": "2024-02-12",
            "end_date": "2024-02-18",
        },
        {
            "name": "Fastnachtsferien",
            "start_date": "2025-03-03",
            "end_date": "2025-03-09",
        },
        {
            "name": "Fastnachtsferien",
            "start_date": "2026-02-16",
            "end_date": "2026-02-22",
        },
    ]

    for vacation in manual_vacations:
        holidays.append(
            {
                "id": f"manual_fastnacht_{vacation['start_date'][:4]}",
                "name": vacation["name"],
                "type": "school_vacation",
                "start_date": vacation["start_date"],
                "end_date": vacation["end_date"],
                "is_public_holiday": False,
                "is_school_vacation": True,
            }
        )

    df = pd.DataFrame(holidays)

    if df.empty:
        print("No holiday data extracted.")
        return

    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"schulferien_holidays_bw.csv")

    df.to_csv(output_file, index=False)
    print(f"Successfully saved holiday data to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch holiday data for Baden-Württemberg."
    )
    parser.add_argument(
        "--start-year", type=int, default=2020, help="Start year for fetching holidays"
    )
    parser.add_argument(
        "--end-year", type=int, default=2027, help="End year for fetching holidays"
    )

    args = parser.parse_args()

    output_directory = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "data",
        "processed",
        "holidays",
    )

    if int(args.start_year) < 2020:
        print("API DELIVERS NO DATA BEFORE 2020")
        print("API DELIVERS NO DATA BEFORE 2020")
        print("API DELIVERS NO DATA BEFORE 2020")
        print("API DELIVERS NO DATA BEFORE 2020")
        print("API DELIVERS NO DATA BEFORE 2020")
        print("API DELIVERS NO DATA BEFORE 2020")
    else:
        fetch_holidays(args.start_year, args.end_year, output_directory)
