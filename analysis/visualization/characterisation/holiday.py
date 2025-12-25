import polars as pl
from analysis.visualization.characterisation.features import calc_feature_vector

def compute_holiday_deltas(loader):
    intervals = loader.get_all_holiday_intervals(school_vacation=False)
    rows = []

    for station in loader.get_bicyle_stations():
        baseline = calc_feature_vector(
            loader=loader,
            station_name=station,
            neg_dates=True
        )
        holiday = calc_feature_vector(
            loader=loader,
            station_name=station,
            filter_dates=intervals
        )

        if baseline is None or holiday is None:
            continue

        row = {"station": station}

        if "DPI" in baseline and "DPI" in holiday:
            row["ΔDPI"] = holiday["DPI"] - baseline["DPI"]

        if "WSD" in baseline and "WSD" in holiday:
            row["ΔWSD"] = holiday["WSD"] - baseline["WSD"]

        if len(row) > 1:  
            rows.append(row)

    return pl.DataFrame(rows)


