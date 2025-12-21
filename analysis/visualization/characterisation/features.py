from analysis.visualization.characterisation.helpers import find_peak
from analysis.visualization.characterisation.indices import (hourly_index, monthly_index)
import polars as pl
import numpy as np


def warm_cold_drop(Im):
    q90 = Im["I_m"].quantile(0.9)
    q10 = Im["I_m"].quantile(0.1)
    return (q90 - q10) / q90

    
def double_peak_index(Ih):
    h_m, p_m = find_peak(Ih, 5, 10)     # first peak 
    h_e, p_e = find_peak(Ih, 14, 20)    # second peak

    midday = (
        Ih.filter((pl.col("hour") >= 8) & (pl.col("hour") < 14))
        .select(pl.mean("I_h"))
        .item()
    )

    strength = ((p_m - midday) + (p_e - midday)) / 2
    strength = max(strength, 0)

    symmetry = 1 - abs(p_m - p_e) / max(p_m, p_e)

    distance = min(abs(h_e - h_m) / 10, 1.0)

    score = strength * symmetry * distance
    return float(max(score, 0))


def weekend_shape_diff(Ih_wd, Ih_we):
    p_wd = Ih_wd["I_h"].to_numpy()
    p_we = Ih_we["I_h"].to_numpy()

    p_wd = p_wd / p_wd.sum()
    p_we = p_we / p_we.sum()

    return float(np.linalg.norm(p_wd - p_we))




def calc_feature_vector(loader, station_name, interval=None):
    Ih_weekday = hourly_index(loader=loader, station_name=station_name, interval=interval, weekday=True)
    Ih_weekend = hourly_index(loader=loader, station_name=station_name, interval=interval, weekday=False)
    Im = monthly_index(loader=loader, station_name=station_name, interval=interval)
    
    # seasons must be required, station which does not exist for given interval => height = 0 
    # we want to cluster all stations which do not exist for given interval with None
    if (Im.filter(pl.col("month").is_in([6,7,8])).height == 0 or 
        Im.filter(pl.col("month").is_in([11,12,1,2])).height == 0):
        return None

    dpi = double_peak_index(Ih_weekday)
    drop_season = warm_cold_drop(Im)
    shape_diff = weekend_shape_diff(Ih_wd=Ih_weekday,Ih_we=Ih_weekend)

    return {
        # Double peak in hourly index
        # Exists in utilitarian, does not exist in leisure
        "DPI": dpi, 
        "Drop_season": drop_season,
        "Shape_diff_wd_we": shape_diff
    }



def build_feature_df(loader, interval=None):
    rows = []

    for station in loader.get_bicyle_stations():
        feats = calc_feature_vector(loader, station, interval)

        row = {"station": station}
        if feats is None:
            row["valid"] = False
        else:
            row.update(feats)
            row["valid"] = True

        rows.append(row)

    return pl.DataFrame(rows)
