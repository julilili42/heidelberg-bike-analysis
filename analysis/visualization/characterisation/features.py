from analysis.visualization.characterisation.helpers import find_peak
from analysis.visualization.characterisation.indices import hourly_index, monthly_index
import polars as pl
import numpy as np


def build_feature_df(loader, interval=None, filter_dates=None, neg_dates=False):
    rows = []

    for station in loader.get_bicyle_stations():
        feats = calc_feature_vector(
            loader, station, interval, filter_dates=filter_dates, neg_dates=neg_dates
        )

        row = {"station": station}
        if feats is None:
            row["valid"] = False
        else:
            row.update(feats)
            row["valid"] = True

        rows.append(row)

    return pl.DataFrame(rows)


def calc_feature_vector(
    loader, station_name, interval=None, filter_dates=None, neg_dates=False
):
    Ih_weekday = hourly_index(
        loader=loader,
        station_name=station_name,
        interval=interval,
        weekday=True,
        filter_dates=filter_dates,
        neg_dates=neg_dates,
    )
    Ih_weekend = hourly_index(
        loader=loader,
        station_name=station_name,
        interval=interval,
        weekday=False,
        filter_dates=filter_dates,
        neg_dates=neg_dates,
    )
    Im = monthly_index(
        loader=loader,
        station_name=station_name,
        interval=interval,
        filter_dates=filter_dates,
        neg_dates=neg_dates,
    )

    # seasons must be required, station which does not exist for given interval => height = 0
    # we want to cluster all stations which do not exist for given interval with None
    if (
        Im.filter(pl.col("month").is_in([6, 7, 8])).height == 0
        or Im.filter(pl.col("month").is_in([11, 12, 1, 2])).height == 0
    ):
        return None

    dpi = double_peak_index(Ih=Ih_weekday)
    wsd = weekend_shape_diff_index(Ih_wd=Ih_weekday, Ih_we=Ih_weekend)
    sdi = seasonal_drop_index(Im=Im)
    
    if dpi is None or wsd is None or sdi is None:
        return None

    return {"DPI": dpi, "WSD": wsd, "SDI": sdi}


""" FEATURES """


def seasonal_drop_index(Im):
    q10 = Im["I_m"].quantile(0.1)
    q90 = Im["I_m"].quantile(0.9)

    if q90 is None or q90 <= 0:
        return None
    
    return (q90 - q10) / q90


def double_peak_index(Ih):
    h_m, p_m = find_peak(Ih, 5, 10)  # first peak
    h_e, p_e = find_peak(Ih, 14, 20)  # second peak

    midday_df = Ih.filter((pl.col("hour") >= 8) & (pl.col("hour") < 14))
    if midday_df.height == 0:
        return None
    
    midday = midday_df.select(pl.mean("I_h")).item()
    if midday is None:
        return None
    
    strength = ((p_m - midday) + (p_e - midday)) / 2
    strength = max(strength, 0)

    symmetry = 1 - abs(p_m - p_e) / max(p_m, p_e)

    distance = min(abs(h_e - h_m) / 10, 1.0)

    score = strength * symmetry * distance
    return float(max(score, 0))


def weekend_shape_diff_index(Ih_wd, Ih_we):
    if Ih_wd is None or Ih_we is None:
        return None
    
    p_wd = Ih_wd["I_h"].to_numpy()
    p_we = Ih_we["I_h"].to_numpy()

    if len(p_wd) != 24 or len(p_we) != 24:
        return None
    

    p_wd = p_wd / p_wd.sum()
    p_we = p_we / p_we.sum()

    return float(np.linalg.norm(p_wd - p_we))
