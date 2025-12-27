import polars as pl
from analysis.visualization.characterisation.event_utils import get_baseline_event_features, utilitarian_score 

def compute_holiday_deltas(loader):
    intervals = loader.get_all_holiday_intervals(school_vacation=False)
    rows = []

    for station in loader.get_bicyle_stations():
        base, hol = get_baseline_event_features(
            loader,
            station,
            event_intervals=intervals,
        )

        if base is None or hol is None:
            continue

        U_base = utilitarian_score(base)
        U_hol  = utilitarian_score(hol)

        rows.append({
            "station": station,
            
            "U_base": U_base,
            "U_event": U_hol,
            "ΔU": U_hol - U_base,

            "ΔDPI": hol["DPI"] - base["DPI"],
            "ΔWSD": hol["WSD"] - base["WSD"],
            "ΔSDI": hol["SDI"] - base["SDI"],  
        })

    return pl.DataFrame(rows)
