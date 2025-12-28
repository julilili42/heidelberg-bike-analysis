import polars as pl
from analysis.visualization.characterisation.features import calc_feature_vector

def get_baseline_event_features(
    loader,
    station,
    event_intervals=None,
    baseline_negate=True,
):
    base = calc_feature_vector(
        loader=loader,
        station_name=station,
        neg_dates=baseline_negate,
        filter_dates=event_intervals,
    )
    event = calc_feature_vector(
        loader=loader,
        station_name=station,
        filter_dates=event_intervals,
    )

    if base is None or event is None:
        return None, None

    return base, event


# event = {holiday, weather}
def compute_event_deltas(loader, intervals):
    rows = []

    for station in loader.get_bicyle_stations():
        base, event = get_baseline_event_features(
            loader,
            station,
            event_intervals=intervals,
        )

        if base is None or event is None:
            continue

        U_base = utilitarian_score(base)
        U_event  = utilitarian_score(event)

        rows.append({
            "station": station,
            
            "U_base": U_base,
            "U_event": U_event,
            "U_delta": U_event - U_base,

            "DPI_delta": event["DPI"] - base["DPI"],
            "WSD_delta": event["WSD"] - base["WSD"],
            "SDI_delta": event["SDI"] - base["SDI"],  
        })

    return pl.DataFrame(rows)



def utilitarian_score(feats):
    return feats["DPI"] + feats["WSD"] - feats["SDI"]