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



def utilitarian_score(feats):
    return feats["DPI"] + feats["WSD"] - feats["SDI"]