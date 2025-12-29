import polars as pl
from data_io.loader.data_loader import DataLoader
from analysis.visualization.characterisation.features import build_feature_df

CITY = "Stadt_Heidelberg"
N_CLUSTERS = 3

DATASET_START = "2016-01-01"
DATASET_END = "2025-01-01"
TIME_SERIES_MODE = "sliding"
WINDOW_MONTHS = 24

dl = DataLoader(city=CITY)

X = build_feature_df(dl)

EXCLUDE = {"station", "valid", "cluster", "date"}
FEATURES = [
    c for c in X.columns
    if c not in EXCLUDE and X[c].dtype in (pl.Float32, pl.Float64)
]
