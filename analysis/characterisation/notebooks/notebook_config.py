# config file used for notebooks

import polars as pl
from data_io.loader.data_loader import DataLoader
from sklearn.preprocessing import StandardScaler
from analysis.characterisation.features import build_feature_df


CITY = "Stadt_Heidelberg"

dl = DataLoader(city=CITY)

scaler = StandardScaler()

X = build_feature_df(dl)

N_CLUSTERS = 3

DATASET_START = "2016-01-01"
DATASET_END = "2025-01-01"
TIME_SERIES_MODE = "sliding"
WINDOW_MONTHS = 24

EXCLUDE = {"station", "valid", "cluster", "date"}
FEATURES = [
    c for c in X.columns
    if c not in EXCLUDE and X[c].dtype in (pl.Float32, pl.Float64)
]


