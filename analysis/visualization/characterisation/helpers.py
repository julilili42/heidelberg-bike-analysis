import polars as pl

def find_peak(Ih, hour_min, hour_max):
    w = Ih.filter((pl.col("hour") >= hour_min) & (pl.col("hour") < hour_max))
    if w.height == 0:
        return None, None
    row = w.sort("I_h", descending=True).row(0)
    return row[w.columns.index("hour")], row[w.columns.index("I_h")]