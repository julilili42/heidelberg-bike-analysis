import polars as pl

# get the absolute and relative failure rate with start and end date 
# (to validate the output with Martin's Schaubild)
def station_outage_rate(dl):
    rows = []

    for station in dl.get_bicyle_stations():
        bd = dl.get_bicycle(station_name=station, sample_rate="1h")
        df = bd.df

        if df.is_empty():
            continue
        
        dt = (df.select(pl.col("datetime")).unique().sort("datetime"))        
        start = bd.min_date()
        end = bd.max_date()
        

        expected = pl.datetime_range(start=start, end=end, interval="1h", eager=True)
        
        missing_hours = (pl.DataFrame({"datetime": expected}).join(dt, on="datetime", how="anti").height)
        
        total_expected = len(expected)
        rows.append({
            "station": station,
            "start": start.year,
            "end": end.year,
            "expected_hours": total_expected,
            "missing_hours": missing_hours,
            "outage_rate": missing_hours / total_expected,
        })

    return pl.DataFrame(rows).sort("outage_rate", descending=True)