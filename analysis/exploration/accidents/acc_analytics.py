import numpy as np
import polars as pl
import folium
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from sklearn.neighbors import BallTree

EARTH_RADIUS = 6371000

# helper functions

def _prepare_station_data(loader, radius_map, default_radius):
    if radius_map is None:
        radius_map = {}

    stations = loader.get_bicyle_stations()
    station_coords = []
    station_names = []
    radii_meters = [] 

    for s in stations:
        lat, lon = loader.get_bicycle_location(s)
        station_coords.append([lat, lon])
        station_names.append(s)
        r = radius_map.get(s, default_radius)
        radii_meters.append(r)
    
    return station_names, np.array(station_coords), np.array(radii_meters)

def _get_accident_data(loader, columns):
    try:
        return loader.accident_data.df.select(columns).drop_nulls()
    except AttributeError:
        return loader.get_accidents().df.select(columns).drop_nulls()

def _perform_spatial_search(station_coords, accident_coords, radii_meters):
    X_stations = np.radians(station_coords)
    X_accidents = np.radians(accident_coords)
    radii_radians = radii_meters / EARTH_RADIUS

    tree = BallTree(X_accidents, metric='haversine')
    return tree.query_radius(X_stations, r=radii_radians)

def _add_legend(m, years, year_colors):
    legend_html = '''
         <div style="position: fixed; bottom: 50px; left: 50px; width: 160px; height: auto; 
         border: 2px solid grey; z-index: 9999; background-color: white; padding: 10px; font-size: 14px;">
         <b>Legend</b><br>
         <i style="background:red; opacity:0.3; width:12px; height:12px; float:left; margin-right:6px; border:1px solid red;"></i>Station (Radius)<br>
         <br><b>Accident Years</b><br>
    '''
    for year in years:
        color = year_colors[year]
        legend_html += f'<i style="background:{color}; width:12px; height:12px; float:left; margin-right:6px; opacity:0.9;"></i>{year}<br>'
    legend_html += "</div>"
    m.get_root().html.add_child(folium.Element(legend_html))


# main functions

def calculate_station_stats(loader, radius_map=None, default_radius=50):
    names, coords, radii = _prepare_station_data(loader, radius_map, default_radius)
    
    df_acc = _get_accident_data(loader, ["latitude", "longitude", "is_bicycle"])
    acc_coords = df_acc.select(["latitude", "longitude"]).to_numpy()
    is_bicycle_arr = df_acc["is_bicycle"].to_numpy()

    indices_list = _perform_spatial_search(coords, acc_coords, radii)

    total_counts = []
    bike_counts = []

    for indices in indices_list:
        n_total = len(indices)
        total_counts.append(n_total)
        if n_total > 0:
            bike_counts.append(int(np.sum(is_bicycle_arr[indices])))
        else:
            bike_counts.append(0)

    results = pl.DataFrame({
        "Station": names,
        "Radius (m)": radii,
        "Total Accidents": total_counts,
        "Bicycle Accidents": bike_counts
    })

    results = results.with_columns(
        (pl.col("Bicycle Accidents") / pl.col("Total Accidents")).fill_nan(0).alias("Bicycle Share")
    )

    return results.sort("Bicycle Accidents", descending=True)


def calculate_yearly_stats(loader, radius_map=None, default_radius=50):
    names, coords, radii = _prepare_station_data(loader, radius_map, default_radius)
    
    acc_df = (
        loader.accident_data
        .bicycle_only()
        .df
        .select(["latitude", "longitude", "year"])
        .drop_nulls()
    )
    
    acc_coords = acc_df.select(["latitude", "longitude"]).to_numpy()
    years_arr = acc_df["year"].to_numpy()

    indices_list = _perform_spatial_search(coords, acc_coords, radii)
    results = []

    for i, station in enumerate(names):
        indices = indices_list[i]
        station_years = years_arr[indices]
        unique_years, counts = np.unique(station_years, return_counts=True)
        
        for year, count in zip(unique_years, counts):
            results.append({
                "Station": station,
                "Jahr": int(year),
                "Unfälle": int(count)
            })
            
    df_long = pl.DataFrame(results)
    
    if df_long.is_empty():
        return pl.DataFrame({"Station": names})

    df_pivot = (
        df_long
        .pivot(values="Unfälle", index="Station", columns="Jahr", aggregate_function="sum")
        .fill_null(0) 
    )
    year_cols = [col for col in df_pivot.columns if col != "Station"]
    year_cols.sort()
    
    final_cols = ["Station"] + year_cols
    df_pivot = df_pivot.select(final_cols)
    
    df_pivot = df_pivot.with_columns(
        pl.sum_horizontal(year_cols).alias("Total")
    ).sort("Total", descending=True).drop("Total")

    return df_pivot


def plot_accident_map(loader, radius_map=None, default_radius=50):
    acc = (
        loader.accident_data
        .bicycle_only()
        .filter_region(state=8, region=2, district=21)
    )
    points = acc.map_points()
    years = sorted(acc.df["year"].unique().to_list())

    cmap = plt.colormaps['viridis']
    year_colors = {
        year: mcolors.rgb2hex(cmap(i / (len(years)-1))) 
        for i, year in enumerate(years)
    }

    center_coords = acc.df.select(["latitude", "longitude"]).mean().to_numpy()[0].tolist()
    m = folium.Map(location=center_coords, zoom_start=14)

    for lat, lon, year, month in points:
        folium.CircleMarker(
            location=[lat, lon], radius=3, popup=f"Accident: {year}-{month}",
            color=year_colors[year], fill=True, fill_color=year_colors[year], fill_opacity=0.7
        ).add_to(m)

    names, coords, radii = _prepare_station_data(loader, radius_map, default_radius)

    for i, station in enumerate(names):
        slat, slon = coords[i]
        r = radii[i]
        
        folium.Circle(
            location=[slat, slon], radius=float(r),
            color="red", weight=2, fill=True, fill_color="red", fill_opacity=0.1,
            popup=f"Station: {station}<br>Radius: {r}m"
        ).add_to(m)

        folium.CircleMarker(
            location=[slat, slon], radius=4, color="black", fill=True, 
            fill_color="white", fill_opacity=1.0, popup=station
        ).add_to(m)

    _add_legend(m, years, year_colors)
    return m