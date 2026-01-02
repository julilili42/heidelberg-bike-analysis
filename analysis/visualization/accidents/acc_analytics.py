import numpy as np
import polars as pl
import folium
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from sklearn.neighbors import BallTree

EARTH_RADIUS = 6371000

def calculate_station_stats(loader, radius_map=None, default_radius=50):
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

    try:
        df_all_accidents = loader.accident_data.df.select(
            ["latitude", "longitude", "is_bicycle"]
        ).drop_nulls()
    except AttributeError:
        df_all_accidents = loader.get_accidents().df.select(
            ["latitude", "longitude", "is_bicycle"]
        ).drop_nulls()

    X_stations = np.radians(station_coords)
    X_accidents = np.radians(df_all_accidents.select(["latitude", "longitude"]).to_numpy())
    is_bicycle_arr = df_all_accidents["is_bicycle"].to_numpy()
    
    radii_radians = np.array(radii_meters) / EARTH_RADIUS

    tree = BallTree(X_accidents, metric='haversine')
    indices_list = tree.query_radius(X_stations, r=radii_radians)

    total_counts = []
    bike_counts = []

    for indices in indices_list:
        n_total = len(indices)
        total_counts.append(n_total)
        
        if n_total > 0:
            n_bike = int(np.sum(is_bicycle_arr[indices]))
            bike_counts.append(n_bike)
        else:
            bike_counts.append(0)

    results = pl.DataFrame({
        "Station": station_names,
        "Radius (m)": radii_meters,
        "Total Accidents": total_counts,
        "Bicycle Accidents": bike_counts
    })

    results = results.with_columns(
        (pl.col("Bicycle Accidents") / pl.col("Total Accidents")).fill_nan(0).alias("Bicycle Share")
    )

    return results.sort("Bicycle Accidents", descending=True)


def plot_accident_map(loader, radius_map=None, default_radius=50):
    if radius_map is None:
        radius_map = {}
        
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

    stations = loader.get_bicyle_stations()

    for s in stations:
        slat, slon = loader.get_bicycle_location(s)
        
        r = radius_map.get(s, default_radius)
        
        folium.Circle(
            location=[slat, slon],
            radius=r,
            color="red", weight=2, fill=True, fill_color="red", fill_opacity=0.1,
            popup=f"Station: {s}<br>Radius: {r}m"
        ).add_to(m)

        folium.CircleMarker(
            location=[slat, slon], radius=4, color="black", fill=True, 
            fill_color="white", fill_opacity=1.0, popup=s
        ).add_to(m)

    _add_legend(m, years, year_colors)
    return m

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