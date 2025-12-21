import folium
import polars as pl

CLUSTER_COLORS = {
    0: "blue",
    1: "orange",
    2: "green",
    3: "red"
}

def add_cluster_legend(m):
    legend_html = """
     <div style="
     position: fixed;
     bottom: 40px;
     left: 40px;
     z-index: 9999;
     background-color: white;
     padding: 10px;
     border: 2px solid grey;
     border-radius: 5px;
     font-size: 14px;
     ">
     <b>Clusters</b><br>
     <i style="background: blue; width: 10px; height: 10px; float: left; margin-right: 6px;"></i>
     Cluster 0<br>
     <i style="background: orange; width: 10px; height: 10px; float: left; margin-right: 6px;"></i>
     Cluster 1<br>
     <i style="background: green; width: 10px; height: 10px; float: left; margin-right: 6px;"></i>
     Cluster 2
     </div>
     """
    m.get_root().html.add_child(folium.Element(legend_html))


def bicycle_station_cluster_map(loader, cluster_df, cluster_col):
    stations = loader.get_bicyle_stations()

    lats, lons = [], []
    for s in stations:
        lat, lon = loader.get_bicycle_location(s)
        lats.append(lat)
        lons.append(lon)

    m = folium.Map(
        location=[sum(lats)/len(lats), sum(lons)/len(lons)],
        zoom_start=13
    )

    for s, lat, lon in zip(stations, lats, lons):
        cluster = (
            cluster_df
            .filter(pl.col("station") == s)
            .select(cluster_col)
            .item()
        )

        color = CLUSTER_COLORS.get(cluster, "gray")

        folium.CircleMarker(
            location=[lat, lon],
            radius=7,
            color=color,
            fill=True,
            fill_opacity=0.9,
            popup=f"{s}<br>Cluster {cluster}"
        ).add_to(m)

    return m