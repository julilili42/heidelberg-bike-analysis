import folium
import polars as pl

CLUSTER_COLORS = {
    0: "blue",
    1: "orange",
    2: "green",
    3: "red"
}


def dominant_cluster_probs(cluster_probs):
    return (
        cluster_probs
        .sort("probability", descending=True)
        .group_by("station")
        .head(1)
        .select(["station", "cluster", "probability"])
    )


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



def bicycle_station_cluster_map(loader, cluster_probs, min_radius=5, max_radius=20):
    dom = dominant_cluster_probs(cluster_probs)

    stations = loader.get_bicyle_stations()
    lats, lons = zip(*(loader.get_bicycle_location(s) for s in stations))

    m = folium.Map(
        location=[sum(lats)/len(lats), sum(lons)/len(lons)],
        zoom_start=13
    )

    for s, lat, lon in zip(stations, lats, lons):
        row = dom.filter(pl.col("station") == s)
        if row.is_empty():
            continue

        cluster = row["cluster"].item()
        prob = row["probability"].item()

        color = CLUSTER_COLORS.get(cluster, "gray")
        radius = min_radius + prob**2 * (max_radius - min_radius)

        folium.CircleMarker(
            location=[lat, lon],
            radius=radius,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=75,  
            popup=f"""
            <b>{s}</b><br>
            Cluster: {cluster}<br>
            P = {prob:.2f}
            """
        ).add_to(m)

    add_cluster_legend(m)
    return m