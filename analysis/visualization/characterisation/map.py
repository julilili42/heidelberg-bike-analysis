import folium
import polars as pl

USAGE_COLORS = {
    "leisure": "green",
    "mixed": "orange",
    "utilitarian": "red"
}



def dominant_cluster_probs(cluster_probs_labeled):
    return (
        cluster_probs_labeled
        .sort("probability", descending=True)
        .group_by("station")
        .head(1)
        .select(["station", "cluster", "usage_type", "probability"])
    )



def add_usage_legend(m):
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
     <b>Usage type</b><br>
     <i style="background: green; width:10px; height:10px; float:left; margin-right:6px;"></i>
     Leisure<br>
     <i style="background: orange; width:10px; height:10px; float:left; margin-right:6px;"></i>
     Mixed<br>
     <i style="background: red; width:10px; height:10px; float:left; margin-right:6px;"></i>
     Utilitarian
     </div>
     """
    m.get_root().html.add_child(folium.Element(legend_html))




def bicycle_station_cluster_map(loader, cluster_probs_labeled, min_radius=5, max_radius=20):
    dom = dominant_cluster_probs(cluster_probs_labeled)

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

        usage = row["usage_type"].item()
        prob = row["probability"].item()

        color = USAGE_COLORS.get(usage, "gray")
        radius = min_radius + prob**2 * (max_radius - min_radius)

        folium.CircleMarker(
            location=[lat, lon],
            radius=radius,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.75,
            popup=f"""
            <b>{s}</b><br>
            Usage: {usage}<br>
            P = {prob:.2f}
            """
        ).add_to(m)

    add_usage_legend(m)
    return m
