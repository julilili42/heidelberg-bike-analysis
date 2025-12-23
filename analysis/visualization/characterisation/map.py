import folium
import polars as pl

USAGE_COLORS = {
    "recreational": "darkgreen",
    "leisure": "green",              # k=3 legacy
    "mixed": "orange",               # k=3
    "mixed recreational": "lightgreen",
    "mixed utilitarian": "orange",
    "utilitarian": "red",
}


def dominant_usage_probs(df_probs):
    return (
        df_probs
        .sort("probability", descending=True)
        .group_by("station")
        .head(1)
        .select(["station", "usage_type", "probability"])
    )




def add_usage_legend(m, usage_types):
    rows = []
    for u in usage_types:
        color = USAGE_COLORS.get(u, "gray")
        rows.append(
            f'<i style="background:{color}; width:10px; height:10px; '
            f'float:left; margin-right:6px;"></i>{u}<br>'
        )

    legend_html = f"""
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
     {''.join(rows)}
     </div>
     """

    m.get_root().html.add_child(folium.Element(legend_html))





def bicycle_station_cluster_map(loader, usage_probs, min_radius=5, max_radius=20):
    dom = dominant_usage_probs(usage_probs)
    
    usage_types = (
        usage_probs["usage_type"]
        .drop_nulls()
        .unique()
        .to_list()
    )

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
            popup=(
                f"<b>{s}</b><br>"
                f"Usage: {usage}<br>"
                f"P = {prob:.2f}<br>"
            )
        ).add_to(m)

    add_usage_legend(m, usage_types)
    return m

