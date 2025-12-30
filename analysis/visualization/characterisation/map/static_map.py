from matplotlib.patches import Rectangle
import numpy as np
import matplotlib.pyplot as plt
import contextily as cx
from matplotlib.lines import Line2D

def add_scalebar(
    ax,
    y_center,
    length_km=3,
    height_km=0.1,
    location=(0.25, 0.01),
    linewidth=1.2,
    fontsize=14,
    manual_factor=1.63,
):
    lat_center = np.degrees(np.arctan(np.sinh(y_center / 6378137)))
    mercator_correction = np.cos(np.radians(lat_center))

    length_m = length_km * 1000 / mercator_correction * manual_factor
    height_m = height_km * 1000

    xmin, xmax = ax.get_xlim()
    ymin, ymax = ax.get_ylim()

    x0 = xmin + location[0] * (xmax - xmin)
    y0 = ymin + location[1] * (ymax - ymin)

    # blocks
    ax.add_patch(Rectangle((x0, y0), length_m/2, height_m,
                           facecolor="black", edgecolor="black",
                           linewidth=linewidth, zorder=10))
    ax.add_patch(Rectangle((x0+length_m/2, y0), length_m/2, height_m,
                           facecolor="white", edgecolor="black",
                           linewidth=linewidth, zorder=10))
    ax.add_patch(Rectangle((x0, y0), length_m, height_m,
                           fill=False, edgecolor="black",
                           linewidth=linewidth, zorder=11))

    # labels
    ax.text(x0, y0 + height_m*1.6, "0", ha="center", va="bottom", fontsize=fontsize)
    ax.text(x0 + length_m/2, y0 + height_m*1.6,
            f"{length_km/2:.0f}", ha="center", va="bottom", fontsize=fontsize)
    ax.text(x0 + length_m, y0 + height_m*1.6,
            f"{length_km} km", ha="center", va="bottom", fontsize=fontsize)




import polars as pl
import geopandas as gpd

def build_station_geodataframe(usage_probs, loader):
    dom = (
        usage_probs
        .sort("probability", descending=True)
        .group_by("station")
        .head(1)
    )

    rows = []
    for row in dom.iter_rows(named=True):
        lat, lon = loader.get_bicycle_location(row["station"])
        rows.append({
            "station": row["station"],
            "usage_type": row["usage_type"],
            "probability": row["probability"],
            "lat": lat,
            "lon": lon,
        })

    df = pl.DataFrame(rows).to_pandas()

    return gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.lon, df.lat),
        crs="EPSG:4326"
    ).to_crs(epsg=3857)






USAGE_STYLE = {
    "recreational": dict(color="darkgreen", marker="o"),
    "mixed":        dict(color="orange",    marker="^"),
    "utilitarian":  dict(color="red",       marker="s"),
}

def marker_size(p, s_min=50, s_max=500):
    return s_min + (p**2) * (s_max - s_min)


def plot_station_map(gdf, city, zoom=0.6, shift_x=0, shift_y=0, figsize=(14,9)):
    fig, ax = plt.subplots(figsize=figsize)

    # stations
    for usage, style in USAGE_STYLE.items():
        sub = gdf[gdf.usage_type == usage]
        ax.scatter(
            sub.geometry.x,
            sub.geometry.y,
            s=marker_size(sub.probability),
            c=style["color"],
            marker=style["marker"],
            edgecolor="black",
            linewidth=0.6,
            alpha=0.9,
            zorder=4,
        )

    # boundary
    city.boundary.plot(ax=ax, color="black", linewidth=1.2, zorder=3)

    # extent
    xmin, ymin, xmax, ymax = city.total_bounds
    x_center = (xmin + xmax) / 2 + shift_x * (xmax - xmin)
    y_center = (ymin + ymax) / 2 + shift_y * (ymax - ymin)
    w = (xmax - xmin) * zoom / 2
    h = (ymax - ymin) * zoom / 2

    ax.set_xlim(x_center - w, x_center + w)
    ax.set_ylim(y_center - h, y_center + h)

    # basemap
    cx.add_basemap(ax, source=cx.providers.OpenStreetMap.Mapnik,
                   attribution=False, alpha=0.85)

    ax.set_aspect("equal")
    ax.set_axis_off()

    return fig, ax, y_center





def add_legends(ax):
    usage_handles = [
        Line2D([0], [0],
               marker=USAGE_STYLE[u]["marker"],
               color="w",
               markerfacecolor=USAGE_STYLE[u]["color"],
               markeredgecolor="black",
               markersize=10,
               label=u)
        for u in USAGE_STYLE
    ]

    size_handles = [
        plt.scatter([], [], s=marker_size(p),
                    color="gray", edgecolor="black",
                    label=f"P = {p:.2f}")
        for p in [0.4, 0.6, 0.8]
    ]

    legend1 = ax.legend(
        handles=usage_handles,
        title="Dominant usage type",
        loc="upper left",
        frameon=True,
        facecolor="white",
        framealpha=0.85,
        edgecolor="black",
    )

    legend2 = ax.legend(
        handles=size_handles,
        title="Dominance probability",
        loc="lower left",
        frameon=True,
        facecolor="white",
        framealpha=0.85,
        edgecolor="black",
        borderpad=1,
        labelspacing=0.8,
    )

    ax.add_artist(legend1)
    ax.add_artist(legend2)


def plot_bicycle_usage_map(
    usage_probs,
    loader,
    city_geojson_path,
    zoom=0.6,
    shift_x=0.0,
    shift_y=0.0,
    scalebar=False,
    scalebar_km=3,
    scalebar_height_km=0.1,
    scalebar_location=(0.25, 0.01),
    scalebar_fontsize=14,
    save=False,
    outfile="station_usage_map.png",
    figsize=(14,9)
):
    gdf = build_station_geodataframe(usage_probs, loader)

    city = gpd.read_file(city_geojson_path).to_crs(epsg=3857)

    fig, ax, y_center = plot_station_map(
        gdf,
        city,
        zoom=zoom,
        shift_x=shift_x,
        shift_y=shift_y,
        figsize=figsize
    )

    add_legends(ax)

    if scalebar:
        add_scalebar(
            ax,
            y_center=y_center,
            length_km=scalebar_km,
            height_km=scalebar_height_km,
            location=scalebar_location,
            fontsize=scalebar_fontsize,
        )

    plt.tight_layout()

    if save:
        fig.savefig(
            outfile,
            bbox_inches="tight",
        )

    return fig, ax
