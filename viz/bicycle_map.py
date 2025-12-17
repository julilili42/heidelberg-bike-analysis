import folium

def bicycle_station_map(loader):
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
        folium.Marker([lat, lon], popup=s).add_to(m)

    return m