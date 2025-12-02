# Environment setup

## Setup python
If you use python, this is pretty straightforward, run:
```zsh
uv sync
```
in the project directory. This generates the python environment and downloads all dependencies. Otherwise install them manually, all of them are listed in `pyproject.toml`.

Then install the core utilities:
```zsh
uv pip install -e .
```

# Fetch Data
### Bicycle Counter Data
```zsh
python data_io/fetch/fetch_cycle_data.py
```

### Accident Data
```zsh
python data_io/fetch/fetch_accident_data.py
```

### Weather Data
```zsh
python data_io/fetch/fetch_weather_data.py
```

### All Data
```zsh
python data_io/fetch/fetch_all.py
```

This downloads historical weather data for Heidelberg (2013-2025) from the Open-Meteo API, including:
- Temperature, humidity, precipitation
- Wind speed and direction
- Weather conditions (rain, snow, fog, etc.)
- Cloud cover

The data is automatically processed and combined into a single dataset with daily aggregates for easier analysis.

# Resources
Check out the ``resources`` folder.
