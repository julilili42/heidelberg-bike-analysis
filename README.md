# Environment setup

## Setup python
If you use python, this is pretty straightforward, run:
```bash
uv sync
```
in the project directory. This generates the python environment and downloads all dependencies. Otherwise install them manually, all of them are listed in `pyproject.toml`.

Then install the core utilities:
```
uv pip install -e .
```

When using Visual Studio Code: Set base path for jupyter notebooks:
```"jupyter.notebookFileRoot": "${workspaceFolder}"```

## Download data
To fetch data, run:

### Bicycle Counter Data
```bash
python datalit/fetch/fetch_cycle_data.py
```

### Accident Data
```bash
python datalit/fetch/fetch_accident_data.py
```

### Weather Data
```bash
python datalit/fetch/fetch_weather_data.py
```

This downloads historical weather data for Heidelberg (2013-2025) from the Open-Meteo API, including:
- Temperature, humidity, precipitation
- Wind speed and direction
- Weather conditions (rain, snow, fog, etc.)
- Cloud cover

The data is automatically processed and combined into a single dataset with daily aggregates for easier analysis.

## Helpful ressources
Check out the ``resources`` folder.