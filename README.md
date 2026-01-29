# Usage Patterns of Bicycle Counting Stations in Heidelberg and the Influence of External Factors

As cycling becomes key in sustainable urban mobility, understanding spatiotemporal traffic patterns is essential for infrastructure planning. While previous studies have classified bicycle traffic, they often rely on predefined, rule-based approaches that may fail to capture hybrid usage behaviors. Addressing this limitation, we present a data-driven approach to classify urban bicycle traffic using hourly data from counting stations in Heidelberg, Germany. We derive features to quantify the shape of traffic patterns across various timescales. Subsequent k-means distinguishes distinct usage patterns: utilitarian, recreational, and mixed. Finally, we investigate the influence of external factors, such as weather and public holidays, which varies significantly across different usage types.

## For Reviewers
- `analysis/characterisation`: contains all the deeper analysis for the project report used to create the final paper. Here, you find all notebooks that created the plots and tables.
- `analysis/exploration`: this code is not used for the final report, but contains analysis tools to visualize and get a better understanding of the data.
- `analysis/sanity`: this code contains sanity checks.

To improve orientation, we link all paths were the figures and tables used in the paper are created:

### Tables:
- [Table 1 - Overview Stations](analysis/sanity/sanity_tables.ipynb)
- [Table 3 - Effect Temperature](analysis/characterisation/notebooks/07_weather_impact.ipynb)
- [Table 4 - Effect Precipitation](analysis/characterisation/notebooks/07_weather_impact.ipynb)
- [Table 5 - Effect Holidays](analysis/characterisation/notebooks/06_holiday_impact.ipynb)


### Figures:
- [Figure 1 - Probabilistic Assignment](analysis/characterisation/notebooks/04_probabalistic_clustering.ipynb)
- [Figure 2 - Map of Stations](analysis/characterisation/notebooks/05_map.ipynb)
- [Figure 3 - Holiday Changes](analysis/characterisation/notebooks/06_holiday_impact.ipynb)


> **Important**  
> To run the code, it is recommended to use Visual Studio Code, as the base paths for the notebooks have been changed (see `.vscode/`). But before you run any code, setup the python environment and fetch the data as described below.

## Setup
If you use `uv`, this is pretty straightforward, run:
```zsh
uv sync
```
in the project directory. This generates the python environment and downloads all dependencies. Otherwise install them manually, all of them are listed in `pyproject.toml`.

Then install the core utilities:
```zsh
uv pip install -e .
```

## Fetch Data
```zsh
python data_io/fetch/fetch_all.py
```

This will fetch:
- cycle count
- accident data
- weather
- holidays

This downloads historical weather data for Heidelberg (2013-2025) from the Open-Meteo API, including:
- Temperature, humidity, precipitation
- Wind speed and direction
- Weather conditions (rain, snow, fog, etc.)
- Cloud cover

The data is automatically processed and combined into a single dataset with daily aggregates for easier analysis.