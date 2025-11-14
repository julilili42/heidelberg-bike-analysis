# Environment setup

## Setup python
If you use python, this is pretty straightforward, run:
```bash
uv sync
```
in the project directory. This generates the python environment and downloads all dependencies. Otherwise install them manually, all of them are listed in `pyproject.toml`.

## Download data
To fetch data, run:
```bash
python src/fetch/fetch_cycle_data.py
```

```bash
python src/fetch/fetch_accident_data.py
```

## Helpful ressources
Check out the ``resources`` folder.