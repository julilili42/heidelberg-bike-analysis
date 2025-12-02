# automates data loading by running all fetch scripts
import subprocess

scripts = [
    "data_io/fetch/fetch_accident_data.py",
    "data_io/fetch/fetch_cycle_data.py",
    "data_io/fetch/fetch_weather_data.py",
]

for script in scripts:
    result = subprocess.run(["uv", "run", script], check=False)
    if result.returncode != 0:
        print(f"{script} failed.\n")
    else:
        print(f"{script} completed successfully.\n")

print("Data loading finished.")