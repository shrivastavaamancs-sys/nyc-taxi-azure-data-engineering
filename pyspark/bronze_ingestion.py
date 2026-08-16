import os
import shutil
from datetime import datetime

RAW_PATH = "data/raw/aggregated_nyc_yellow_taxi_2024.csv"
BRONZE_PATH = "data/bronze/nyc_taxi_2024"

os.makedirs(BRONZE_PATH, exist_ok=True)

output_file = os.path.join(
    BRONZE_PATH,
    "aggregated_nyc_yellow_taxi_2024.csv"
)

print("=== Bronze Ingestion Started ===")

shutil.copy2(RAW_PATH, output_file)

print("Source:", RAW_PATH)
print("Bronze:", output_file)
print("Ingestion Time:", datetime.now())

print("=== Bronze data written successfully ===")