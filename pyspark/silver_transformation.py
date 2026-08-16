from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    regexp_replace,
    current_timestamp,
    expr
)

import os
import csv


# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("NYC-Taxi-Silver-Transformation")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. PATHS
# ============================================================

BRONZE_PATH = (
    "data/bronze/nyc_taxi_2024/"
    "aggregated_nyc_yellow_taxi_2024.csv"
)

SILVER_PATH = "data/silver/nyc_taxi_2024"

OUTPUT_FILE = os.path.join(
    SILVER_PATH,
    "nyc_taxi_silver.csv"
)


# ============================================================
# 3. START
# ============================================================

print("=" * 60)
print("NYC TAXI SILVER TRANSFORMATION STARTED")
print("=" * 60)

print(f"Bronze Source : {BRONZE_PATH}")
print(f"Silver Output : {OUTPUT_FILE}")


# ============================================================
# 4. READ BRONZE
# ============================================================

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(BRONZE_PATH)
)

print("\n=== BRONZE SCHEMA ===")

df.printSchema()


# ============================================================
# 5. INGESTION TIMESTAMP
# ============================================================

if "ingestion_timestamp" not in df.columns:

    print(
        "\n[INFO] ingestion_timestamp not found."
    )

    print(
        "[INFO] Creating ingestion_timestamp."
    )

    df = df.withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )


# ============================================================
# 6. CREATE HOUR_OF_DAY
# ============================================================

df = df.withColumn(
    "hour_value",
    regexp_replace(
        col("hour").cast("string"),
        ".* ",
        ""
    )
)

df = df.withColumn(
    "hour_of_day",
    col("hour_value")
    .substr(1, 2)
    .cast("int")
)


# ============================================================
# 7. CLEAN NUMERIC COLUMNS
# ============================================================

numeric_columns = [
    "passenger_count",
    "trip_count",
    "trip_distance_sum",
    "duration_sum",
    "fare_amount_sum",
    "extra_sum",
    "mta_tax_sum",
    "tip_amount_sum",
    "tolls_amount_sum",
    "improvement_surcharge_sum",
    "congestion_surcharge_sum",
    "airport_fee_sum",
    "total_amount_sum"
]


print("\n=== CLEANING NUMERIC COLUMNS ===")

for column_name in numeric_columns:

    if column_name in df.columns:

        print(
            f"[INFO] Cleaning numeric column: {column_name}"
        )

        # ----------------------------------------------------
        # try_cast converts valid numbers to DOUBLE.
        #
        # Invalid values such as:
        # Unknown
        # N/A
        # abc
        # empty values
        #
        # are converted to NULL instead of failing the job.
        # ----------------------------------------------------

        df = df.withColumn(
            column_name,
            expr(
                f"try_cast(`{column_name}` as double)"
            )
        )


# ============================================================
# 8. CLEAN BOROUGH COLUMNS
# ============================================================

print("\n=== CLEANING BOROUGH COLUMNS ===")


if "PU_Borough" in df.columns:

    df = df.withColumn(
        "PU_Borough",
        trim(col("PU_Borough"))
    )


if "DO_Borough" in df.columns:

    df = df.withColumn(
        "DO_Borough",
        trim(col("DO_Borough"))
    )


# ============================================================
# 9. DATA QUALITY FILTERS
# ============================================================

print("\n=== APPLYING DATA QUALITY FILTERS ===")


silver_df = (
    df

    # Date must exist
    .filter(
        col("date").isNotNull()
    )

    # Trip count must exist
    .filter(
        col("trip_count").isNotNull()
    )

    # Trip count cannot be negative
    .filter(
        col("trip_count") >= 0
    )

    # Trip distance cannot be negative
    .filter(
        col("trip_distance_sum").isNotNull()
    )
    .filter(
        col("trip_distance_sum") >= 0
    )

    # Total amount cannot be negative
    .filter(
        col("total_amount_sum").isNotNull()
    )
    .filter(
        col("total_amount_sum") >= 0
    )

    # Hour must be between 0 and 23
    .filter(
        col("hour_of_day").between(0, 23)
    )
)


# ============================================================
# 10. SELECT SILVER COLUMNS
# ============================================================

silver_columns = [
    "date",
    "hour_of_day",
    "passenger_count",
    "PU_Borough",
    "DO_Borough",
    "payment_type",
    "trip_count",
    "trip_distance_sum",
    "duration_sum",
    "fare_amount_sum",
    "extra_sum",
    "mta_tax_sum",
    "tip_amount_sum",
    "tolls_amount_sum",
    "improvement_surcharge_sum",
    "congestion_surcharge_sum",
    "airport_fee_sum",
    "total_amount_sum",
    "ingestion_timestamp"
]


silver_df = silver_df.select(
    *silver_columns
)


# ============================================================
# 11. SILVER SCHEMA
# ============================================================

print("\n=== SILVER SCHEMA ===")

silver_df.printSchema()


# ============================================================
# 12. SAMPLE RECORDS
# ============================================================

print("\n=== SILVER SAMPLE RECORDS ===")

silver_df.show(
    5,
    truncate=False
)


# ============================================================
# 13. RECORD COUNT
# ============================================================

print("\n=== SILVER RECORD COUNT ===")

silver_count = silver_df.count()

print(
    f"Silver Records : {silver_count}"
)


# ============================================================
# 14. CREATE OUTPUT DIRECTORY
# ============================================================

os.makedirs(
    SILVER_PATH,
    exist_ok=True
)


# ============================================================
# 15. WRITE SINGLE CSV
# ============================================================

print(
    "\n[INFO] Writing Silver data to CSV..."
)

print(
    "[INFO] Using toLocalIterator() instead of toPandas()."
)

print(
    "[INFO] This avoids loading all records into Pandas memory."
)


# ------------------------------------------------------------
# Remove old output file if it exists
# ------------------------------------------------------------

if os.path.exists(OUTPUT_FILE):

    print(
        f"[INFO] Removing existing output file: {OUTPUT_FILE}"
    )

    os.remove(OUTPUT_FILE)


# ------------------------------------------------------------
# Stream Spark rows to CSV
# ------------------------------------------------------------

rows = silver_df.toLocalIterator()


with open(
    OUTPUT_FILE,
    mode="w",
    newline="",
    encoding="utf-8"
) as file:

    writer = csv.writer(file)

    # --------------------------------------------------------
    # Write header
    # --------------------------------------------------------

    writer.writerow(
        silver_columns
    )

    # --------------------------------------------------------
    # Write records
    # --------------------------------------------------------

    for row in rows:

        writer.writerow(
            [
                row[column_name]
                for column_name in silver_columns
            ]
        )


# ============================================================
# 16. VERIFY OUTPUT
# ============================================================

print(
    "\n=== OUTPUT VERIFICATION ==="
)


if os.path.exists(OUTPUT_FILE):

    output_size = os.path.getsize(
        OUTPUT_FILE
    )

    print(
        "[SUCCESS] Silver CSV file created."
    )

    print(
        f"[SUCCESS] File size : {output_size / (1024 * 1024):.2f} MB"
    )

else:

    print(
        "[ERROR] Silver CSV file was not created."
    )

    spark.stop()

    raise FileNotFoundError(
        OUTPUT_FILE
    )


# ============================================================
# 17. SUCCESS
# ============================================================

print("\n" + "=" * 60)
print("SILVER DATA WRITTEN SUCCESSFULLY")
print("=" * 60)

print(
    f"Records : {silver_count}"
)

print(
    f"Output  : {OUTPUT_FILE}"
)


# ============================================================
# 18. STOP SPARK
# ============================================================

spark.stop()

print(
    "\nSpark session stopped."
)

print(
    "Silver transformation completed successfully."
)