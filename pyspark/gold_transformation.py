from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum,
    avg,
    count,
    round
)
import os
import csv


# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("NYC-Taxi-Gold-Transformation")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. PATHS
# ============================================================

SILVER_PATH = "data/silver/nyc_taxi_2024/nyc_taxi_silver.csv"

GOLD_PATH = "data/gold/nyc_taxi_2024"

DAILY_PATH = os.path.join(
    GOLD_PATH,
    "daily_taxi_metrics.csv"
)

HOURLY_PATH = os.path.join(
    GOLD_PATH,
    "hourly_taxi_metrics.csv"
)

BOROUGH_PATH = os.path.join(
    GOLD_PATH,
    "borough_metrics.csv"
)

PAYMENT_PATH = os.path.join(
    GOLD_PATH,
    "payment_metrics.csv"
)


# ============================================================
# 3. START
# ============================================================

print("=" * 60)
print("NYC TAXI GOLD TRANSFORMATION STARTED")
print("=" * 60)

print(f"Silver Source : {SILVER_PATH}")
print(f"Gold Output   : {GOLD_PATH}")


# ============================================================
# 4. READ SILVER
# ============================================================

print("\n[INFO] Reading Silver data...")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(SILVER_PATH)
)

print("\n=== SILVER SCHEMA ===")
df.printSchema()


# ============================================================
# 5. BASIC VALIDATION
# ============================================================

print("\n[INFO] Validating Silver data...")

silver_count = df.count()

print(f"Silver Records : {silver_count}")


# ============================================================
# 6. CREATE GOLD DIRECTORY
# ============================================================

os.makedirs(
    GOLD_PATH,
    exist_ok=True
)


# ============================================================
# 7. DAILY TAXI METRICS
# ============================================================

print("\n[INFO] Creating daily_taxi_metrics...")

daily_df = (
    df
    .groupBy("date")
    .agg(
        sum("trip_count").alias("total_trips"),

        round(
            sum("trip_distance_sum"),
            2
        ).alias("total_distance"),

        round(
            sum("total_amount_sum"),
            2
        ).alias("total_revenue"),

        round(
            avg("trip_distance_sum"),
            2
        ).alias("avg_trip_distance"),

        round(
            avg("duration_sum"),
            2
        ).alias("avg_trip_duration"),

        round(
            avg("fare_amount_sum"),
            2
        ).alias("avg_fare"),

        round(
            avg("tip_amount_sum"),
            2
        ).alias("avg_tip")
    )
    .orderBy("date")
)


print("\n=== DAILY TAXI METRICS ===")

daily_df.show(
    10,
    truncate=False
)

print(
    f"Daily Records : {daily_df.count()}"
)


# ============================================================
# 8. HOURLY TAXI METRICS
# ============================================================

print("\n[INFO] Creating hourly_taxi_metrics...")

hourly_df = (
    df
    .groupBy("hour_of_day")
    .agg(
        sum("trip_count").alias("total_trips"),

        round(
            sum("trip_distance_sum"),
            2
        ).alias("total_distance"),

        round(
            sum("total_amount_sum"),
            2
        ).alias("total_revenue"),

        round(
            avg("duration_sum"),
            2
        ).alias("avg_trip_duration"),

        round(
            avg("fare_amount_sum"),
            2
        ).alias("avg_fare")
    )
    .orderBy("hour_of_day")
)


print("\n=== HOURLY TAXI METRICS ===")

hourly_df.show(
    24,
    truncate=False
)

print(
    f"Hourly Records : {hourly_df.count()}"
)


# ============================================================
# 9. BOROUGH METRICS
# ============================================================

print("\n[INFO] Creating borough_metrics...")

borough_df = (
    df
    .groupBy("PU_Borough")
    .agg(
        sum("trip_count").alias("total_trips"),

        round(
            sum("trip_distance_sum"),
            2
        ).alias("total_distance"),

        round(
            sum("total_amount_sum"),
            2
        ).alias("total_revenue"),

        round(
            avg("trip_distance_sum"),
            2
        ).alias("avg_trip_distance"),

        round(
            avg("duration_sum"),
            2
        ).alias("avg_trip_duration"),

        round(
            avg("fare_amount_sum"),
            2
        ).alias("avg_fare")
    )
    .orderBy(
        col("total_trips").desc()
    )
)


print("\n=== BOROUGH METRICS ===")

borough_df.show(
    20,
    truncate=False
)

print(
    f"Borough Records : {borough_df.count()}"
)


# ============================================================
# 10. PAYMENT METRICS
# ============================================================

print("\n[INFO] Creating payment_metrics...")

payment_df = (
    df
    .groupBy("payment_type")
    .agg(
        sum("trip_count").alias("total_trips"),

        round(
            sum("total_amount_sum"),
            2
        ).alias("total_revenue"),

        round(
            avg("fare_amount_sum"),
            2
        ).alias("avg_fare"),

        round(
            avg("tip_amount_sum"),
            2
        ).alias("avg_tip")
    )
    .orderBy("payment_type")
)


print("\n=== PAYMENT METRICS ===")

payment_df.show(
    truncate=False
)

print(
    f"Payment Records : {payment_df.count()}"
)


# ============================================================
# 11. WRITE GOLD CSV FILES
# ============================================================

def write_single_csv(dataframe, output_file, table_name):

    print(
        f"\n[INFO] Writing {table_name}..."
    )

    rows = dataframe.toLocalIterator()

    with open(
        output_file,
        mode="w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.writer(file)

        writer.writerow(
            dataframe.columns
        )

        for row in rows:

            writer.writerow(
                [
                    row[column]
                    for column in dataframe.columns
                ]
            )

    file_size = os.path.getsize(
        output_file
    )

    print(
        f"[SUCCESS] {table_name} created."
    )

    print(
        f"[SUCCESS] File size : "
        f"{file_size / (1024 * 1024):.2f} MB"
    )


# ============================================================
# 12. WRITE ALL GOLD TABLES
# ============================================================

write_single_csv(
    daily_df,
    DAILY_PATH,
    "daily_taxi_metrics"
)

write_single_csv(
    hourly_df,
    HOURLY_PATH,
    "hourly_taxi_metrics"
)

write_single_csv(
    borough_df,
    BOROUGH_PATH,
    "borough_metrics"
)

write_single_csv(
    payment_df,
    PAYMENT_PATH,
    "payment_metrics"
)


# ============================================================
# 13. FINAL VERIFICATION
# ============================================================

print("\n" + "=" * 60)
print("GOLD DATA VERIFICATION")
print("=" * 60)

gold_files = [
    DAILY_PATH,
    HOURLY_PATH,
    BOROUGH_PATH,
    PAYMENT_PATH
]

for file_path in gold_files:

    if os.path.exists(file_path):

        size_mb = (
            os.path.getsize(file_path)
            / (1024 * 1024)
        )

        print(
            f"[SUCCESS] {file_path} "
            f"({size_mb:.2f} MB)"
        )

    else:

        print(
            f"[ERROR] Missing: {file_path}"
        )


# ============================================================
# 14. SUCCESS
# ============================================================

print("\n" + "=" * 60)
print("GOLD TRANSFORMATION COMPLETED SUCCESSFULLY")
print("=" * 60)

print("Created Gold Tables:")
print("1. daily_taxi_metrics.csv")
print("2. hourly_taxi_metrics.csv")
print("3. borough_metrics.csv")
print("4. payment_metrics.csv")


# ============================================================
# 15. STOP SPARK
# ============================================================

spark.stop()

print("\nSpark session stopped.")