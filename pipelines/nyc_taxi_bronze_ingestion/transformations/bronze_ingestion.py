# SDP Pipeline 1: Bronze Auto Loader Ingestion
#
# Mode: Triggered (daily schedule or on-demand)
# Target: nyc_taxi_company.bronze
# Ingestion: Auto Loader (cloudFiles) for incremental file processing
#
# Tables:
#   yellow_taxi_trips  - Parquet, monthly TLC files (Hive-partitioned year/month)
#   taxi_zones         - CSV, one-time TLC zone lookup
#   weather_daily      - JSON, NOAA daily observations
#   sports_events_raw  - JSON, curated NYC sports event schedules

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ADLS landing zone paths
STORAGE_ACCOUNT = "azinfrasummit"
CONTAINER = "taxicompany"
BASE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

LANDING_PATH = f"{BASE_PATH}/landing"


# ---------------------------------------------------------------------------
# Bronze: Yellow Taxi Trips (Parquet, incremental)
# ---------------------------------------------------------------------------
# Auto Loader incrementally ingests new Parquet files as they land.
# Hive-style partitions (year=YYYY/month=MM) are automatically picked up as columns.

@dp.table(
    name="yellow_taxi_trips",
    comment="Bronze: raw NYC TLC Yellow Taxi trip records ingested via Auto Loader from ADLS landing zone.",
    table_properties={"delta.feature.timestampNtz": "supported"}
)
def yellow_taxi_trips():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{LANDING_PATH}/nyc_taxi/yellow/")
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


# ---------------------------------------------------------------------------
# Bronze: Taxi Zones (CSV, one-time)
# ---------------------------------------------------------------------------
# One-time ingestion of the TLC taxi zone lookup CSV.
# Auto Loader ensures idempotent processing — re-running won't duplicate rows.

@dp.table(
    name="taxi_zones",
    comment="Bronze: TLC taxi zone lookup table mapping LocationID to borough, zone name, and service zone."
)
def taxi_zones():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{LANDING_PATH}/nyc_taxi/zones/")
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


# ---------------------------------------------------------------------------
# Bronze: Weather Daily (JSON, incremental)
# ---------------------------------------------------------------------------
# Incrementally ingests NOAA daily weather observation JSON files.
# New weather data files dropped into the landing zone are picked up on each trigger.

@dp.table(
    name="weather_daily",
    comment="Bronze: NOAA daily weather observations for NYC Central Park station, ingested via Auto Loader."
)
def weather_daily():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "false")
        .load(f"{LANDING_PATH}/weather/")
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


# ---------------------------------------------------------------------------
# Bronze: Sports Events (JSON, incremental)
# ---------------------------------------------------------------------------
# Incrementally ingests curated NYC sports event schedule JSON files.
# Covers NBA (Knicks, Nets), NHL (Rangers), MLB (Yankees, Mets) home games.

@dp.table(
    name="sports_events_raw",
    comment="Bronze: curated NYC sports event schedule for taxi demand analysis, ingested via Auto Loader."
)
def sports_events_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "false")
        .load(f"{LANDING_PATH}/events/")
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )