# SDP Pipeline 2: NYC Taxi Live Streaming
#
# Mode: Continuous
# Target: nyc_taxi_company.streaming
#
# Tables:
#   ride_requests         - Bronze streaming table (Auto Loader from synthetic JSON)
#   ride_requests_clean   - Silver streaming table (zone-enriched via stream-static join)
#   demand_vs_baseline    - Gold materialized view (15-min tumbling windows vs historical baseline)
#   surge_alerts          - Gold materialized view (filtered surges + event/weather context)

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ADLS landing zone path
STORAGE_ACCOUNT = "azinfrasummit"
CONTAINER = "taxicompany"
BASE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
LANDING_PATH = f"{BASE_PATH}/landing"


# ---------------------------------------------------------------------------
# Bronze: Live Ride Requests (JSON, continuous Auto Loader)
# ---------------------------------------------------------------------------
# Ingests JSONL files dropped by the synthetic generator notebook.
# Each file contains ~50-100 ride request records.

@dp.table(
    name="ride_requests",
    comment="Bronze: raw live ride request events ingested via Auto Loader from synthetic generator."
)
def ride_requests():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "false")
        .load(f"{LANDING_PATH}/synthetic/live_ride_requests/")
        .select(
            F.col("request_id").cast("string"),
            F.col("event_ts").cast("timestamp"),
            F.col("pickup_location_id").cast("int"),
            F.col("dropoff_location_id").cast("int"),
            F.col("passenger_count").cast("int"),
            F.col("requested_vehicle_type").cast("string"),
            F.col("estimated_distance_miles").cast("double"),
            F.col("estimated_fare_amount").cast("double"),
            F.col("request_status").cast("string"),
            F.col("schema_version").cast("string"),
            F.col("_generator_scenario").cast("string"),
        )
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


# ---------------------------------------------------------------------------
# Silver: Zone-Enriched Ride Requests (streaming table, stream-static join)
# ---------------------------------------------------------------------------
# Joins live requests with the taxi zone lookup for borough/zone names.
# Adds derived time fields for downstream windowed aggregation.

@dp.table(
    name="ride_requests_clean",
    comment="Silver: zone-enriched live ride requests with pickup/dropoff borough and zone names."
)
def ride_requests_clean():
    raw = spark.readStream.table("ride_requests")
    zones = spark.read.table("nyc_taxi_company.silver.taxi_zones")

    pickup_zones = zones.select(
        F.col("location_id").alias("_pu_loc"),
        F.col("borough").alias("pickup_borough"),
        F.col("zone_name").alias("pickup_zone"),
    )
    dropoff_zones = zones.select(
        F.col("location_id").alias("_do_loc"),
        F.col("borough").alias("dropoff_borough"),
        F.col("zone_name").alias("dropoff_zone"),
    )

    return (
        raw
        .join(pickup_zones, raw.pickup_location_id == pickup_zones._pu_loc, "left")
        .join(dropoff_zones, raw.dropoff_location_id == dropoff_zones._do_loc, "left")
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("event_hour", F.hour("event_ts"))
        .select(
            "request_id",
            "event_ts",
            "event_date",
            "event_hour",
            "pickup_location_id",
            "pickup_borough",
            "pickup_zone",
            "dropoff_location_id",
            "dropoff_borough",
            "dropoff_zone",
            "passenger_count",
            "requested_vehicle_type",
            "estimated_distance_miles",
            "estimated_fare_amount",
            "request_status",
            "schema_version",
        )
    )


# ---------------------------------------------------------------------------
# Gold: Live Demand vs Historical Baseline (materialized view)
# ---------------------------------------------------------------------------
# Compares live ride request counts in 15-minute tumbling windows against
# the historical zone-hour demand baseline.
# Only the last 24 hours are computed to keep the MV manageable.
# Baseline is scaled from hourly to 15-min (÷4) for fair comparison.

@dp.materialized_view(
    name="demand_vs_baseline",
    comment="Gold: 15-minute windowed live demand compared against historical zone-hour baseline."
)
def demand_vs_baseline():
    clean = spark.read.table("ride_requests_clean")
    baseline = spark.read.table("nyc_taxi_company.gold.zone_hour_demand_baseline")

    # Focus on recent data only (last 24h)
    recent = clean.filter(
        F.col("event_ts") >= F.current_timestamp() - F.expr("INTERVAL 24 HOURS")
    )

    # 15-minute tumbling windows per pickup zone
    windowed = (
        recent
        .groupBy(
            F.window("event_ts", "15 minutes").alias("w"),
            "pickup_location_id",
            "pickup_borough",
            "pickup_zone",
        )
        .agg(F.count("*").alias("live_request_count"))
        .select(
            F.col("w.start").alias("window_start_ts"),
            F.col("w.end").alias("window_end_ts"),
            "pickup_location_id",
            "pickup_borough",
            "pickup_zone",
            "live_request_count",
            F.month("w.start").alias("_month"),
            F.dayofweek("w.start").alias("_dow"),
            F.hour("w.start").alias("_hour"),
        )
    )

    # Historical baseline scaled to 15-min granularity (hourly avg ÷ 4)
    baseline_15m = baseline.select(
        F.col("pickup_location_id").alias("_bl_loc"),
        F.col("pickup_month").alias("_bl_month"),
        F.col("pickup_day_of_week").alias("_bl_dow"),
        F.col("pickup_hour").alias("_bl_hour"),
        (F.col("avg_trip_count") / F.lit(4.0)).alias("baseline_avg_trip_count"),
    )

    return (
        windowed
        .join(
            baseline_15m,
            (F.col("pickup_location_id") == F.col("_bl_loc"))
            & (F.col("_month") == F.col("_bl_month"))
            & (F.col("_dow") == F.col("_bl_dow"))
            & (F.col("_hour") == F.col("_bl_hour")),
            "left",
        )
        .withColumn(
            "demand_ratio",
            F.when(
                F.col("baseline_avg_trip_count") > 0,
                F.round(F.col("live_request_count") / F.col("baseline_avg_trip_count"), 2),
            ),
        )
        .withColumn(
            "demand_lift_pct",
            F.when(
                F.col("baseline_avg_trip_count") > 0,
                F.round(
                    ((F.col("live_request_count") - F.col("baseline_avg_trip_count"))
                     / F.col("baseline_avg_trip_count")) * 100.0,
                    1,
                ),
            ),
        )
        .withColumn(
            "anomaly_band",
            F.when(F.col("demand_ratio") >= 2.0, "Critical")
             .when(F.col("demand_ratio") >= 1.5, "High")
             .when(F.col("demand_ratio") >= 1.2, "Elevated")
             .when(F.col("demand_ratio").isNull(), "No Baseline")
             .when(F.col("demand_ratio") <= 0.5, "Low")
             .otherwise("Normal"),
        )
        .select(
            "window_start_ts",
            "window_end_ts",
            "pickup_location_id",
            "pickup_borough",
            "pickup_zone",
            "live_request_count",
            "baseline_avg_trip_count",
            "demand_ratio",
            "demand_lift_pct",
            "anomaly_band",
        )
    )


# ---------------------------------------------------------------------------
# Gold: Live Surge Alerts (materialized view)
# ---------------------------------------------------------------------------
# Filters windows where demand is Elevated/High/Critical, then enriches with:
#   - Sports event context (which event, what phase) via venue catchment zones
#   - Weather context (rain/snow/cold day)
# Computes a human-readable alert_reason explaining the surge.

@dp.materialized_view(
    name="surge_alerts",
    comment="Gold: live surge alerts enriched with sports event and weather context."
)
def surge_alerts():
    demand = spark.read.table("demand_vs_baseline")
    events = spark.read.table("nyc_taxi_company.silver.sports_events")
    catchment = spark.read.table("nyc_taxi_company.silver.venue_zone_catchment")
    weather = spark.read.table("nyc_taxi_company.silver.weather_daily")

    # Only elevated+ anomalies
    surges = demand.filter(F.col("anomaly_band").isin("Elevated", "High", "Critical"))

    # Build event-catchment lookup: which zones are near which venues
    event_zones = (
        events.alias("e")
        .join(
            catchment.select(
                "venue_name",
                F.col("location_id").alias("catchment_loc_id"),
            ),
            "venue_name",
        )
        .select(
            F.col("e.event_id").alias("related_event_id"),
            F.col("e.event_name").alias("related_event_name"),
            F.col("e.venue_name").alias("related_venue_name"),
            F.col("e.event_start_ts_local"),
            F.col("e.estimated_event_end_ts_local"),
            F.col("e.analysis_window_start_ts"),
            F.col("e.analysis_window_end_ts"),
            "catchment_loc_id",
        )
    )

    # Spatial + temporal join: surge zone in event catchment
    # AND window overlaps event analysis window
    with_events = (
        surges.alias("s")
        .join(
            event_zones.alias("ev"),
            (F.col("s.pickup_location_id") == F.col("ev.catchment_loc_id"))
            & (F.col("s.window_start_ts") >= F.col("ev.analysis_window_start_ts"))
            & (F.col("s.window_end_ts") <= F.col("ev.analysis_window_end_ts")),
            "left",
        )
        .withColumn(
            "event_phase",
            F.when(F.col("related_event_id").isNull(), None)
             .when(F.col("s.window_end_ts") <= F.col("ev.event_start_ts_local"), "Pre-event")
             .when(F.col("s.window_start_ts") >= F.col("ev.estimated_event_end_ts_local"), "Post-event")
             .otherwise("During-event"),
        )
    )

    # Weather join on window date
    with_weather = with_events.join(
        weather.select("weather_date", "weather_condition"),
        F.to_date(F.col("s.window_start_ts")) == F.col("weather_date"),
        "left",
    )

    # Compute human-readable alert reason
    return (
        with_weather
        .withColumn(
            "alert_reason",
            F.when(
                F.col("related_event_id").isNotNull()
                & F.col("weather_condition").isin("Snow", "Rain"),
                F.concat(
                    F.lit("Surge near "),
                    F.col("related_venue_name"),
                    F.lit(" ("),
                    F.col("event_phase"),
                    F.lit(") + "),
                    F.col("weather_condition"),
                    F.lit(" weather"),
                ),
            )
            .when(
                F.col("related_event_id").isNotNull(),
                F.concat(
                    F.lit("Surge near "),
                    F.col("related_venue_name"),
                    F.lit(" \u2014 "),
                    F.col("related_event_name"),
                    F.lit(" ("),
                    F.col("event_phase"),
                    F.lit(")"),
                ),
            )
            .when(
                F.col("weather_condition").isin("Snow", "Rain"),
                F.concat(F.col("weather_condition"), F.lit(" day demand surge")),
            )
            .otherwise("Unexplained demand anomaly"),
        )
        .select(
            "window_start_ts",
            "window_end_ts",
            "pickup_location_id",
            "pickup_borough",
            "pickup_zone",
            "live_request_count",
            "baseline_avg_trip_count",
            "demand_ratio",
            "anomaly_band",
            "related_event_id",
            "related_event_name",
            "related_venue_name",
            "event_phase",
            "weather_condition",
            "alert_reason",
        )
    )
