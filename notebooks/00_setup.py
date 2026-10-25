# Databricks notebook source
# MAGIC %md
# MAGIC # 00_setup — one-time, safe to re-run
# MAGIC Creates the schemas, the landing volume, and every Bronze / Silver / operational table with **explicit column types**.
# MAGIC Re-running changes nothing (`IF NOT EXISTS`), so it is also how an existing Phase 2 workspace is upgraded to Phase 3.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

statements = [
    f"CREATE SCHEMA IF NOT EXISTS {catalog}.citibike_raw",
    f"CREATE SCHEMA IF NOT EXISTS {catalog}.citibike_bronze",
    f"CREATE SCHEMA IF NOT EXISTS {catalog}.citibike_silver",
    f"CREATE SCHEMA IF NOT EXISTS {catalog}.citibike_ops",
    f"CREATE SCHEMA IF NOT EXISTS {catalog}.citibike_gold",
    f"CREATE VOLUME IF NOT EXISTS {catalog}.citibike_raw.landing",

    # ---------------- BRONZE: raw as received, every source field is STRING ----------------
    f"""CREATE TABLE IF NOT EXISTS {catalog}.citibike_bronze.bronze_trips (
        ride_id STRING, rideable_type STRING, started_at STRING, ended_at STRING,
        start_station_name STRING, start_station_id STRING, end_station_name STRING, end_station_id STRING,
        start_lat STRING, start_lng STRING, end_lat STRING, end_lng STRING, member_casual STRING,
        _corrupt_record STRING, _extra_columns STRING,
        source_file STRING, source_month STRING, batch_id STRING, load_type STRING, load_timestamp TIMESTAMP)""",

    f"""CREATE TABLE IF NOT EXISTS {catalog}.citibike_bronze.bronze_gbfs_station_status (
        poll_timestamp_utc STRING, station_id STRING, num_bikes_available STRING, num_ebikes_available STRING,
        num_docks_available STRING, is_installed STRING, is_renting STRING, is_returning STRING, last_reported STRING,
        _corrupt_record STRING, _extra_columns STRING,
        source_file STRING, batch_id STRING, load_type STRING, load_timestamp TIMESTAMP)""",

    # ---------------- SILVER: typed, validated, de-duplicated ----------------
    f"""CREATE TABLE IF NOT EXISTS {catalog}.citibike_silver.silver_trips (
        ride_id STRING, rideable_type STRING, started_at TIMESTAMP, ended_at TIMESTAMP,
        start_station_name STRING, start_station_id STRING, end_station_name STRING, end_station_id STRING,
        start_lat DOUBLE, start_lng DOUBLE, end_lat DOUBLE, end_lng DOUBLE, member_casual STRING,
        trip_duration_seconds BIGINT, trip_date DATE, trip_hour INT, day_of_week INT,
        row_hash STRING, source_file STRING, source_month STRING, batch_id STRING, load_timestamp TIMESTAMP)""",

    f"""CREATE TABLE IF NOT EXISTS {catalog}.citibike_silver.silver_stations (
        station_id STRING, station_name STRING, latitude DOUBLE, longitude DOUBLE,
        row_hash STRING, batch_id STRING, load_timestamp TIMESTAMP)""",

    f"""CREATE TABLE IF NOT EXISTS {catalog}.citibike_silver.silver_gbfs_station_status (
        station_id STRING, poll_timestamp TIMESTAMP, num_bikes_available INT, num_ebikes_available INT,
        num_docks_available INT, is_installed BOOLEAN, is_renting BOOLEAN, is_returning BOOLEAN,
        last_reported_ts TIMESTAMP,
        row_hash STRING, source_file STRING, batch_id STRING, load_timestamp TIMESTAMP)""",

    f"""CREATE TABLE IF NOT EXISTS {catalog}.citibike_bronze.bronze_gbfs_station_info (
        snapshot_timestamp_utc STRING, station_id STRING, short_name STRING, name STRING,
        lat STRING, lon STRING, capacity STRING, region_id STRING,
        _corrupt_record STRING, _extra_columns STRING,
        source_file STRING, batch_id STRING, load_type STRING, load_timestamp TIMESTAMP)""",

    f"""CREATE TABLE IF NOT EXISTS {catalog}.citibike_silver.silver_gbfs_station_info (
        gbfs_station_id STRING, short_name STRING, station_name STRING, latitude DOUBLE, longitude DOUBLE,
        capacity INT, region_id STRING, info_as_of TIMESTAMP,
        row_hash STRING, source_file STRING, batch_id STRING, load_timestamp TIMESTAMP)""",

    f"""CREATE TABLE IF NOT EXISTS {catalog}.citibike_silver.quarantine_records (
        entity STRING, record_hash STRING, record_key STRING, source_file STRING, batch_id STRING,
        quarantine_reason STRING, raw_record STRING, load_timestamp TIMESTAMP)""",

    # ---------------- GOLD: star schema (liquid clustering on the columns dashboards filter by) ----------------
    f"""CREATE TABLE IF NOT EXISTS {catalog}.citibike_gold.dim_station (
        station_key BIGINT, station_id STRING, station_name STRING, latitude DOUBLE, longitude DOUBLE,
        gbfs_station_id STRING, capacity INT, has_trip_history BOOLEAN, has_live_feed BOOLEAN,
        row_hash STRING, batch_id STRING, load_timestamp TIMESTAMP)""",

    f"""CREATE TABLE IF NOT EXISTS {catalog}.citibike_gold.dim_date (
        date_key INT, full_date DATE, year INT, quarter INT, quarter_label STRING, month INT, month_name STRING,
        month_short STRING, year_month STRING, year_month_key INT, day_of_month INT, day_of_week INT,
        day_of_week_mon INT, day_name STRING, day_short STRING, is_weekend BOOLEAN, week_of_year INT, season STRING,
        row_hash STRING, batch_id STRING, load_timestamp TIMESTAMP)""",

    f"""CREATE TABLE IF NOT EXISTS {catalog}.citibike_gold.dim_time (
        time_key INT, hour_label STRING, hour_12_label STRING, time_of_day STRING, time_of_day_sort INT,
        is_commute_peak BOOLEAN, row_hash STRING, batch_id STRING, load_timestamp TIMESTAMP)""",

    f"""CREATE TABLE IF NOT EXISTS {catalog}.citibike_gold.dim_rider_type (
        rider_type_key BIGINT, member_casual STRING, rideable_type STRING, member_label STRING, bike_label STRING,
        rider_type_label STRING, row_hash STRING, batch_id STRING, load_timestamp TIMESTAMP)""",

    f"""CREATE TABLE IF NOT EXISTS {catalog}.citibike_gold.fact_trips (
        ride_id STRING, date_key INT, time_key INT, start_station_key BIGINT, end_station_key BIGINT,
        rider_type_key BIGINT, started_at TIMESTAMP, ended_at TIMESTAMP, trip_duration_seconds BIGINT,
        trip_duration_minutes DOUBLE, distance_km DOUBLE, is_round_trip BOOLEAN, source_month STRING,
        row_hash STRING, batch_id STRING, load_timestamp TIMESTAMP)
        CLUSTER BY (date_key, start_station_key)""",

    f"""CREATE TABLE IF NOT EXISTS {catalog}.citibike_gold.fact_station_availability (
        gbfs_station_id STRING, poll_timestamp TIMESTAMP, poll_timestamp_local TIMESTAMP, station_key BIGINT,
        date_key INT, time_key INT, bikes_available INT, ebikes_available INT, classic_bikes_available INT,
        docks_available INT, capacity INT, fill_ratio DOUBLE, is_empty BOOLEAN, is_full BOOLEAN,
        is_renting BOOLEAN, is_returning BOOLEAN, last_reported_ts TIMESTAMP,
        row_hash STRING, batch_id STRING, load_timestamp TIMESTAMP)
        CLUSTER BY (date_key, station_key)""",

    f"""CREATE TABLE IF NOT EXISTS {catalog}.citibike_gold.gold_daily_ridership_summary (
        date_key INT, total_trips BIGINT, member_trips BIGINT, casual_trips BIGINT, classic_trips BIGINT,
        electric_trips BIGINT, round_trips BIGINT, avg_duration_minutes DOUBLE, median_duration_minutes DOUBLE,
        total_distance_km DOUBLE, avg_distance_km DOUBLE, member_share DOUBLE,
        batch_id STRING, load_timestamp TIMESTAMP)
        CLUSTER BY (date_key)""",

    f"""CREATE TABLE IF NOT EXISTS {catalog}.citibike_gold.gold_station_popularity (
        year_month_key INT, station_key BIGINT, departures BIGINT, arrivals BIGINT, net_flow BIGINT,
        total_activity BIGINT, member_departures BIGINT, casual_departures BIGINT,
        avg_departure_duration_minutes DOUBLE, activity_rank INT, batch_id STRING, load_timestamp TIMESTAMP)
        CLUSTER BY (year_month_key, station_key)""",

    f"""CREATE TABLE IF NOT EXISTS {catalog}.citibike_gold.gold_hourly_demand_pattern (
        day_of_week INT, day_of_week_mon INT, day_name STRING, time_key INT, total_trips BIGINT,
        member_trips BIGINT, casual_trips BIGINT, days_observed BIGINT, avg_trips_per_day DOUBLE,
        avg_duration_minutes DOUBLE, batch_id STRING, load_timestamp TIMESTAMP)""",

    # ---------------- OPS: audit log ----------------
    f"""CREATE TABLE IF NOT EXISTS {catalog}.citibike_ops.pipeline_execution_logs (
        run_id STRING, batch_id STRING, pipeline_layer STRING, notebook STRING, load_type STRING,
        parameter STRING, start_time TIMESTAMP, end_time TIMESTAMP, duration_seconds DOUBLE, status STRING,
        rows_read BIGINT, rows_inserted BIGINT, rows_updated BIGINT, rows_quarantined BIGINT,
        message STRING, load_timestamp TIMESTAMP)""",
]

for stmt in statements:
    spark.sql(stmt)
    print("OK:", " ".join(stmt.split())[:90])

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {catalog}.citibike_bronze"))
display(spark.sql(f"SHOW TABLES IN {catalog}.citibike_silver"))
display(spark.sql(f"SHOW TABLES IN {catalog}.citibike_ops"))
display(spark.sql(f"SHOW TABLES IN {catalog}.citibike_gold"))
