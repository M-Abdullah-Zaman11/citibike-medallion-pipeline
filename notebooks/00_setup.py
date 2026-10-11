# Databricks notebook source
# MAGIC %md
# MAGIC # 00_setup — one-time, safe to re-run
# MAGIC Creates the schemas, the landing volume, and every Bronze / Silver / operational table with **explicit column types**.
# MAGIC Re-running changes nothing (`IF NOT EXISTS`).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

statements = [
    f"CREATE SCHEMA IF NOT EXISTS {catalog}.citibike_raw",
    f"CREATE SCHEMA IF NOT EXISTS {catalog}.citibike_bronze",
    f"CREATE SCHEMA IF NOT EXISTS {catalog}.citibike_silver",
    f"CREATE SCHEMA IF NOT EXISTS {catalog}.citibike_ops",
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

    f"""CREATE TABLE IF NOT EXISTS {catalog}.citibike_silver.quarantine_records (
        entity STRING, record_hash STRING, record_key STRING, source_file STRING, batch_id STRING,
        quarantine_reason STRING, raw_record STRING, load_timestamp TIMESTAMP)""",

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
