# Databricks notebook source
# MAGIC %md
# MAGIC # 99 — Validation & idempotency evidence
# MAGIC Run this after the pipeline notebooks. Sections 1-6 cover Bronze/Silver (Phase 2), sections 7-12 cover Gold (Phase 3). Screenshots of these results are the evidence for the Phase 2 requirements.
# MAGIC
# MAGIC **Idempotency demo:** run `03_bronze_to_silver_trips` twice on the same month, then check section 4:
# MAGIC the second run must show `rows_inserted = 0` and `rows_updated = 0`, and section 2 must show no duplicates.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
c = dbutils.widgets.get("catalog")

def show(title, sql):
    print(f"\n=== {title}")
    display(spark.sql(sql))

# COMMAND ----------

show("1. Row counts per table", f"""
SELECT 'bronze_trips' AS table_name, COUNT(*) AS row_count FROM {c}.citibike_bronze.bronze_trips UNION ALL
SELECT 'bronze_gbfs_station_status', COUNT(*) FROM {c}.citibike_bronze.bronze_gbfs_station_status UNION ALL
SELECT 'silver_trips', COUNT(*) FROM {c}.citibike_silver.silver_trips UNION ALL
SELECT 'silver_stations', COUNT(*) FROM {c}.citibike_silver.silver_stations UNION ALL
SELECT 'silver_gbfs_station_status', COUNT(*) FROM {c}.citibike_silver.silver_gbfs_station_status UNION ALL
SELECT 'bronze_gbfs_station_info', COUNT(*) FROM {c}.citibike_bronze.bronze_gbfs_station_info UNION ALL
SELECT 'silver_gbfs_station_info', COUNT(*) FROM {c}.citibike_silver.silver_gbfs_station_info UNION ALL
SELECT 'quarantine_records', COUNT(*) FROM {c}.citibike_silver.quarantine_records UNION ALL
SELECT 'pipeline_execution_logs', COUNT(*) FROM {c}.citibike_ops.pipeline_execution_logs""")

# COMMAND ----------

show("2a. Duplicate ride_ids in Silver (expect 0 rows)", f"""
SELECT ride_id, COUNT(*) AS copies FROM {c}.citibike_silver.silver_trips GROUP BY ride_id HAVING COUNT(*) > 1""")

show("2b. Duplicate (station, poll time) in Silver GBFS (expect 0 rows)", f"""
SELECT station_id, poll_timestamp, COUNT(*) AS copies FROM {c}.citibike_silver.silver_gbfs_station_status
GROUP BY station_id, poll_timestamp HAVING COUNT(*) > 1""")

# COMMAND ----------

show("3. Rows without a load_timestamp, per table (expect all zeros)", f"""
SELECT 'bronze_trips' AS table_name, COUNT(*) AS missing FROM {c}.citibike_bronze.bronze_trips WHERE load_timestamp IS NULL UNION ALL
SELECT 'bronze_gbfs_station_status', COUNT(*) FROM {c}.citibike_bronze.bronze_gbfs_station_status WHERE load_timestamp IS NULL UNION ALL
SELECT 'silver_trips', COUNT(*) FROM {c}.citibike_silver.silver_trips WHERE load_timestamp IS NULL UNION ALL
SELECT 'silver_stations', COUNT(*) FROM {c}.citibike_silver.silver_stations WHERE load_timestamp IS NULL UNION ALL
SELECT 'silver_gbfs_station_status', COUNT(*) FROM {c}.citibike_silver.silver_gbfs_station_status WHERE load_timestamp IS NULL UNION ALL
SELECT 'bronze_gbfs_station_info', COUNT(*) FROM {c}.citibike_bronze.bronze_gbfs_station_info WHERE load_timestamp IS NULL UNION ALL
SELECT 'silver_gbfs_station_info', COUNT(*) FROM {c}.citibike_silver.silver_gbfs_station_info WHERE load_timestamp IS NULL UNION ALL
SELECT 'quarantine_records', COUNT(*) FROM {c}.citibike_silver.quarantine_records WHERE load_timestamp IS NULL""")

# COMMAND ----------

show("4. Most recent audit-log entries (idempotency evidence: re-runs show 0 inserted / 0 updated)", f"""
SELECT start_time, pipeline_layer, parameter, status, rows_read, rows_inserted, rows_updated, rows_quarantined, message
FROM {c}.citibike_ops.pipeline_execution_logs ORDER BY start_time DESC LIMIT 30""")

# COMMAND ----------

show("5. Why rows were quarantined", f"""
SELECT entity, quarantine_reason, COUNT(*) AS rows FROM {c}.citibike_silver.quarantine_records
GROUP BY entity, quarantine_reason ORDER BY rows DESC""")

show("6. Silver column types (should be strict: timestamps, doubles, ints)", f"DESCRIBE {c}.citibike_silver.silver_trips")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold layer checks (Phase 3)

# COMMAND ----------

g = f"{c}.citibike_gold"

show("7. Gold row counts", f"""
SELECT 'dim_station' AS table_name, COUNT(*) AS row_count FROM {g}.dim_station UNION ALL
SELECT 'dim_date', COUNT(*) FROM {g}.dim_date UNION ALL
SELECT 'dim_time', COUNT(*) FROM {g}.dim_time UNION ALL
SELECT 'dim_rider_type', COUNT(*) FROM {g}.dim_rider_type UNION ALL
SELECT 'fact_trips', COUNT(*) FROM {g}.fact_trips UNION ALL
SELECT 'fact_station_availability', COUNT(*) FROM {g}.fact_station_availability UNION ALL
SELECT 'gold_daily_ridership_summary', COUNT(*) FROM {g}.gold_daily_ridership_summary UNION ALL
SELECT 'gold_station_popularity', COUNT(*) FROM {g}.gold_station_popularity UNION ALL
SELECT 'gold_hourly_demand_pattern', COUNT(*) FROM {g}.gold_hourly_demand_pattern""")

# COMMAND ----------

show("8. Reconciliation: every Silver trip reaches Gold, and aggregates add up (expect all diffs = 0)", f"""
SELECT 'silver_trips vs fact_trips' AS check_name,
       (SELECT COUNT(*) FROM {c}.citibike_silver.silver_trips) - (SELECT COUNT(*) FROM {g}.fact_trips) AS diff UNION ALL
SELECT 'fact_trips vs SUM(daily.total_trips)',
       (SELECT COUNT(*) FROM {g}.fact_trips) - (SELECT COALESCE(SUM(total_trips), 0) FROM {g}.gold_daily_ridership_summary) UNION ALL
SELECT 'fact_trips vs SUM(station_popularity.departures)',
       (SELECT COUNT(*) FROM {g}.fact_trips) - (SELECT COALESCE(SUM(departures), 0) FROM {g}.gold_station_popularity) UNION ALL
SELECT 'fact_trips vs SUM(hourly.total_trips)',
       (SELECT COUNT(*) FROM {g}.fact_trips) - (SELECT COALESCE(SUM(total_trips), 0) FROM {g}.gold_hourly_demand_pattern)""")

# COMMAND ----------

show("9. Referential integrity: fact keys with no matching dimension row (expect all zeros)", f"""
SELECT 'fact_trips.date_key' AS foreign_key, COUNT(*) AS orphans FROM {g}.fact_trips f LEFT ANTI JOIN {g}.dim_date d ON f.date_key = d.date_key UNION ALL
SELECT 'fact_trips.time_key', COUNT(*) FROM {g}.fact_trips f LEFT ANTI JOIN {g}.dim_time d ON f.time_key = d.time_key UNION ALL
SELECT 'fact_trips.start_station_key', COUNT(*) FROM {g}.fact_trips f LEFT ANTI JOIN {g}.dim_station d ON f.start_station_key = d.station_key UNION ALL
SELECT 'fact_trips.end_station_key', COUNT(*) FROM {g}.fact_trips f LEFT ANTI JOIN {g}.dim_station d ON f.end_station_key = d.station_key UNION ALL
SELECT 'fact_trips.rider_type_key', COUNT(*) FROM {g}.fact_trips f LEFT ANTI JOIN {g}.dim_rider_type d ON f.rider_type_key = d.rider_type_key UNION ALL
SELECT 'fact_station_availability.station_key', COUNT(*) FROM {g}.fact_station_availability f LEFT ANTI JOIN {g}.dim_station d ON f.station_key = d.station_key UNION ALL
SELECT 'fact_station_availability.date_key', COUNT(*) FROM {g}.fact_station_availability f LEFT ANTI JOIN {g}.dim_date d ON f.date_key = d.date_key""")

# COMMAND ----------

show("10. Primary-key uniqueness in Gold (expect 0 rows)", f"""
SELECT 'dim_station' AS table_name, CAST(station_key AS STRING) AS key_value, COUNT(*) AS copies FROM {g}.dim_station GROUP BY station_key HAVING COUNT(*) > 1 UNION ALL
SELECT 'dim_date', CAST(date_key AS STRING), COUNT(*) FROM {g}.dim_date GROUP BY date_key HAVING COUNT(*) > 1 UNION ALL
SELECT 'dim_rider_type', CAST(rider_type_key AS STRING), COUNT(*) FROM {g}.dim_rider_type GROUP BY rider_type_key HAVING COUNT(*) > 1 UNION ALL
SELECT 'fact_trips', ride_id, COUNT(*) FROM {g}.fact_trips GROUP BY ride_id HAVING COUNT(*) > 1 UNION ALL
SELECT 'fact_station_availability', CONCAT(gbfs_station_id, ' @ ', poll_timestamp), COUNT(*) FROM {g}.fact_station_availability
GROUP BY gbfs_station_id, poll_timestamp HAVING COUNT(*) > 1""")

# COMMAND ----------

show("11. Station ID mapping coverage (trip ids <-> live GBFS ids via station_information)", f"""
SELECT has_trip_history, has_live_feed, COUNT(*) AS stations FROM {g}.dim_station
WHERE station_key <> -1 GROUP BY has_trip_history, has_live_feed ORDER BY has_trip_history DESC, has_live_feed DESC""")

show("11b. Share of trips whose start station is linked to the live feed", f"""
SELECT ROUND(100 * AVG(CASE WHEN d.has_live_feed THEN 1 ELSE 0 END), 1) AS pct_trips_with_live_station
FROM {g}.fact_trips f JOIN {g}.dim_station d ON f.start_station_key = d.station_key""")

# COMMAND ----------

show("12. Silver-to-Gold audit entries (rows aggregated = rows_read, execution time = duration_seconds)", f"""
SELECT start_time, parameter, status, rows_read AS rows_aggregated, rows_inserted, rows_updated, duration_seconds, message
FROM {c}.citibike_ops.pipeline_execution_logs
WHERE pipeline_layer IN ('silver_to_gold', 'gold_maintenance') ORDER BY start_time DESC LIMIT 40""")
