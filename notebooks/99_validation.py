# Databricks notebook source
# MAGIC %md
# MAGIC # 99 — Validation & idempotency evidence
# MAGIC Run this after the pipeline notebooks. Screenshots of these results are the evidence for the Phase 2 requirements.
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
