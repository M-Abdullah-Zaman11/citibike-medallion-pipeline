# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Bronze → Silver (trip history)
# MAGIC Casts to strict types, validates, de-duplicates, and **upserts with `MERGE INTO`** into `silver_trips`.
# MAGIC
# MAGIC * **Idempotent:** a row is inserted only if its `ride_id` is new, and updated only if its content changed (`row_hash`).
# MAGIC   Running the same month twice reports `rows_inserted = 0` and `rows_updated = 0` the second time.
# MAGIC * **Quarantine:** rows that break a rule go to `quarantine_records` with the reason, instead of being dropped or crashing the batch.
# MAGIC * **Parameterized:** pass any month range to backfill from Bronze.

# COMMAND ----------
# MAGIC %run ./01_common

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.dropdown("load_type", "full", ["full", "incremental"], "Load type (label for audit)")
dbutils.widgets.text("start_month", "202603", "Start month (YYYYMM)")
dbutils.widgets.text("end_month", "202603", "End month (YYYYMM, inclusive)")
dbutils.widgets.text("valid_member_types", "member,casual", "Accepted member_casual values")
dbutils.widgets.text("max_trip_hours", "24", "Longest plausible trip (hours)")
dbutils.widgets.text("batch_id", "", "Batch ID (blank = auto)")
dbutils.widgets.dropdown("fail_on_error", "false", ["false", "true"], "Raise an error at the END if any step failed")

# COMMAND ----------

cfg = get_config(dbutils.widgets.get("catalog"))
load_type = dbutils.widgets.get("load_type")
months = month_range(dbutils.widgets.get("start_month").strip(), dbutils.widgets.get("end_month").strip())
member_types = tuple(x.strip().lower() for x in dbutils.widgets.get("valid_member_types").split(",") if x.strip())
max_seconds = int(float(dbutils.widgets.get("max_trip_hours")) * 3600)
ctx = new_context("03_bronze_to_silver_trips", load_type, dbutils.widgets.get("batch_id"))
print(f"batch_id={ctx['batch_id']} | load_type={load_type} | months={months}")

# COMMAND ----------

def process_month(month):
    bronze = spark.table(cfg["bronze_trips"]).where(F.col("source_month") == month)
    rows_read = bronze.count()
    if rows_read == 0:
        raise ValueError(f"No Bronze rows for {month} - run 02_raw_to_bronze_trips for that month first")
    valid, quarantine = transform_trips_silver(bronze, ctx["batch_id"], member_types, max_seconds)
    n_quarantined = quarantine.count()
    trips = merge_into(cfg["silver_trips"], valid, ["ride_id"])
    merge_into(cfg["quarantine"], quarantine, ["entity", "record_hash"], update_when_changed=False)
    return {"rows_read": rows_read, **trips, "rows_quarantined": n_quarantined}


def process_stations(month):
    trips = spark.table(cfg["silver_trips"]).where(F.col("source_month") == month)
    stations = derive_stations(trips, ctx["batch_id"])
    res = merge_into(cfg["silver_stations"], stations, ["station_id"])
    return {"rows_read": stations.count(), **res}


results = []
for month in months:
    outcome = run_step(cfg, ctx, "bronze_to_silver", f"trips month={month}", lambda m=month: process_month(m))
    results.append(outcome)
    if outcome[0] == "SUCCESS":
        results.append(run_step(cfg, ctx, "bronze_to_silver_stations", f"stations month={month}", lambda m=month: process_stations(m)))

# COMMAND ----------

failed = [r for r in results if r[0] == "FAILURE"]
print(f"Done: {len(results) - len(failed)} succeeded, {len(failed)} failed")
display(spark.table(cfg["logs"]).where(F.col("batch_id") == ctx["batch_id"]).orderBy("start_time"))
if failed and dbutils.widgets.get("fail_on_error") == "true":
    raise Exception(f"{len(failed)} step(s) failed - see pipeline_execution_logs for batch {ctx['batch_id']}")
