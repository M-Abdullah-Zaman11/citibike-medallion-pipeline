# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Silver → Gold (live station availability)
# MAGIC Runs automatically after `05_gbfs_bronze_to_silver` with the **same date range**.
# MAGIC Builds `fact_station_availability` — a periodic-snapshot fact (one row per installed station per hourly poll) —
# MAGIC linked to the same `dim_station` as `fact_trips` through the GBFS id ↔ short id mapping from `station_information`.
# MAGIC
# MAGIC * `MERGE INTO` keyed on `(gbfs_station_id, poll_timestamp)`, one poll-date at a time → idempotent and backfillable.
# MAGIC * Poll times are converted from UTC to New York time so availability and trips share `dim_date` / `dim_time`.
# MAGIC * Stations with `is_installed = false` are excluded, and placeholder `last_reported` values (1970) become NULL.

# COMMAND ----------
# MAGIC %run ./01_common

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.dropdown("load_type", "incremental", ["full", "incremental"], "Load type (label for audit)")
dbutils.widgets.text("start_date", "", "Start poll date YYYY-MM-DD, UTC (blank = earliest in Silver)")
dbutils.widgets.text("end_date", "", "End poll date YYYY-MM-DD, UTC (blank = latest in Silver)")
dbutils.widgets.text("batch_id", "", "Batch ID (blank = auto)")
dbutils.widgets.dropdown("fail_on_error", "false", ["false", "true"], "Raise an error at the END if any step failed")

# COMMAND ----------

from datetime import date, timedelta

spark.conf.set("spark.sql.session.timeZone", "UTC")   # required by from_utc_timestamp in build_fact_station_availability
cfg = get_config(dbutils.widgets.get("catalog"))
load_type = dbutils.widgets.get("load_type")
ctx = new_context("08_silver_to_gold_gbfs", load_type, dbutils.widgets.get("batch_id"))

silver_all = spark.table(cfg["silver_gbfs"])
poll_date = F.to_date("poll_timestamp")
lo, hi = silver_all.agg(F.min(poll_date), F.max(poll_date)).collect()[0]
if lo is None:
    raise ValueError("silver_gbfs_station_status is empty - run 05_gbfs_bronze_to_silver first")
start = date.fromisoformat(dbutils.widgets.get("start_date").strip()) if dbutils.widgets.get("start_date").strip() else lo
end = date.fromisoformat(dbutils.widgets.get("end_date").strip()) if dbutils.widgets.get("end_date").strip() else hi
print(f"batch_id={ctx['batch_id']} | {start} -> {end}")

# COMMAND ----------
# MAGIC %md ### Step 1 — Conformed dimensions (picks up any new station_information first)

# COMMAND ----------

results = refresh_dimensions(cfg, ctx)

# COMMAND ----------
# MAGIC %md ### Step 2 — `fact_station_availability` (one MERGE per poll date)

# COMMAND ----------

def load_day(d):
    silver = silver_all.where(poll_date == F.lit(d))
    rows_read = silver.count()
    if rows_read == 0:
        return {"rows_read": 0, "rows_inserted": 0, "rows_updated": 0, "message": "no snapshots on this date"}
    fact = build_fact_station_availability(silver, spark.table(cfg["dim_station"]), ctx["batch_id"])
    n_written = fact.count()
    n_unmapped = fact.where(F.col("station_key") == UNKNOWN_KEY).count()
    res = merge_into(cfg["fact_availability"], fact, ["gbfs_station_id", "poll_timestamp"])
    return {"rows_read": rows_read, **res,
            "message": f"{rows_read - n_written} not-installed snapshots excluded; {n_unmapped} rows without a station mapping"}


d = start
while d <= end:
    results.append(run_step(cfg, ctx, "silver_to_gold", f"fact_station_availability poll_date={d}", lambda x=d: load_day(x)))
    d += timedelta(days=1)

# COMMAND ----------

failed = [r for r in results if r[0] == "FAILURE"]
print(f"Done: {len(results) - len(failed)} succeeded, {len(failed)} failed")
display(spark.table(cfg["logs"]).where(F.col("batch_id") == ctx["batch_id"]).orderBy("start_time"))
if failed and dbutils.widgets.get("fail_on_error") == "true":
    raise Exception(f"{len(failed)} step(s) failed - see pipeline_execution_logs for batch {ctx['batch_id']}")
