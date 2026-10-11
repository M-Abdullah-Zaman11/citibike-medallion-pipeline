# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Bronze → Silver (live GBFS station status)
# MAGIC Casts counts to INT, flags to BOOLEAN (accepts `1/0` and `True/False`), epoch seconds to TIMESTAMP,
# MAGIC quarantines invalid snapshots, and upserts with `MERGE INTO` keyed on `(station_id, poll_timestamp)`.
# MAGIC Processes one poll-date at a time, so any date range can be backfilled.

# COMMAND ----------
# MAGIC %run ./01_common

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.dropdown("load_type", "incremental", ["full", "incremental"], "Load type (label for audit)")
dbutils.widgets.text("start_date", "", "Start poll date YYYY-MM-DD (blank = earliest in Bronze)")
dbutils.widgets.text("end_date", "", "End poll date YYYY-MM-DD (blank = latest in Bronze)")
dbutils.widgets.text("batch_id", "", "Batch ID (blank = auto)")
dbutils.widgets.dropdown("fail_on_error", "false", ["false", "true"], "Raise an error at the END if any step failed")

# COMMAND ----------

from datetime import date, timedelta

cfg = get_config(dbutils.widgets.get("catalog"))
load_type = dbutils.widgets.get("load_type")
ctx = new_context("05_gbfs_bronze_to_silver", load_type, dbutils.widgets.get("batch_id"))

bronze_all = spark.table(cfg["bronze_gbfs"])
poll_date = F.to_date(F.expr("try_cast(poll_timestamp_utc as timestamp)"))
lo, hi = bronze_all.agg(F.min(poll_date), F.max(poll_date)).collect()[0]
if lo is None:
    raise ValueError("bronze_gbfs_station_status has no readable snapshots - run 04_gbfs_raw_to_bronze first")

start = date.fromisoformat(dbutils.widgets.get("start_date").strip()) if dbutils.widgets.get("start_date").strip() else lo
end = date.fromisoformat(dbutils.widgets.get("end_date").strip()) if dbutils.widgets.get("end_date").strip() else hi
print(f"batch_id={ctx['batch_id']} | {start} -> {end}")

# COMMAND ----------

def process_day(d):
    # rows whose timestamp cannot be parsed belong to no day, so they are included in every run
    # (they end up in quarantine, and the quarantine MERGE makes that repeat harmless)
    bronze = bronze_all.where((poll_date == F.lit(d)) | poll_date.isNull())
    rows_read = bronze.count()
    if rows_read == 0:
        return {"rows_read": 0, "rows_inserted": 0, "rows_updated": 0, "message": "no snapshots on this date"}
    valid, quarantine = transform_gbfs_silver(bronze, ctx["batch_id"])
    n_quarantined = quarantine.count()
    res = merge_into(cfg["silver_gbfs"], valid, ["station_id", "poll_timestamp"])
    merge_into(cfg["quarantine"], quarantine, ["entity", "record_hash"], update_when_changed=False)
    return {"rows_read": rows_read, **res, "rows_quarantined": n_quarantined}


results = []
d = start
while d <= end:
    results.append(run_step(cfg, ctx, "bronze_to_silver_gbfs", f"poll_date={d}", lambda x=d: process_day(x)))
    d += timedelta(days=1)

# COMMAND ----------

failed = [r for r in results if r[0] == "FAILURE"]
print(f"Done: {len(results) - len(failed)} succeeded, {len(failed)} failed")
display(spark.table(cfg["logs"]).where(F.col("batch_id") == ctx["batch_id"]).orderBy("start_time"))
if failed and dbutils.widgets.get("fail_on_error") == "true":
    raise Exception(f"{len(failed)} step(s) failed - see pipeline_execution_logs for batch {ctx['batch_id']}")
