# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Raw → Bronze (trip history)
# MAGIC Reads every CSV in `<landing>/trips/<YYYYMM>/` using the **explicit schema contract** (no `inferSchema`),
# MAGIC adds lineage metadata, and writes to `bronze_trips`.
# MAGIC
# MAGIC * **Idempotent:** each file's rows are replaced (`replaceWhere` on `source_file`), so re-running a file never duplicates it.
# MAGIC * **Parameterized:** any month range can be (re)loaded; nothing is hardcoded to "today".
# MAGIC * **Fault tolerant:** a bad file is logged as FAILURE and the remaining files still load.
# MAGIC * **Drift aware:** extra / missing / reordered columns are detected per file and handled, never fatal.

# COMMAND ----------
# MAGIC %run ./01_common

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.dropdown("load_type", "full", ["full", "incremental"], "Load type (label for audit)")
dbutils.widgets.text("start_month", "202603", "Start month (YYYYMM)")
dbutils.widgets.text("end_month", "202603", "End month (YYYYMM, inclusive)")
dbutils.widgets.text("landing_path", "", "Landing path override (blank = default volume)")
dbutils.widgets.dropdown("evolve_schema", "false", ["false", "true"], "On new columns: evolve table (true) or capture as JSON (false)")
dbutils.widgets.text("batch_id", "", "Batch ID (blank = auto)")
dbutils.widgets.dropdown("fail_on_error", "false", ["false", "true"], "Raise an error at the END if any step failed")

# COMMAND ----------

import os

cfg = get_config(dbutils.widgets.get("catalog"))
load_type = dbutils.widgets.get("load_type")
evolve_schema = dbutils.widgets.get("evolve_schema") == "true"
landing = dbutils.widgets.get("landing_path").strip() or cfg["landing"]
months = month_range(dbutils.widgets.get("start_month").strip(), dbutils.widgets.get("end_month").strip())
ctx = new_context("02_raw_to_bronze_trips", load_type, dbutils.widgets.get("batch_id"))
print(f"batch_id={ctx['batch_id']} | load_type={load_type} | months={months} | landing={landing}")

# COMMAND ----------

def ingest_file(path, month):
    df, drift = read_raw_csv(path, TRIP_COLUMNS, TRIP_RAW_SCHEMA, evolve_schema)
    df = add_bronze_metadata(df, path, ctx["batch_id"], load_type, month)
    write_bronze_trips_file(df, cfg["bronze_trips"], path, evolve=drift["evolved"])
    n = spark.table(cfg["bronze_trips"]).where(F.col("source_file") == path).count()
    return {"rows_read": n, "rows_inserted": n, "rows_updated": 0, "message": drift_message(drift)}


def missing_folder(folder):
    raise FileNotFoundError(f"No folder {folder} - upload that month's CSV files first")


results = []
for month in months:
    folder = f"{landing}/trips/{month}"
    files = sorted(f for f in os.listdir(folder) if f.lower().endswith(".csv")) if os.path.isdir(folder) else []
    if not files:
        results.append(run_step(cfg, ctx, "raw_to_bronze", folder, lambda f=folder: missing_folder(f)))
        continue
    for name in files:
        path = f"{folder}/{name}"
        results.append(run_step(cfg, ctx, "raw_to_bronze", path, lambda p=path, m=month: ingest_file(p, m)))

# COMMAND ----------

failed = [r for r in results if r[0] == "FAILURE"]
print(f"Done: {len(results) - len(failed)} succeeded, {len(failed)} failed")
display(spark.table(cfg["logs"]).where(F.col("batch_id") == ctx["batch_id"]).orderBy("start_time"))
if failed and dbutils.widgets.get("fail_on_error") == "true":
    raise Exception(f"{len(failed)} step(s) failed - see pipeline_execution_logs for batch {ctx['batch_id']}")
