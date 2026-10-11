# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Raw → Bronze (live GBFS station status)
# MAGIC The hourly collector appends to one growing CSV, so the same snapshots are seen again on every upload.
# MAGIC Bronze therefore uses an **insert-only `MERGE INTO`** keyed on `(station_id, poll_timestamp_utc)`:
# MAGIC already-loaded snapshots are skipped, only genuinely new ones are inserted.

# COMMAND ----------
# MAGIC %run ./01_common

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.dropdown("load_type", "incremental", ["full", "incremental"], "Load type (label for audit)")
dbutils.widgets.text("source_path", "", "Folder override (blank = <landing>/gbfs)")
dbutils.widgets.text("file_name", "", "Single file name (blank = every .csv in the folder)")
dbutils.widgets.text("batch_id", "", "Batch ID (blank = auto)")
dbutils.widgets.dropdown("fail_on_error", "false", ["false", "true"], "Raise an error at the END if any step failed")

# COMMAND ----------

import os

cfg = get_config(dbutils.widgets.get("catalog"))
load_type = dbutils.widgets.get("load_type")
folder = dbutils.widgets.get("source_path").strip() or f"{cfg['landing']}/gbfs"
single = dbutils.widgets.get("file_name").strip()
ctx = new_context("04_gbfs_raw_to_bronze", load_type, dbutils.widgets.get("batch_id"))
BRONZE_KEY = ["station_id", "poll_timestamp_utc", CORRUPT_COL]


def ingest_file(path):
    df, drift = read_raw_csv(path, GBFS_COLUMNS, GBFS_RAW_SCHEMA, evolve_schema=False)
    df = add_bronze_metadata(df, path, ctx["batch_id"], load_type).dropDuplicates(BRONZE_KEY)
    df = df.select(*BRONZE_GBFS_COLUMNS)
    rows_read = df.count()
    res = merge_into(cfg["bronze_gbfs"], df, BRONZE_KEY, update_when_changed=False)
    return {"rows_read": rows_read, **res, "message": drift_message(drift)}


def missing_folder(p):
    raise FileNotFoundError(f"Nothing to load at {p} - upload the GBFS CSV first")


files = [single] if single else (sorted(f for f in os.listdir(folder) if f.lower().endswith(".csv")) if os.path.isdir(folder) else [])
results = []
if not files:
    results.append(run_step(cfg, ctx, "raw_to_bronze_gbfs", folder, lambda: missing_folder(folder)))
for name in files:
    path = f"{folder}/{name}"
    results.append(run_step(cfg, ctx, "raw_to_bronze_gbfs", path, lambda p=path: ingest_file(p)))

# COMMAND ----------

failed = [r for r in results if r[0] == "FAILURE"]
print(f"Done: {len(results) - len(failed)} succeeded, {len(failed)} failed")
display(spark.table(cfg["logs"]).where(F.col("batch_id") == ctx["batch_id"]).orderBy("start_time"))
if failed and dbutils.widgets.get("fail_on_error") == "true":
    raise Exception(f"{len(failed)} step(s) failed - see pipeline_execution_logs for batch {ctx['batch_id']}")
