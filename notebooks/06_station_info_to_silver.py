# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — GBFS station_information: Raw → Bronze → Silver
# MAGIC The live `station_status` feed identifies stations by a UUID / 19-digit id, while the trip files use short ids
# MAGIC such as `5989.02`. The `station_information` feed lists **both** (`station_id` and `short_name`), plus dock
# MAGIC `capacity`, so it is the bridge that lets both facts share one `dim_station` in Gold.
# MAGIC
# MAGIC * **Raw → Bronze:** explicit schema, drift handling, each file's rows replaced atomically (`replaceWhere` on `source_file`).
# MAGIC * **Bronze → Silver:** typed and validated; latest snapshot per station; `MERGE INTO` keyed on `gbfs_station_id`,
# MAGIC   updating only when the station's details actually changed (`row_hash`). Invalid rows go to `quarantine_records`.

# COMMAND ----------
# MAGIC %run ./01_common

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.dropdown("load_type", "incremental", ["full", "incremental"], "Load type (label for audit)")
dbutils.widgets.text("source_path", "", "Folder override (blank = <landing>/station_info)")
dbutils.widgets.text("file_name", "", "Single file name (blank = every .csv in the folder)")
dbutils.widgets.text("batch_id", "", "Batch ID (blank = auto)")
dbutils.widgets.dropdown("fail_on_error", "false", ["false", "true"], "Raise an error at the END if any step failed")

# COMMAND ----------

import os

cfg = get_config(dbutils.widgets.get("catalog"))
load_type = dbutils.widgets.get("load_type")
folder = dbutils.widgets.get("source_path").strip() or f"{cfg['landing']}/station_info"
single = dbutils.widgets.get("file_name").strip()
ctx = new_context("06_station_info_to_silver", load_type, dbutils.widgets.get("batch_id"))
print(f"batch_id={ctx['batch_id']} | folder={folder}")

# COMMAND ----------

def ingest_file(path):
    df, drift = read_raw_csv(path, STATION_INFO_COLUMNS, STATION_INFO_RAW_SCHEMA, evolve_schema=False)
    df = add_bronze_metadata(df, path, ctx["batch_id"], load_type).select(*BRONZE_STATION_INFO_COLUMNS)
    write_bronze_file(df, cfg["bronze_station_info"], path, evolve=False)
    n = spark.table(cfg["bronze_station_info"]).where(F.col("source_file") == path).count()
    return {"rows_read": n, "rows_inserted": n, "rows_updated": 0, "message": drift_message(drift)}


def to_silver():
    # whole Bronze table every time: it is small (~2,000 stations per snapshot) and the result then
    # depends only on the data, never on which file triggered the run -> idempotent
    bronze = spark.table(cfg["bronze_station_info"])
    rows_read = bronze.count()
    if rows_read == 0:
        raise ValueError("bronze_gbfs_station_info is empty - load a station_information file first")
    valid, quarantine = transform_station_info_silver(bronze, ctx["batch_id"])
    n_quarantined = quarantine.count()
    res = merge_into(cfg["silver_station_info"], valid, ["gbfs_station_id"])
    merge_into(cfg["quarantine"], quarantine, ["entity", "record_hash"], update_when_changed=False)
    return {"rows_read": rows_read, **res, "rows_quarantined": n_quarantined}


def missing_folder(p):
    raise FileNotFoundError(f"Nothing to load at {p} - run scripts/fetch_station_info.py and upload the CSV first")


files = [single] if single else (sorted(f for f in os.listdir(folder) if f.lower().endswith(".csv")) if os.path.isdir(folder) else [])
results = []
if not files:
    results.append(run_step(cfg, ctx, "raw_to_bronze_station_info", folder, lambda: missing_folder(folder)))
for name in files:
    path = f"{folder}/{name}"
    results.append(run_step(cfg, ctx, "raw_to_bronze_station_info", path, lambda p=path: ingest_file(p)))

if any(r[0] == "SUCCESS" for r in results):
    results.append(run_step(cfg, ctx, "bronze_to_silver_station_info", "station_info (all snapshots)", to_silver))

# COMMAND ----------

failed = [r for r in results if r[0] == "FAILURE"]
print(f"Done: {len(results) - len(failed)} succeeded, {len(failed)} failed")
display(spark.table(cfg["logs"]).where(F.col("batch_id") == ctx["batch_id"]).orderBy("start_time"))
if failed and dbutils.widgets.get("fail_on_error") == "true":
    raise Exception(f"{len(failed)} step(s) failed - see pipeline_execution_logs for batch {ctx['batch_id']}")
