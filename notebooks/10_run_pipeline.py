# Databricks notebook source
# MAGIC %md
# MAGIC # 10 — End-to-end pipeline (Raw → Bronze → Silver → Gold)
# MAGIC One entry point that runs every layer in order with **one shared `batch_id`**, so a whole run can be traced
# MAGIC in `pipeline_execution_logs`. Gold is never a separate manual step: whenever new trips or GBFS snapshots reach
# MAGIC Silver, the matching Gold notebook runs straight after with the same parameters.
# MAGIC
# MAGIC ```
# MAGIC trips:        02 raw→bronze ─▶ 03 bronze→silver ─▶ 07 silver→gold (dims, fact_trips, aggregates) ─┐
# MAGIC station info: 06 raw→bronze→silver ───────────────────────────────────────────────────────────┤
# MAGIC live GBFS:    04 raw→bronze ─▶ 05 bronze→silver ─▶ 08 silver→gold (fact_station_availability) ──┴▶ 09 optimize
# MAGIC ```
# MAGIC Each child notebook is called with `fail_on_error=true`: if a layer fails, the layers that depend on it do not run,
# MAGIC so Gold is never refreshed from a half-loaded Silver. Every notebook is idempotent, so the fix is simply to re-run.
# MAGIC
# MAGIC Schedule this notebook as a Databricks Job (or use `jobs/citibike_pipeline_job.json` for a multi-task Job).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.dropdown("load_type", "incremental", ["full", "incremental"], "Load type")
dbutils.widgets.text("start_month", "", "Trips: start month YYYYMM (blank = skip trips)")
dbutils.widgets.text("end_month", "", "Trips: end month YYYYMM (blank = same as start)")
dbutils.widgets.dropdown("run_station_info", "true", ["true", "false"], "Load station_information files")
dbutils.widgets.dropdown("run_gbfs", "true", ["true", "false"], "Load live GBFS snapshots")
dbutils.widgets.text("gbfs_start_date", "", "GBFS: start date YYYY-MM-DD (blank = all)")
dbutils.widgets.text("gbfs_end_date", "", "GBFS: end date YYYY-MM-DD (blank = all)")
dbutils.widgets.dropdown("run_optimize", "true", ["true", "false"], "OPTIMIZE Gold at the end")

# COMMAND ----------

import time
from datetime import datetime, timezone

w = {k: dbutils.widgets.get(k).strip() for k in
     ["catalog", "load_type", "start_month", "end_month", "run_station_info", "run_gbfs",
      "gbfs_start_date", "gbfs_end_date", "run_optimize"]}
batch_id = datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%SZ")
base = {"catalog": w["catalog"], "load_type": w["load_type"], "batch_id": batch_id, "fail_on_error": "true"}
months = {"start_month": w["start_month"], "end_month": w["end_month"] or w["start_month"]}
gbfs_dates = {"start_date": w["gbfs_start_date"], "end_date": w["gbfs_end_date"]}

plan = []
if w["start_month"]:
    plan += [("02_raw_to_bronze_trips", months), ("03_bronze_to_silver_trips", months)]
if w["run_station_info"] == "true":
    plan += [("06_station_info_to_silver", {})]
if w["start_month"]:
    plan += [("07_silver_to_gold_trips", months)]
if w["run_gbfs"] == "true":
    plan += [("04_gbfs_raw_to_bronze", {}), ("05_gbfs_bronze_to_silver", gbfs_dates), ("08_silver_to_gold_gbfs", gbfs_dates)]
if w["run_optimize"] == "true":
    plan += [("09_optimize_gold", {})]

print(f"batch_id = {batch_id}")
for name, _ in plan:
    print("  ->", name)

# COMMAND ----------

timings = []
for name, extra in plan:
    params = {**base, **extra}
    if name == "09_optimize_gold":
        params = {"catalog": w["catalog"], "batch_id": batch_id}
    t0 = time.time()
    print(f"Running {name} ...")
    dbutils.notebook.run(f"./{name}", 3600, params)   # raises if the child fails -> downstream layers are skipped
    timings.append((name, round(time.time() - t0, 1)))
    print(f"  done in {timings[-1][1]} s")

# COMMAND ----------

display(spark.createDataFrame(timings, "notebook string, wall_clock_seconds double"))
display(spark.table(f"{w['catalog']}.citibike_ops.pipeline_execution_logs")
        .where(f"batch_id = '{batch_id}'").orderBy("start_time"))
