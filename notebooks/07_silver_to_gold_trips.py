# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Silver → Gold (trip star schema + aggregates)
# MAGIC Runs automatically after `03_bronze_to_silver_trips` (see `10_run_pipeline` / the Databricks Job) with the **same month range**.
# MAGIC
# MAGIC | Step | Table(s) | Write strategy | Why |
# MAGIC |---|---|---|---|
# MAGIC | 1 | `dim_station`, `dim_date`, `dim_time`, `dim_rider_type` | `MERGE INTO` on the surrogate key, update only if `row_hash` changed | Slowly changing reference data; re-runs touch nothing |
# MAGIC | 2 | `fact_trips` | `MERGE INTO` on `ride_id`, one month at a time | Incremental and idempotent, like Silver |
# MAGIC | 3 | `gold_daily_ridership_summary`, `gold_station_popularity` | Recompute only the affected date / month range, then `replaceWhere` | Aggregates cannot be upserted row by row without double counting |
# MAGIC | 3 | `gold_hourly_demand_pattern` | Full overwrite | 168 rows; cheaper to rebuild than to patch |
# MAGIC
# MAGIC Every step writes an audit row (layer `silver_to_gold`) with rows aggregated (`rows_read`), rows written and duration.

# COMMAND ----------
# MAGIC %run ./01_common

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.dropdown("load_type", "incremental", ["full", "incremental"], "Load type (label for audit)")
dbutils.widgets.text("start_month", "202603", "Start month (YYYYMM)")
dbutils.widgets.text("end_month", "202603", "End month (YYYYMM, inclusive)")
dbutils.widgets.text("batch_id", "", "Batch ID (blank = auto)")
dbutils.widgets.dropdown("fail_on_error", "false", ["false", "true"], "Raise an error at the END if any step failed")

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "UTC")   # GBFS poll times are UTC; converted to New York time explicitly
cfg = get_config(dbutils.widgets.get("catalog"))
load_type = dbutils.widgets.get("load_type")
months = month_range(dbutils.widgets.get("start_month").strip(), dbutils.widgets.get("end_month").strip())
ctx = new_context("07_silver_to_gold_trips", load_type, dbutils.widgets.get("batch_id"))
print(f"batch_id={ctx['batch_id']} | load_type={load_type} | months={months}")

# COMMAND ----------
# MAGIC %md ### Step 1 — Conformed dimensions

# COMMAND ----------

results = refresh_dimensions(cfg, ctx)

# COMMAND ----------
# MAGIC %md ### Step 2 — `fact_trips` (one MERGE per month)

# COMMAND ----------

def load_fact_month(month):
    silver = spark.table(cfg["silver_trips"]).where(F.col("source_month") == month)
    rows_read = silver.count()
    if rows_read == 0:
        raise ValueError(f"No Silver trips for {month} - run 03_bronze_to_silver_trips for that month first")
    fact = build_fact_trips(silver, ctx["batch_id"])
    res = merge_into(cfg["fact_trips"], fact, ["ride_id"])
    return {"rows_read": rows_read, **res}


loaded = []
for month in months:
    status, _ = run_step(cfg, ctx, "silver_to_gold", f"fact_trips month={month}", lambda m=month: load_fact_month(m))
    results.append((status, _))
    if status == "SUCCESS":
        loaded.append(month)

# COMMAND ----------
# MAGIC %md ### Step 3 — Aggregate tables for the affected range

# COMMAND ----------

if loaded:
    fact_all = spark.table(cfg["fact_trips"])
    dim_rider = spark.table(cfg["dim_rider_type"])
    dim_date = spark.table(cfg["dim_date"])
    # a monthly file can contain trips that started on the last evening of the previous month,
    # so the affected range comes from the trips themselves, not from the month labels
    lo, hi = fact_all.where(F.col("source_month").isin(loaded)).agg(F.min("date_key"), F.max("date_key")).collect()[0]
    ym_lo, ym_hi = lo // 100, hi // 100
    print(f"Affected dates {lo} to {hi} (months {ym_lo} to {ym_hi})")

    def daily():
        src = fact_all.where(F.col("date_key").between(lo, hi))
        out = build_gold_daily(src, dim_rider, ctx["batch_id"])
        return {"rows_read": src.count(), **replace_range(cfg["gold_daily"], out, f"date_key BETWEEN {lo} AND {hi}"),
                "message": f"aggregated trips into days {lo}-{hi}"}

    def station_popularity():
        src = fact_all.where(F.col("date_key").between(ym_lo * 100 + 1, ym_hi * 100 + 31))
        out = build_gold_station_popularity(src, dim_rider, ctx["batch_id"])
        return {"rows_read": src.count(),
                **replace_range(cfg["gold_station_popularity"], out, f"year_month_key BETWEEN {ym_lo} AND {ym_hi}"),
                "message": f"aggregated trips into station-months {ym_lo}-{ym_hi}"}

    def hourly():
        out = build_gold_hourly(fact_all, dim_date, dim_rider, ctx["batch_id"])
        return {"rows_read": fact_all.count(), **overwrite_table(cfg["gold_hourly"], out),
                "message": "full rebuild (day of week x hour)"}

    results.append(run_step(cfg, ctx, "silver_to_gold", f"gold_daily_ridership_summary {lo}-{hi}", daily))
    results.append(run_step(cfg, ctx, "silver_to_gold", f"gold_station_popularity {ym_lo}-{ym_hi}", station_popularity))
    results.append(run_step(cfg, ctx, "silver_to_gold", "gold_hourly_demand_pattern (all)", hourly))
else:
    print("No fact month loaded successfully - aggregates left unchanged")

# COMMAND ----------

failed = [r for r in results if r[0] == "FAILURE"]
print(f"Done: {len(results) - len(failed)} succeeded, {len(failed)} failed")
display(spark.table(cfg["logs"]).where(F.col("batch_id") == ctx["batch_id"]).orderBy("start_time"))
if failed and dbutils.widgets.get("fail_on_error") == "true":
    raise Exception(f"{len(failed)} step(s) failed - see pipeline_execution_logs for batch {ctx['batch_id']}")
