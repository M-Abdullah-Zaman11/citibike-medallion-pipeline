# Databricks notebook source
# MAGIC %md
# MAGIC # 09 — Gold read optimization (liquid clustering + OPTIMIZE)
# MAGIC The Gold tables are created with **liquid clustering** (`CLUSTER BY` in `00_setup`):
# MAGIC
# MAGIC | Table | Clustered on | Why |
# MAGIC |---|---|---|
# MAGIC | `fact_trips` | `date_key, start_station_key` | Almost every dashboard query filters by date range, and station pages filter by station |
# MAGIC | `fact_station_availability` | `date_key, station_key` | Same access pattern for the live-availability page |
# MAGIC | `gold_daily_ridership_summary` | `date_key` | Trend charts read date ranges |
# MAGIC | `gold_station_popularity` | `year_month_key, station_key` | Station rankings are per month |
# MAGIC
# MAGIC Every `MERGE` adds small files. `OPTIMIZE` compacts them and physically co-locates rows by the clustering keys,
# MAGIC so Delta's per-file min/max statistics let Spark **skip files** that cannot match a filter (data skipping).
# MAGIC Run it after the Gold notebooks (it is the last task of the Job). Each table's run is audited like any other step.

# COMMAND ----------
# MAGIC %run ./01_common

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("batch_id", "", "Batch ID (blank = auto)")
cfg = get_config(dbutils.widgets.get("catalog"))
ctx = new_context("09_optimize_gold", "maintenance", dbutils.widgets.get("batch_id"))

TABLES = ["fact_trips", "fact_availability", "gold_daily", "gold_station_popularity",
          "dim_station", "dim_date", "dim_rider_type", "gold_hourly", "dim_time"]

# COMMAND ----------

def detail(table):
    return spark.sql(f"DESCRIBE DETAIL {table}").select("numFiles", "sizeInBytes", "clusteringColumns").collect()[0]


def optimize(table):
    before = detail(table)
    spark.sql(f"OPTIMIZE {table}")
    after = detail(table)
    return {"rows_read": spark.table(table).count(), "rows_inserted": 0, "rows_updated": 0,
            "message": f"files {before['numFiles']} -> {after['numFiles']}, "
                       f"size {before['sizeInBytes'] / 1e6:.1f} MB -> {after['sizeInBytes'] / 1e6:.1f} MB, "
                       f"clustering={list(after['clusteringColumns'] or [])}"}


results = [run_step(cfg, ctx, "gold_maintenance", cfg[t], lambda x=cfg[t]: optimize(x)) for t in TABLES]

# COMMAND ----------

display(spark.table(cfg["logs"]).where(F.col("batch_id") == ctx["batch_id"]).orderBy("start_time")
        .select("parameter", "status", "duration_seconds", "rows_read", "message"))

# COMMAND ----------
# MAGIC %md
# MAGIC ### Evidence for the report: data skipping
# MAGIC Run the query below, open **View → Spark UI / Query profile**, and compare *files read* with *files total*
# MAGIC for the `fact_trips` scan. After `OPTIMIZE`, a one-week filter should read only a small fraction of the files.

# COMMAND ----------

wk = spark.table(cfg["fact_trips"]).agg(F.max("date_key")).collect()[0][0]
if wk:
    display(spark.sql(f"""
        SELECT date_key, COUNT(*) AS trips, ROUND(AVG(trip_duration_minutes), 1) AS avg_minutes
        FROM {cfg['fact_trips']}
        WHERE date_key BETWEEN {wk - 6} AND {wk}
        GROUP BY date_key ORDER BY date_key"""))
