# Databricks notebook source
# MAGIC %md
# MAGIC # 90 — Proof notebook (run this once, keep the saved outputs)
# MAGIC Produces, in one run, every feature the Phase 2 rubric asks to *see*:
# MAGIC
# MAGIC 1. **Explicit schemas** (no `inferSchema`) and the strict Silver types
# MAGIC 2. **Backfill** of a month range through Raw → Bronze → Silver, by parameter only
# MAGIC 3. **Idempotency:** re-running Silver creates no duplicates and changes nothing (before/after fingerprint)
# MAGIC 4. **Schema drift:** extra column (captured *and* evolved with `mergeSchema`), type change, malformed row, missing column, reordered columns — none crashes the batch
# MAGIC 5. **The audit log** for all of the above
# MAGIC
# MAGIC The pipeline notebooks are started with `dbutils.notebook.run`, passing parameters — nothing is edited between runs.
# MAGIC Prerequisite: sample CSVs uploaded to `landing/trips/<YYYYMM>/` for every month in the backfill range.

# COMMAND ----------
# MAGIC %run ./01_common

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("backfill_start", "202603", "Backfill start month (YYYYMM)")
dbutils.widgets.text("backfill_end", "202609", "Backfill end month (YYYYMM)")
dbutils.widgets.dropdown("run_backfill", "yes", ["yes", "no"], "1. Run backfill")
dbutils.widgets.dropdown("run_idempotency", "yes", ["yes", "no"], "2. Run idempotency re-run")
dbutils.widgets.dropdown("run_drift", "yes", ["yes", "no"], "3. Run schema-drift cases")
dbutils.widgets.dropdown("cleanup_demo_rows", "no", ["no", "yes"], "4. Remove the drift demo rows afterwards")

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import Row

cfg = get_config(dbutils.widgets.get("catalog"))
stamp = datetime.now(timezone.utc).strftime("%H%M%S")
bf_start, bf_end = dbutils.widgets.get("backfill_start").strip(), dbutils.widgets.get("backfill_end").strip()


def run_nb(name, **args):
    """Start a pipeline notebook with parameters (this is exactly what a scheduled Job would do)."""
    return dbutils.notebook.run(f"./{name}", 3600, {k: str(v) for k, v in args.items()})


def show_log(batch_ids, title):
    print(f"\n=== {title}")
    display(
        spark.table(cfg["logs"]).where(F.col("batch_id").isin(batch_ids))
        .select("batch_id", "pipeline_layer", "load_type", "parameter", "start_time", "end_time", "status",
                "rows_read", "rows_inserted", "rows_updated", "rows_quarantined", "message")
        .orderBy("start_time")
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Explicit schemas — defined with `StructType` / `StructField` before any file is read

# COMMAND ----------

print("Raw -> Bronze contract: trips (every source field is STRING so a read can never fail)")
for f in TRIP_RAW_SCHEMA.fields:
    print(f"  {f.name:<22} {f.dataType.simpleString():<8} nullable={f.nullable}")

print("\nRaw -> Bronze contract: GBFS station status")
for f in GBFS_RAW_SCHEMA.fields:
    print(f"  {f.name:<22} {f.dataType.simpleString():<8} nullable={f.nullable}")

print("\nSilver is strictly typed (timestamps, doubles, ints, dates):")
display(spark.sql(f"DESCRIBE {cfg['silver_trips']}"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Backfill — a month range, by parameter only

# COMMAND ----------

if dbutils.widgets.get("run_backfill") == "yes":
    b1 = f"demo_backfill_{stamp}"
    common = dict(catalog=cfg["catalog"], load_type="full", start_month=bf_start, end_month=bf_end, batch_id=b1)
    run_nb("02_raw_to_bronze_trips", **common)
    run_nb("03_bronze_to_silver_trips", **common)
    show_log([b1], f"BACKFILL {bf_start} -> {bf_end}: Raw->Bronze then Bronze->Silver")
else:
    print("skipped")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Idempotency — re-run the same input, Silver must be identical afterwards

# COMMAND ----------

def silver_fingerprint():
    return (
        spark.table(cfg["silver_trips"])
        .agg(F.count("*").alias("rows"),
             F.countDistinct("ride_id").alias("distinct_ride_ids"),
             F.max("load_timestamp").alias("latest_load_timestamp"),     # unchanged only if NO row was touched
             F.sum(F.hash("row_hash")).alias("content_checksum"))
        .collect()[0].asDict()
    )


if dbutils.widgets.get("run_idempotency") == "yes":
    before = silver_fingerprint()
    b2 = f"demo_rerun_{stamp}"
    common = dict(catalog=cfg["catalog"], load_type="full", start_month=bf_start, end_month=bf_end, batch_id=b2)
    run_nb("02_raw_to_bronze_trips", **{**common, "end_month": bf_start})   # re-load one month into Bronze (replaced, not duplicated)
    run_nb("03_bronze_to_silver_trips", **common)                           # re-run Silver for the whole range
    after = silver_fingerprint()

    print("\n=== Silver fingerprint BEFORE vs AFTER the re-run")
    display(spark.createDataFrame([Row(run="before", **before), Row(run="after", **after)]))
    print("IDEMPOTENT: Silver is identical after the re-run" if before == after else "NOT IDENTICAL - investigate")
    show_log([b2], "RE-RUN of the same input: expect rows_inserted = 0 and rows_updated = 0 on every Silver row")

    print("\n=== Duplicate check (expect no rows)")
    display(spark.sql(f"SELECT ride_id, COUNT(*) AS copies FROM {cfg['silver_trips']} GROUP BY ride_id HAVING COUNT(*) > 1"))
    print("=== Bronze rows per source file (a re-loaded file is replaced, never doubled)")
    display(spark.sql(f"SELECT source_month, source_file, COUNT(*) AS rows FROM {cfg['bronze_trips']} WHERE source_month < '2099' GROUP BY 1, 2 ORDER BY 1"))
else:
    print("skipped")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Schema drift — five cases, none crashes the batch
# MAGIC Test files are written straight into the landing volume (months `209901`–`209905`, clearly separate from real data).

# COMMAND ----------

if dbutils.widgets.get("run_drift") == "yes":
    landing = cfg["landing"]
    HEADER = ["ride_id", "rideable_type", "started_at", "ended_at", "start_station_name", "start_station_id",
              "end_station_name", "end_station_id", "start_lat", "start_lng", "end_lat", "end_lng", "member_casual"]

    def trip(ride_id, started="2026-03-15 12:13:34.479", slat="40.738046", member="member"):
        return [ride_id, "classic_bike", started, "2026-03-15 12:17:05.525", "W 15 St & 6 Ave", "5989.02",
                "E 10 St & 5 Ave", "5872.08", slat, "-73.996430", "40.733320", "-73.995101", member]

    def put(month, name, header, rows):
        text = "\n".join(",".join(r) for r in [header] + rows) + "\n"
        dbutils.fs.put(f"{landing}/trips/{month}/{name}", text, True)

    # 1 & 2: a NEW column appears  (first captured as JSON, then evolved into the table with mergeSchema)
    put("209901", "drift_extra_column.csv", HEADER + ["new_flag"], [trip("DRIFT_EXTRA_1") + ["Y"], trip("DRIFT_EXTRA_2") + ["N"]])
    put("209902", "drift_extra_column_evolve.csv", HEADER + ["new_flag"], [trip("DRIFT_EVOLVE_1") + ["Y"], trip("DRIFT_EVOLVE_2") + ["N"]])
    # 3: a DATA TYPE changes (date in a new format, text in a numeric column) + a malformed row
    put("209903", "drift_type_change.csv", HEADER, [
        trip("DRIFT_TYPE_1"),
        trip("DRIFT_TYPE_2", started="15/03/2026 12:13"),
        trip("DRIFT_TYPE_3", slat="N/A"),
        ["DRIFT_TYPE_4", "classic_bike"],
    ])
    # 4: a column DISAPPEARS
    put("209904", "drift_missing_column.csv", HEADER[:-1], [trip("DRIFT_MISSING_1")[:-1], trip("DRIFT_MISSING_2")[:-1]])
    # 5: columns REORDERED
    put("209905", "drift_reordered.csv", [HEADER[-1]] + HEADER[:-1], [[r[-1]] + r[:-1] for r in [trip("DRIFT_ORDER_1"), trip("DRIFT_ORDER_2")]])

    b3 = f"demo_drift_{stamp}"
    cases = [("209901", "false"), ("209902", "true"), ("209903", "false"), ("209904", "false"), ("209905", "false")]
    for month, evolve in cases:
        run_nb("02_raw_to_bronze_trips", catalog=cfg["catalog"], load_type="incremental", start_month=month,
               end_month=month, evolve_schema=evolve, batch_id=b3)
    run_nb("03_bronze_to_silver_trips", catalog=cfg["catalog"], load_type="incremental", start_month="209901",
           end_month="209905", batch_id=b3)
    show_log([b3], "SCHEMA DRIFT: every step finishes; the message column reports what was detected")
else:
    print("skipped")

# COMMAND ----------

if dbutils.widgets.get("run_drift") == "yes":
    print("=== What happened to the data")
    print("Extra column, captured as JSON (209901) / evolved into a real column with mergeSchema (209902):")
    display(spark.sql(f"SELECT source_month, ride_id, _extra_columns, new_flag FROM {cfg['bronze_trips']} WHERE source_month IN ('209901','209902') ORDER BY 1, 2"))
    print("Bronze now has the evolved column:")
    display(spark.sql(f"DESCRIBE {cfg['bronze_trips']}").where(F.col("col_name") == "new_flag"))
    print("Bad records are quarantined with the reason (type change, malformed row, missing column):")
    display(spark.sql(f"SELECT record_key, quarantine_reason, source_file FROM {cfg['quarantine']} WHERE source_file LIKE '%/trips/2099%' ORDER BY source_file, record_key"))
    print("Good records from the same batches still reached Silver (including the reordered-columns file):")
    display(spark.sql(f"SELECT source_month, ride_id, member_casual, started_at, start_lat FROM {cfg['silver_trips']} WHERE source_month >= '209901' ORDER BY 1, 2"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Optional cleanup of the drift demo rows
# MAGIC Set widget 4 to `yes` and run this cell to remove the `2099xx` test rows. Re-run `03` for any real month afterwards to resync the stations table.

# COMMAND ----------

if dbutils.widgets.get("cleanup_demo_rows") == "yes":
    spark.sql(f"DELETE FROM {cfg['silver_trips']} WHERE source_month >= '209900'")
    spark.sql(f"DELETE FROM {cfg['bronze_trips']} WHERE source_month >= '209900'")
    spark.sql(f"DELETE FROM {cfg['quarantine']} WHERE source_file LIKE '%/trips/2099%'")
    print("Demo rows removed.")
else:
    print("Cleanup not requested (widget 4 = no).")

# COMMAND ----------

print("=== Full audit log, newest first")
display(spark.table(cfg["logs"]).orderBy(F.col("start_time").desc()).limit(60))
