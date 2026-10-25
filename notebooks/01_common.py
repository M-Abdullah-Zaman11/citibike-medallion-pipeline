# Databricks notebook source
# MAGIC %md
# MAGIC # 01_common — shared contracts, helpers and audit logging
# MAGIC Not run on its own. Every pipeline notebook starts with `%run ./01_common`.
# MAGIC
# MAGIC Contents: table names, explicit schemas (the "data contract"), schema-drift-aware CSV reading,
# MAGIC Silver cleaning rules, an idempotent `MERGE INTO` helper, and the audit-log writer.

# COMMAND ----------

import re
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType

CORRUPT_COL = "_corrupt_record"   # raw text of any row that could not be parsed
EXTRA_COL = "_extra_columns"      # JSON of unexpected columns (schema drift) when not evolving the schema

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Configuration — every table and path name lives here, nowhere else

# COMMAND ----------

def get_config(catalog: str = "workspace") -> dict:
    return {
        "catalog": catalog,
        "landing": f"/Volumes/{catalog}/citibike_raw/landing",
        "bronze_trips": f"{catalog}.citibike_bronze.bronze_trips",
        "bronze_gbfs": f"{catalog}.citibike_bronze.bronze_gbfs_station_status",
        "silver_trips": f"{catalog}.citibike_silver.silver_trips",
        "silver_stations": f"{catalog}.citibike_silver.silver_stations",
        "silver_gbfs": f"{catalog}.citibike_silver.silver_gbfs_station_status",
        "quarantine": f"{catalog}.citibike_silver.quarantine_records",
        "logs": f"{catalog}.citibike_ops.pipeline_execution_logs",
        # ---- Phase 3: GBFS station reference feed ----
        "bronze_station_info": f"{catalog}.citibike_bronze.bronze_gbfs_station_info",
        "silver_station_info": f"{catalog}.citibike_silver.silver_gbfs_station_info",
        # ---- Phase 3: Gold star schema ----
        "dim_station": f"{catalog}.citibike_gold.dim_station",
        "dim_date": f"{catalog}.citibike_gold.dim_date",
        "dim_time": f"{catalog}.citibike_gold.dim_time",
        "dim_rider_type": f"{catalog}.citibike_gold.dim_rider_type",
        "fact_trips": f"{catalog}.citibike_gold.fact_trips",
        "fact_availability": f"{catalog}.citibike_gold.fact_station_availability",
        "gold_daily": f"{catalog}.citibike_gold.gold_daily_ridership_summary",
        "gold_station_popularity": f"{catalog}.citibike_gold.gold_station_popularity",
        "gold_hourly": f"{catalog}.citibike_gold.gold_hourly_demand_pattern",
    }

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Data contracts — explicit schemas (no `inferSchema` anywhere)
# MAGIC Bronze stores every source field as STRING so nothing can fail on read; strict typing happens on the way into Silver.

# COMMAND ----------

TRIP_COLUMNS = [
    "ride_id", "rideable_type", "started_at", "ended_at",
    "start_station_name", "start_station_id", "end_station_name", "end_station_id",
    "start_lat", "start_lng", "end_lat", "end_lng", "member_casual",
]
GBFS_COLUMNS = [
    "poll_timestamp_utc", "station_id", "num_bikes_available", "num_ebikes_available",
    "num_docks_available", "is_installed", "is_renting", "is_returning", "last_reported",
]


def raw_schema(columns):
    """Explicit all-STRING StructType plus a slot for unparseable rows."""
    return StructType(
        [StructField(c, StringType(), True) for c in columns]
        + [StructField(CORRUPT_COL, StringType(), True)]
    )


TRIP_RAW_SCHEMA = raw_schema(TRIP_COLUMNS)
GBFS_RAW_SCHEMA = raw_schema(GBFS_COLUMNS)

BRONZE_TRIP_COLUMNS = TRIP_COLUMNS + [CORRUPT_COL, EXTRA_COL, "source_file", "source_month", "batch_id", "load_type", "load_timestamp"]
BRONZE_GBFS_COLUMNS = GBFS_COLUMNS + [CORRUPT_COL, EXTRA_COL, "source_file", "batch_id", "load_type", "load_timestamp"]

# Phase 3: GBFS station_information snapshot (written by scripts/fetch_station_info.py)
STATION_INFO_COLUMNS = [
    "snapshot_timestamp_utc", "station_id", "short_name", "name", "lat", "lon", "capacity", "region_id",
]
STATION_INFO_RAW_SCHEMA = raw_schema(STATION_INFO_COLUMNS)
BRONZE_STATION_INFO_COLUMNS = STATION_INFO_COLUMNS + [CORRUPT_COL, EXTRA_COL, "source_file", "batch_id", "load_type", "load_timestamp"]

LOG_SCHEMA = StructType([
    StructField("run_id", StringType()), StructField("batch_id", StringType()),
    StructField("pipeline_layer", StringType()), StructField("notebook", StringType()),
    StructField("load_type", StringType()), StructField("parameter", StringType()),
    StructField("start_time", TimestampType()), StructField("end_time", TimestampType()),
    StructField("duration_seconds", DoubleType()), StructField("status", StringType()),
    StructField("rows_read", LongType()), StructField("rows_inserted", LongType()),
    StructField("rows_updated", LongType()), StructField("rows_quarantined", LongType()),
    StructField("message", StringType()), StructField("load_timestamp", TimestampType()),
])

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Reading raw CSVs with drift detection

# COMMAND ----------

def _spark():
    return SparkSession.builder.getOrCreate()


def read_header(path: str):
    """Header row of a CSV file as a list of column names."""
    first = _spark().read.text(path).limit(1).collect()
    if not first:
        raise ValueError(f"Empty file: {path}")
    return [c.strip().strip('"') for c in first[0][0].lstrip("\ufeff").split(",")]


def read_raw_csv(path, contract_columns, contract_schema, evolve_schema=False):
    """
    Read one raw CSV against the explicit contract.

    * Header matches the contract exactly  -> read with the contract StructType.
    * Header differs (extra / missing / reordered columns) -> build an explicit StructType from the
      file's own header, read by name, then align to the contract:
        - missing columns are filled with NULL,
        - extra columns are kept as real columns (evolve_schema=True, written with mergeSchema)
          or packed into a JSON `_extra_columns` string (evolve_schema=False).
    * Rows that cannot be parsed are kept in `_corrupt_record` (never crash the batch).
    Returns (dataframe, drift_info).
    """
    header = read_header(path)
    missing = [c for c in contract_columns if c not in header]
    extra = [c for c in header if c not in contract_columns]
    conforming = header == contract_columns
    schema = contract_schema if conforming else raw_schema(header)

    df = (
        _spark().read.format("csv")
        .schema(schema)
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COL)
        .load(path)
    )

    for c in missing:
        df = df.withColumn(c, F.lit(None).cast("string"))
    if extra and not evolve_schema:
        df = df.withColumn(EXTRA_COL, F.to_json(F.struct(*[F.col(f"`{c}`") for c in extra])))
        df = df.drop(*extra)
    elif EXTRA_COL not in df.columns:
        df = df.withColumn(EXTRA_COL, F.lit(None).cast("string"))

    kept_extra = extra if evolve_schema else []
    ordered = contract_columns + [CORRUPT_COL, EXTRA_COL] + kept_extra
    drift = {"conforming": conforming, "missing": missing, "extra": extra, "evolved": bool(kept_extra)}
    return df.select(*ordered), drift


def drift_message(drift: dict) -> str:
    if drift["conforming"]:
        return "schema matches contract"
    if not drift["missing"] and not drift["extra"]:
        return "SCHEMA DRIFT column order differs from contract (read by name)"
    return f"SCHEMA DRIFT missing={drift['missing']} extra={drift['extra']} evolved={drift['evolved']}"


def add_bronze_metadata(df, source_file, batch_id, load_type, source_month=None):
    df = (
        df.withColumn("source_file", F.lit(source_file))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("load_type", F.lit(load_type))
        .withColumn("load_timestamp", F.current_timestamp())
    )
    if source_month is not None:
        df = df.withColumn("source_month", F.lit(source_month))
    return df

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Silver transformations (pure functions: Bronze DataFrame in, DataFrames out)

# COMMAND ----------

def _clean(col):
    """Trim; empty string becomes NULL."""
    t = F.trim(F.col(col))
    return F.when(t == "", F.lit(None)).otherwise(t)


def _hash(columns):
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in columns]), 256)


def _reasons(rules):
    """rules = [(reason_name, boolean_condition)] -> ';'-joined reasons (empty string = record is valid)."""
    return F.concat_ws(";", *[F.when(cond, F.lit(name)) for name, cond in rules])


TRIP_SILVER_BUSINESS_COLUMNS = [
    "ride_id", "rideable_type", "started_at", "ended_at",
    "start_station_name", "start_station_id", "end_station_name", "end_station_id",
    "start_lat", "start_lng", "end_lat", "end_lng", "member_casual",
    "trip_duration_seconds", "trip_date", "trip_hour", "day_of_week",
]
TRIP_SILVER_COLUMNS = TRIP_SILVER_BUSINESS_COLUMNS + ["row_hash", "source_file", "source_month", "batch_id", "load_timestamp"]
QUARANTINE_COLUMNS = ["entity", "record_hash", "record_key", "source_file", "batch_id", "quarantine_reason", "raw_record", "load_timestamp"]


def transform_trips_silver(bronze_df, batch_id, valid_member_types=("member", "casual"), max_trip_seconds=86400):
    """Cast, validate and de-duplicate trips. Returns (valid_df, quarantine_df)."""
    raw_cols = TRIP_COLUMNS
    typed = bronze_df.select(
        _clean("ride_id").alias("ride_id"),
        F.lower(_clean("rideable_type")).alias("rideable_type"),
        F.expr("try_cast(started_at as timestamp)").alias("started_at"),
        F.expr("try_cast(ended_at as timestamp)").alias("ended_at"),
        _clean("start_station_name").alias("start_station_name"),
        _clean("start_station_id").alias("start_station_id"),     # identifier -> stays STRING ("5989.02")
        _clean("end_station_name").alias("end_station_name"),
        _clean("end_station_id").alias("end_station_id"),
        F.expr("try_cast(start_lat as double)").alias("start_lat"),
        F.expr("try_cast(start_lng as double)").alias("start_lng"),
        F.expr("try_cast(end_lat as double)").alias("end_lat"),
        F.expr("try_cast(end_lng as double)").alias("end_lng"),
        _clean("start_lat").alias("_raw_start_lat"), _clean("start_lng").alias("_raw_start_lng"),
        _clean("end_lat").alias("_raw_end_lat"), _clean("end_lng").alias("_raw_end_lng"),
        F.lower(_clean("member_casual")).alias("member_casual"),
        F.col(CORRUPT_COL),
        F.to_json(F.struct(*[F.col(c) for c in raw_cols])).alias("raw_record"),
        F.col("source_file"), F.col("source_month"),
    ).withColumn("trip_duration_seconds", F.col("ended_at").cast("long") - F.col("started_at").cast("long"))

    c = F.col
    rules = [
        ("corrupt_record", c(CORRUPT_COL).isNotNull()),
        ("missing_ride_id", c("ride_id").isNull()),
        ("invalid_started_at", c("started_at").isNull()),
        ("invalid_ended_at", c("ended_at").isNull()),
        ("negative_duration", c("trip_duration_seconds") < 0),
        ("duration_over_limit", c("trip_duration_seconds") > max_trip_seconds),
        ("invalid_start_location", c("start_station_id").isNull() & (c("start_lat").isNull() | c("start_lng").isNull())),
        ("invalid_end_location", c("end_station_id").isNull() & (c("end_lat").isNull() | c("end_lng").isNull())),
        # a coordinate that was present but is not a number (e.g. 'N/A') must not silently become NULL
        ("invalid_coordinates",
         (c("_raw_start_lat").isNotNull() & c("start_lat").isNull()) | (c("_raw_start_lng").isNotNull() & c("start_lng").isNull())
         | (c("_raw_end_lat").isNotNull() & c("end_lat").isNull()) | (c("_raw_end_lng").isNotNull() & c("end_lng").isNull())),
        ("invalid_member_type", ~c("member_casual").isin(*valid_member_types) | c("member_casual").isNull()),
    ]
    flagged = typed.withColumn("quarantine_reason", _reasons(rules))

    quarantine = flagged.where(c("quarantine_reason") != "").select(
        F.lit("trips").alias("entity"),
        F.sha2(F.concat_ws("||", c("source_file"), c("raw_record")), 256).alias("record_hash"),
        c("ride_id").alias("record_key"), c("source_file"),
        F.lit(batch_id).alias("batch_id"), c("quarantine_reason"), c("raw_record"),
        F.current_timestamp().alias("load_timestamp"),
    )

    # one row per ride_id: if the same ride appears twice, keep the one from the latest file
    latest = Window.partitionBy("ride_id").orderBy(c("source_file").desc())
    valid = (
        flagged.where(c("quarantine_reason") == "")
        .withColumn("_rn", F.row_number().over(latest)).where(c("_rn") == 1).drop("_rn")
        .withColumn("trip_date", F.to_date("started_at"))
        .withColumn("trip_hour", F.hour("started_at"))
        .withColumn("day_of_week", F.dayofweek("started_at"))      # 1 = Sunday ... 7 = Saturday
        .withColumn("row_hash", _hash(TRIP_SILVER_BUSINESS_COLUMNS))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("load_timestamp", F.current_timestamp())
        .select(*TRIP_SILVER_COLUMNS)
    )
    return valid, quarantine


STATION_COLUMNS = ["station_id", "station_name", "latitude", "longitude", "row_hash", "batch_id", "load_timestamp"]


def derive_stations(valid_trips_df, batch_id):
    """Station reference rows from a batch of valid trips (one row per station_id)."""
    starts = valid_trips_df.select(
        F.col("start_station_id").alias("station_id"), F.col("start_station_name").alias("station_name"),
        F.col("start_lat").alias("latitude"), F.col("start_lng").alias("longitude"))
    ends = valid_trips_df.select(
        F.col("end_station_id").alias("station_id"), F.col("end_station_name").alias("station_name"),
        F.col("end_lat").alias("latitude"), F.col("end_lng").alias("longitude"))
    return (
        starts.unionByName(ends).where(F.col("station_id").isNotNull())
        .groupBy("station_id")
        .agg(F.max("station_name").alias("station_name"),
             F.round(F.avg("latitude"), 6).alias("latitude"),
             F.round(F.avg("longitude"), 6).alias("longitude"))
        .withColumn("row_hash", _hash(["station_id", "station_name", "latitude", "longitude"]))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("load_timestamp", F.current_timestamp())
        .select(*STATION_COLUMNS)
    )


def _to_bool(col):
    s = F.lower(F.trim(F.col(col)))
    return F.when(s.isin("1", "true"), F.lit(True)).when(s.isin("0", "false"), F.lit(False)).otherwise(F.lit(None).cast("boolean"))


GBFS_SILVER_BUSINESS_COLUMNS = [
    "station_id", "poll_timestamp", "num_bikes_available", "num_ebikes_available", "num_docks_available",
    "is_installed", "is_renting", "is_returning", "last_reported_ts",
]
GBFS_SILVER_COLUMNS = GBFS_SILVER_BUSINESS_COLUMNS + ["row_hash", "source_file", "batch_id", "load_timestamp"]


def transform_gbfs_silver(bronze_df, batch_id):
    """Cast and validate GBFS station snapshots. Returns (valid_df, quarantine_df)."""
    typed = bronze_df.select(
        _clean("station_id").alias("station_id"),
        F.expr("try_cast(poll_timestamp_utc as timestamp)").alias("poll_timestamp"),
        F.expr("try_cast(num_bikes_available as int)").alias("num_bikes_available"),
        F.expr("try_cast(num_ebikes_available as int)").alias("num_ebikes_available"),
        F.expr("try_cast(num_docks_available as int)").alias("num_docks_available"),
        _to_bool("is_installed").alias("is_installed"),
        _to_bool("is_renting").alias("is_renting"),
        _to_bool("is_returning").alias("is_returning"),
        F.expr("timestamp_seconds(try_cast(last_reported as bigint))").alias("last_reported_ts"),
        F.col(CORRUPT_COL), F.col("source_file"),
        F.to_json(F.struct(*[F.col(c) for c in GBFS_COLUMNS])).alias("raw_record"),
    )
    c = F.col
    rules = [
        ("corrupt_record", c(CORRUPT_COL).isNotNull()),
        ("missing_station_id", c("station_id").isNull()),
        ("invalid_poll_timestamp", c("poll_timestamp").isNull()),
        ("invalid_bike_count", c("num_bikes_available").isNull() | (c("num_bikes_available") < 0)),
        ("invalid_dock_count", c("num_docks_available").isNull() | (c("num_docks_available") < 0)),
    ]
    flagged = typed.withColumn("quarantine_reason", _reasons(rules))
    quarantine = flagged.where(c("quarantine_reason") != "").select(
        F.lit("gbfs").alias("entity"),
        F.sha2(F.concat_ws("||", c("source_file"), c("raw_record")), 256).alias("record_hash"),
        c("station_id").alias("record_key"), c("source_file"),
        F.lit(batch_id).alias("batch_id"), c("quarantine_reason"), c("raw_record"),
        F.current_timestamp().alias("load_timestamp"),
    )
    latest = Window.partitionBy("station_id", "poll_timestamp").orderBy(c("source_file").desc())
    valid = (
        flagged.where(c("quarantine_reason") == "")
        .withColumn("_rn", F.row_number().over(latest)).where(c("_rn") == 1).drop("_rn")
        .withColumn("row_hash", _hash(GBFS_SILVER_BUSINESS_COLUMNS))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("load_timestamp", F.current_timestamp())
        .select(*GBFS_SILVER_COLUMNS)
    )
    return valid, quarantine


# ---- Phase 3: GBFS station_information (maps live-feed IDs to trip-data station IDs) ----

STATION_INFO_SILVER_BUSINESS_COLUMNS = [
    "gbfs_station_id", "short_name", "station_name", "latitude", "longitude", "capacity", "region_id",
]
STATION_INFO_SILVER_COLUMNS = STATION_INFO_SILVER_BUSINESS_COLUMNS + ["info_as_of", "row_hash", "source_file", "batch_id", "load_timestamp"]


def transform_station_info_silver(bronze_df, batch_id):
    """
    Cast and validate station_information snapshots. Returns (valid_df, quarantine_df).
    * One row per gbfs_station_id: the most recent snapshot wins.
    * row_hash covers only the business columns, so an unchanged station is never rewritten;
      info_as_of therefore records the snapshot in which the station's details last changed.
    """
    typed = bronze_df.select(
        _clean("station_id").alias("gbfs_station_id"),     # UUID or 19-digit id used by the live feed
        _clean("short_name").alias("short_name"),          # "5989.02"-style id used by the trip files
        _clean("name").alias("station_name"),
        F.expr("try_cast(lat as double)").alias("latitude"),
        F.expr("try_cast(lon as double)").alias("longitude"),
        F.expr("try_cast(capacity as int)").alias("capacity"),
        _clean("region_id").alias("region_id"),
        F.expr("try_cast(snapshot_timestamp_utc as timestamp)").alias("info_as_of"),
        _clean("capacity").alias("_raw_capacity"),
        F.col(CORRUPT_COL), F.col("source_file"),
        F.to_json(F.struct(*[F.col(c) for c in STATION_INFO_COLUMNS])).alias("raw_record"),
    )
    c = F.col
    rules = [
        ("corrupt_record", c(CORRUPT_COL).isNotNull()),
        ("missing_station_id", c("gbfs_station_id").isNull()),
        ("invalid_snapshot_timestamp", c("info_as_of").isNull()),
        ("invalid_coordinates", c("latitude").isNull() | c("longitude").isNull()
            | ~c("latitude").between(-90, 90) | ~c("longitude").between(-180, 180)),
        ("invalid_capacity", (c("_raw_capacity").isNotNull() & c("capacity").isNull()) | (c("capacity") < 0)),
    ]
    flagged = typed.withColumn("quarantine_reason", _reasons(rules))
    quarantine = flagged.where(c("quarantine_reason") != "").select(
        F.lit("station_info").alias("entity"),
        F.sha2(F.concat_ws("||", c("source_file"), c("raw_record")), 256).alias("record_hash"),
        c("gbfs_station_id").alias("record_key"), c("source_file"),
        F.lit(batch_id).alias("batch_id"), c("quarantine_reason"), c("raw_record"),
        F.current_timestamp().alias("load_timestamp"),
    )
    latest = Window.partitionBy("gbfs_station_id").orderBy(c("info_as_of").desc(), c("source_file").desc())
    valid = (
        flagged.where(c("quarantine_reason") == "")
        .withColumn("_rn", F.row_number().over(latest)).where(c("_rn") == 1).drop("_rn")
        .withColumn("row_hash", _hash(STATION_INFO_SILVER_BUSINESS_COLUMNS))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("load_timestamp", F.current_timestamp())
        .select(*STATION_INFO_SILVER_COLUMNS)
    )
    return valid, quarantine


# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Idempotent writes — `MERGE INTO`

# COMMAND ----------

def merge_into(table, staging_df, key_columns, update_when_changed=True):
    """
    Upsert staging_df into a Delta table.
    * new key                      -> INSERT
    * existing key, data changed   -> UPDATE (only when row_hash differs)
    * existing key, data unchanged -> untouched, so re-running the same batch changes nothing.
    Returns {"rows_inserted": n, "rows_updated": n}.
    """
    view = "_stg_" + uuid.uuid4().hex[:12]
    staging_df.createOrReplaceTempView(view)
    on = " AND ".join(f"t.{k} <=> s.{k}" for k in key_columns)   # null-safe equality
    matched = "WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET *" if update_when_changed else ""
    sql = f"MERGE INTO {table} AS t USING {view} AS s ON {on} {matched} WHEN NOT MATCHED THEN INSERT *"
    row = _spark().sql(sql).collect()[0].asDict()
    return {"rows_inserted": int(row.get("num_inserted_rows") or 0), "rows_updated": int(row.get("num_updated_rows") or 0)}


def write_bronze_trips_file(df, table, source_file, evolve):
    """
    Replace exactly this file's rows in Bronze (re-running a file never duplicates it).
    Columns the table already has but this file lacks (added by an earlier schema evolution) are filled with NULL
    of the table's own type; columns the table does not have are only written when evolve=True (mergeSchema).
    Also used (as write_bronze_file) for station_information.
    """
    spark = _spark()
    if spark.catalog.tableExists(table):
        fields = spark.table(table).schema.fields
        table_cols = [f.name for f in fields]
        for f in fields:
            if f.name not in df.columns:
                df = df.withColumn(f.name, F.lit(None).cast(f.dataType))
        new_cols = [c for c in df.columns if c not in table_cols]
        df = df.select(*table_cols, *new_cols)
        (df.write.format("delta").mode("overwrite")
           .option("replaceWhere", f"source_file = '{source_file}'")
           .option("mergeSchema", "true" if (evolve and new_cols) else "false")
           .saveAsTable(table))
    else:
        df.write.format("delta").saveAsTable(table)


write_bronze_file = write_bronze_trips_file   # generic "replace this file's rows" writer, also used for station_information

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Audit logging — one row per layer/file/table processed

# COMMAND ----------

def new_context(notebook, load_type, batch_id=""):
    return {
        "run_id": uuid.uuid4().hex,
        "batch_id": batch_id.strip() or datetime.now(timezone.utc).strftime("batch_%Y%m%dT%H%M%SZ"),
        "notebook": notebook,
        "load_type": load_type,
    }


def write_log(cfg, ctx, layer, parameter, start, end, status, metrics=None):
    m = metrics or {}
    row = (
        ctx["run_id"], ctx["batch_id"], layer, ctx["notebook"], ctx["load_type"], parameter,
        start, end, round((end - start).total_seconds(), 2), status,
        m.get("rows_read"), m.get("rows_inserted"), m.get("rows_updated"), m.get("rows_quarantined"),
        (m.get("message") or "")[:2000], datetime.now(timezone.utc),
    )
    _spark().createDataFrame([row], LOG_SCHEMA).write.mode("append").saveAsTable(cfg["logs"])


def run_step(cfg, ctx, layer, parameter, fn):
    """
    Run one unit of work (one file / one month / one table), always write an audit row.
    A failure is logged and returned, so one bad file does not stop the rest of the batch.
    """
    start = datetime.now(timezone.utc)
    status, metrics = "SUCCESS", {}
    try:
        metrics = fn() or {}
    except Exception as e:
        status, metrics = "FAILURE", {"message": f"{type(e).__name__}: {str(e)[:1500]}"}
    end = datetime.now(timezone.utc)
    write_log(cfg, ctx, layer, parameter, start, end, status, metrics)
    print(f"[{status}] {layer} | {parameter} | {metrics}")
    return status, metrics


def month_range(start_month: str, end_month: str):
    """Inclusive list of 'YYYYMM' strings, e.g. ('202603','202605') -> 202603, 202604, 202605."""
    for m in (start_month, end_month):
        if not re.fullmatch(r"\d{6}", m) or not 1 <= int(m[4:]) <= 12:
            raise ValueError(f"Month must look like YYYYMM (e.g. 202603), got '{m}'")
    y, mo = int(start_month[:4]), int(start_month[4:])
    out = []
    while f"{y}{mo:02d}" <= end_month:
        out.append(f"{y}{mo:02d}")
        mo += 1
        if mo == 13:
            y, mo = y + 1, 1
    return out

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Gold layer — star schema builders (Phase 3)
# MAGIC Pure functions: Silver / Gold DataFrames in, Gold DataFrames out. The notebooks decide *what* to write and *how*.
# MAGIC
# MAGIC **Surrogate keys are deterministic hashes** (`xxhash64` of the natural key), never auto-increment numbers.
# MAGIC The same station always gets the same key on every run, re-run and backfill, so facts can be built without
# MAGIC joining to the dimension (no shuffle) and the whole Gold layer stays idempotent.

# COMMAND ----------

UNKNOWN_KEY = -1
NYC_TZ = "America/New_York"

DIM_STATION_COLUMNS = [
    "station_key", "station_id", "station_name", "latitude", "longitude", "gbfs_station_id", "capacity",
    "has_trip_history", "has_live_feed", "row_hash", "batch_id", "load_timestamp",
]
DIM_DATE_COLUMNS = [
    "date_key", "full_date", "year", "quarter", "quarter_label", "month", "month_name", "month_short",
    "year_month", "year_month_key", "day_of_month", "day_of_week", "day_of_week_mon", "day_name", "day_short",
    "is_weekend", "week_of_year", "season", "row_hash", "batch_id", "load_timestamp",
]
DIM_TIME_COLUMNS = [
    "time_key", "hour_label", "hour_12_label", "time_of_day", "time_of_day_sort", "is_commute_peak",
    "row_hash", "batch_id", "load_timestamp",
]
DIM_RIDER_TYPE_COLUMNS = [
    "rider_type_key", "member_casual", "rideable_type", "member_label", "bike_label", "rider_type_label",
    "row_hash", "batch_id", "load_timestamp",
]
FACT_TRIPS_BUSINESS_COLUMNS = [
    "ride_id", "date_key", "time_key", "start_station_key", "end_station_key", "rider_type_key",
    "started_at", "ended_at", "trip_duration_seconds", "trip_duration_minutes", "distance_km", "is_round_trip",
]
FACT_TRIPS_COLUMNS = FACT_TRIPS_BUSINESS_COLUMNS + ["source_month", "row_hash", "batch_id", "load_timestamp"]
FACT_AVAILABILITY_BUSINESS_COLUMNS = [
    "gbfs_station_id", "poll_timestamp", "poll_timestamp_local", "station_key", "date_key", "time_key",
    "bikes_available", "ebikes_available", "classic_bikes_available", "docks_available", "capacity",
    "fill_ratio", "is_empty", "is_full", "is_renting", "is_returning", "last_reported_ts",
]
FACT_AVAILABILITY_COLUMNS = FACT_AVAILABILITY_BUSINESS_COLUMNS + ["row_hash", "batch_id", "load_timestamp"]
GOLD_DAILY_COLUMNS = [
    "date_key", "total_trips", "member_trips", "casual_trips", "classic_trips", "electric_trips", "round_trips",
    "avg_duration_minutes", "median_duration_minutes", "total_distance_km", "avg_distance_km", "member_share",
    "batch_id", "load_timestamp",
]
GOLD_STATION_POPULARITY_COLUMNS = [
    "year_month_key", "station_key", "departures", "arrivals", "net_flow", "total_activity",
    "member_departures", "casual_departures", "avg_departure_duration_minutes", "activity_rank",
    "batch_id", "load_timestamp",
]
GOLD_HOURLY_COLUMNS = [
    "day_of_week", "day_of_week_mon", "day_name", "time_key", "total_trips", "member_trips", "casual_trips",
    "days_observed", "avg_trips_per_day", "avg_duration_minutes", "batch_id", "load_timestamp",
]


def station_key(col):
    """Deterministic surrogate key for a station natural id; NULL id -> the 'Unknown station' member."""
    return F.when(col.isNull(), F.lit(UNKNOWN_KEY).cast("bigint")).otherwise(F.xxhash64(col))


def date_key(col):
    """DATE/TIMESTAMP -> INT yyyymmdd."""
    return F.date_format(col, "yyyyMMdd").cast("int")


def rider_type_key(member_col, rideable_col):
    return F.xxhash64(F.coalesce(member_col, F.lit("unknown")), F.coalesce(rideable_col, F.lit("unknown")))


def haversine_km(lat1, lon1, lat2, lon2):
    """Straight-line distance between two coordinates in km (NULL if any coordinate is missing)."""
    r = F.lit(6371.0)
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    return F.round(2 * r * F.asin(F.sqrt(a)), 3)


def _stamp(df, batch_id, hash_columns=None):
    if hash_columns:
        df = df.withColumn("row_hash", _hash(hash_columns))
    return df.withColumn("batch_id", F.lit(batch_id)).withColumn("load_timestamp", F.current_timestamp())


# ---------------- Dimensions ----------------

def build_dim_station(silver_stations_df, station_info_df, batch_id):
    """
    Conformed station dimension shared by both facts.
    Trip files identify stations by short id ("5989.02"); the live GBFS feed uses a UUID / 19-digit id.
    station_information carries both (short_name + station_id), so it is the bridge between the two.
    * Natural key: trip station_id, else short_name, else the GBFS id (station seen only in the live feed).
    * Name and coordinates prefer the official station_information values, else the trip-derived ones.
    * Includes an 'Unknown station' row (key -1) for trips that have coordinates but no station id.
    """
    s = silver_stations_df.select(
        F.col("station_id").alias("t_station_id"), F.col("station_name").alias("t_name"),
        F.col("latitude").alias("t_lat"), F.col("longitude").alias("t_lon"))

    # if two GBFS ids share a short_name, keep the most recently updated one
    one_per_short = Window.partitionBy("short_name").orderBy(F.col("info_as_of").desc(), F.col("gbfs_station_id"))
    info = station_info_df.select(
        "gbfs_station_id", "short_name", F.col("station_name").alias("i_name"),
        F.col("latitude").alias("i_lat"), F.col("longitude").alias("i_lon"), "capacity", "info_as_of")
    mapped = (info.where(F.col("short_name").isNotNull())
              .withColumn("_rn", F.row_number().over(one_per_short)).where("_rn = 1").drop("_rn"))
    unmapped = info.where(F.col("short_name").isNull())

    joined = s.join(mapped, s["t_station_id"] == mapped["short_name"], "full_outer").select(
        F.coalesce("t_station_id", "short_name").alias("station_id"),
        F.coalesce("i_name", "t_name").alias("station_name"),
        F.coalesce("i_lat", "t_lat").alias("latitude"),
        F.coalesce("i_lon", "t_lon").alias("longitude"),
        "gbfs_station_id", "capacity",
        F.col("t_station_id").isNotNull().alias("has_trip_history"),
        F.col("gbfs_station_id").isNotNull().alias("has_live_feed"),
    )
    live_only = unmapped.select(
        F.col("gbfs_station_id").alias("station_id"), F.col("i_name").alias("station_name"),
        F.col("i_lat").alias("latitude"), F.col("i_lon").alias("longitude"),
        "gbfs_station_id", "capacity",
        F.lit(False).alias("has_trip_history"), F.lit(True).alias("has_live_feed"),
    )
    spark = _spark()
    unknown = spark.createDataFrame(
        [("UNKNOWN", "Unknown station", None, None, None, None, False, False)],
        "station_id string, station_name string, latitude double, longitude double, gbfs_station_id string, "
        "capacity int, has_trip_history boolean, has_live_feed boolean")

    dim = (joined.unionByName(live_only)
           .withColumn("station_key", station_key(F.col("station_id")))
           .unionByName(unknown.withColumn("station_key", F.lit(UNKNOWN_KEY).cast("bigint")))
           .withColumn("latitude", F.round("latitude", 6)).withColumn("longitude", F.round("longitude", 6)))
    business = [c for c in DIM_STATION_COLUMNS if c not in ("row_hash", "batch_id", "load_timestamp")]
    return _stamp(dim, batch_id, business).select(*DIM_STATION_COLUMNS)


def build_dim_date(start_date, end_date, batch_id):
    """One row per calendar day from start_date to end_date (inclusive). 1 = Sunday ... 7 = Saturday, as in Silver."""
    d = _spark().sql(
        f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS full_date")
    m = F.month("full_date")
    dim = d.select(
        date_key(F.col("full_date")).alias("date_key"), "full_date",
        F.year("full_date").alias("year"), F.quarter("full_date").alias("quarter"),
        F.concat(F.lit("Q"), F.quarter("full_date")).alias("quarter_label"),
        m.alias("month"), F.date_format("full_date", "MMMM").alias("month_name"),
        F.date_format("full_date", "MMM").alias("month_short"),
        F.date_format("full_date", "yyyy-MM").alias("year_month"),
        F.date_format("full_date", "yyyyMM").cast("int").alias("year_month_key"),
        F.dayofmonth("full_date").alias("day_of_month"),
        F.dayofweek("full_date").alias("day_of_week"),
        ((F.dayofweek("full_date") + 5) % 7 + 1).alias("day_of_week_mon"),   # 1 = Monday ... 7 = Sunday (sort order)
        F.date_format("full_date", "EEEE").alias("day_name"),
        F.date_format("full_date", "EEE").alias("day_short"),
        F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
        F.weekofyear("full_date").alias("week_of_year"),
        F.when(m.isin(12, 1, 2), "Winter").when(m.isin(3, 4, 5), "Spring")
         .when(m.isin(6, 7, 8), "Summer").otherwise("Autumn").alias("season"),
    )
    business = [c for c in DIM_DATE_COLUMNS if c not in ("row_hash", "batch_id", "load_timestamp")]
    return _stamp(dim, batch_id, business).select(*DIM_DATE_COLUMNS)


def build_dim_time(batch_id):
    """24 rows, one per hour of the day (trip start hour / poll hour, New York local time)."""
    h = F.col("id").cast("int")
    dim = _spark().range(24).select(
        h.alias("time_key"),
        F.format_string("%02d:00", h).alias("hour_label"),
        F.format_string("%d %s", F.when(h % 12 == 0, 12).otherwise(h % 12),
                        F.when(h < 12, "AM").otherwise("PM")).alias("hour_12_label"),
        F.when(h < 6, "Night").when(h < 11, "Morning").when(h < 16, "Midday")
         .when(h < 21, "Evening").otherwise("Night").alias("time_of_day"),
        F.when(h < 6, 4).when(h < 11, 1).when(h < 16, 2).when(h < 21, 3).otherwise(4).alias("time_of_day_sort"),
        (h.between(7, 9) | h.between(17, 19)).alias("is_commute_peak"),
    )
    business = [c for c in DIM_TIME_COLUMNS if c not in ("row_hash", "batch_id", "load_timestamp")]
    return _stamp(dim, batch_id, business).select(*DIM_TIME_COLUMNS)


def build_dim_rider_type(silver_trips_df, batch_id):
    """Junk dimension: every (member_casual, rideable_type) combination seen in Silver, plus the expected four."""
    spark = _spark()
    seed = spark.createDataFrame(
        [(m, r) for m in ("member", "casual") for r in ("classic_bike", "electric_bike")],
        "member_casual string, rideable_type string")
    combos = silver_trips_df.select("member_casual", "rideable_type").distinct().unionByName(seed).distinct()
    member_label = F.initcap(F.coalesce("member_casual", F.lit("unknown")))
    bike_label = F.initcap(F.regexp_replace(F.coalesce("rideable_type", F.lit("unknown")), "_bike$", ""))
    bike_label = F.regexp_replace(bike_label, "_", " ")
    dim = combos.select(
        rider_type_key(F.col("member_casual"), F.col("rideable_type")).alias("rider_type_key"),
        "member_casual", "rideable_type",
        member_label.alias("member_label"), bike_label.alias("bike_label"),
        F.concat_ws(" · ", member_label, bike_label).alias("rider_type_label"),
    )
    business = [c for c in DIM_RIDER_TYPE_COLUMNS if c not in ("row_hash", "batch_id", "load_timestamp")]
    return _stamp(dim, batch_id, business).select(*DIM_RIDER_TYPE_COLUMNS)


# ---------------- Facts ----------------

def build_fact_trips(silver_trips_df, batch_id):
    """
    One row per validated trip. Keys are computed (hashes / date formats), not looked up,
    so this transformation needs no join and no shuffle; dimension coverage is verified in 99_validation.
    """
    t = silver_trips_df
    fact = t.select(
        "ride_id",
        date_key(F.col("started_at")).alias("date_key"),
        F.hour("started_at").alias("time_key"),
        station_key(F.col("start_station_id")).alias("start_station_key"),
        station_key(F.col("end_station_id")).alias("end_station_key"),
        rider_type_key(F.col("member_casual"), F.col("rideable_type")).alias("rider_type_key"),
        "started_at", "ended_at", "trip_duration_seconds",
        F.round(F.col("trip_duration_seconds") / 60.0, 2).alias("trip_duration_minutes"),
        haversine_km(F.col("start_lat"), F.col("start_lng"), F.col("end_lat"), F.col("end_lng")).alias("distance_km"),
        (F.col("start_station_id").isNotNull() & (F.col("start_station_id") == F.col("end_station_id"))).alias("is_round_trip"),
        "source_month",
    )
    return _stamp(fact, batch_id, FACT_TRIPS_BUSINESS_COLUMNS).select(*FACT_TRIPS_COLUMNS)


def build_fact_station_availability(silver_gbfs_df, dim_station_df, batch_id):
    """
    Periodic snapshot fact: one row per installed station per hourly poll.
    * Joined to dim_station on gbfs_station_id (a small table -> broadcast join, no shuffle of the snapshots).
    * Stations that are not installed are excluded (they report 0 bikes / 0 docks and would distort 'empty' KPIs).
    * last_reported values before 2000 are GBFS placeholders (e.g. 86400 = 1970-01-02) and become NULL.
    * Poll time is converted from UTC to New York local time so it lines up with trip times.
      Requires spark.sql.session.timeZone = UTC (set by the calling notebook).
    """
    dim = dim_station_df.where(F.col("gbfs_station_id").isNotNull()).select(
        F.col("gbfs_station_id").alias("_gid"), "station_key", F.col("capacity").alias("_capacity"))
    g = silver_gbfs_df.where(F.col("is_installed") == True)   # noqa: E712 (Spark column comparison)
    local = F.from_utc_timestamp(F.col("poll_timestamp"), NYC_TZ)
    bikes, docks = F.col("num_bikes_available"), F.col("num_docks_available")
    denominator = F.coalesce(F.col("_capacity"), bikes + docks)
    fact = (g.join(F.broadcast(dim), g["station_id"] == dim["_gid"], "left")
            .select(
                F.col("station_id").alias("gbfs_station_id"), "poll_timestamp",
                local.alias("poll_timestamp_local"),
                F.coalesce("station_key", F.lit(UNKNOWN_KEY).cast("bigint")).alias("station_key"),
                date_key(local).alias("date_key"), F.hour(local).alias("time_key"),
                bikes.alias("bikes_available"),
                F.col("num_ebikes_available").alias("ebikes_available"),
                F.greatest(bikes - F.coalesce(F.col("num_ebikes_available"), F.lit(0)), F.lit(0)).alias("classic_bikes_available"),
                docks.alias("docks_available"),
                F.col("_capacity").alias("capacity"),
                F.when(denominator > 0, F.round(F.least(bikes / denominator, F.lit(1.0)), 4)).alias("fill_ratio"),
                (bikes == 0).alias("is_empty"), (docks == 0).alias("is_full"),
                "is_renting", "is_returning",
                F.when(F.col("last_reported_ts") >= F.lit("2000-01-01").cast("timestamp"), F.col("last_reported_ts"))
                 .alias("last_reported_ts"),
            ))
    return _stamp(fact, batch_id, FACT_AVAILABILITY_BUSINESS_COLUMNS).select(*FACT_AVAILABILITY_COLUMNS)


# ---------------- Aggregates (built from fact_trips) ----------------

def build_gold_daily(fact_df, dim_rider_df, batch_id):
    f = fact_df.join(F.broadcast(dim_rider_df.select("rider_type_key", "member_casual", "rideable_type")), "rider_type_key", "left")
    cnt = lambda cond: F.sum(F.when(cond, 1).otherwise(0)).cast("bigint")
    agg = f.groupBy("date_key").agg(
        F.count("*").alias("total_trips"),
        cnt(F.col("member_casual") == "member").alias("member_trips"),
        cnt(F.col("member_casual") == "casual").alias("casual_trips"),
        cnt(F.col("rideable_type") == "classic_bike").alias("classic_trips"),
        cnt(F.col("rideable_type") == "electric_bike").alias("electric_trips"),
        cnt(F.col("is_round_trip")).alias("round_trips"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
        F.round(F.percentile_approx("trip_duration_minutes", 0.5), 2).alias("median_duration_minutes"),
        F.round(F.sum("distance_km"), 1).alias("total_distance_km"),
        F.round(F.avg("distance_km"), 3).alias("avg_distance_km"),
    ).withColumn("member_share", F.round(F.col("member_trips") / F.col("total_trips"), 4))
    return _stamp(agg, batch_id).select(*GOLD_DAILY_COLUMNS)


def build_gold_station_popularity(fact_df, dim_rider_df, batch_id):
    """Departures and arrivals per station per month. net_flow > 0 = station drains bikes (needs refilling)."""
    f = fact_df.join(F.broadcast(dim_rider_df.select("rider_type_key", "member_casual")), "rider_type_key", "left")
    ym = (F.col("date_key") / 100).cast("int").alias("year_month_key")
    dep = f.groupBy(ym, F.col("start_station_key").alias("station_key")).agg(
        F.count("*").alias("departures"),
        F.sum(F.when(F.col("member_casual") == "member", 1).otherwise(0)).cast("bigint").alias("member_departures"),
        F.sum(F.when(F.col("member_casual") == "casual", 1).otherwise(0)).cast("bigint").alias("casual_departures"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_departure_duration_minutes"))
    arr = f.groupBy(ym, F.col("end_station_key").alias("station_key")).agg(F.count("*").alias("arrivals"))
    z = F.lit(0).cast("bigint")
    out = (dep.join(arr, ["year_month_key", "station_key"], "full_outer")
           .select("year_month_key", "station_key",
                   F.coalesce("departures", z).alias("departures"), F.coalesce("arrivals", z).alias("arrivals"),
                   F.coalesce("member_departures", z).alias("member_departures"),
                   F.coalesce("casual_departures", z).alias("casual_departures"),
                   "avg_departure_duration_minutes")
           .withColumn("net_flow", F.col("departures") - F.col("arrivals"))
           .withColumn("total_activity", F.col("departures") + F.col("arrivals"))
           .withColumn("activity_rank", F.dense_rank().over(
               Window.partitionBy("year_month_key").orderBy(F.col("total_activity").desc()))))
    return _stamp(out, batch_id).select(*GOLD_STATION_POPULARITY_COLUMNS)


def build_gold_hourly(fact_df, dim_date_df, dim_rider_df, batch_id):
    """Day-of-week x hour heatmap (168 rows), with average trips per observed day for fair comparison."""
    d = dim_date_df.select("date_key", "day_of_week", "day_of_week_mon", "day_name")
    f = (fact_df.join(F.broadcast(d), "date_key")
         .join(F.broadcast(dim_rider_df.select("rider_type_key", "member_casual")), "rider_type_key", "left"))
    days = (fact_df.select("date_key").distinct().join(F.broadcast(d), "date_key")
            .groupBy("day_of_week").agg(F.count("*").alias("days_observed")))
    agg = f.groupBy("day_of_week", "day_of_week_mon", "day_name", "time_key").agg(
        F.count("*").alias("total_trips"),
        F.sum(F.when(F.col("member_casual") == "member", 1).otherwise(0)).cast("bigint").alias("member_trips"),
        F.sum(F.when(F.col("member_casual") == "casual", 1).otherwise(0)).cast("bigint").alias("casual_trips"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"))
    out = (agg.join(F.broadcast(days), "day_of_week")
           .withColumn("avg_trips_per_day", F.round(F.col("total_trips") / F.col("days_observed"), 1)))
    return _stamp(out, batch_id).select(*GOLD_HOURLY_COLUMNS)


# ---------------- Gold writers ----------------

def replace_range(table, df, predicate):
    """
    Atomically replace only the rows matching `predicate` (e.g. a date range) with df.
    Used for aggregates: re-running a range rewrites exactly that range, so results never double count.
    Returns {"rows_inserted": n, "rows_updated": 0}.
    """
    n = df.count()
    (df.write.format("delta").mode("overwrite").option("replaceWhere", predicate).saveAsTable(table))
    return {"rows_inserted": n, "rows_updated": 0}


def overwrite_table(table, df):
    """Replace the whole contents of a small table, keeping its definition (clustering, comments)."""
    view = "_ovw_" + uuid.uuid4().hex[:12]
    df.createOrReplaceTempView(view)
    cols = ", ".join(df.columns)
    _spark().sql(f"INSERT OVERWRITE {table} ({cols}) SELECT {cols} FROM {view}")
    return {"rows_inserted": _spark().table(table).count(), "rows_updated": 0}


def refresh_dimensions(cfg, ctx):
    """
    Rebuild every dimension from Silver and MERGE it into Gold (one audited step per dimension).
    Called at the start of both Gold notebooks; unchanged rows are skipped by row_hash, so it is cheap and idempotent.
    """
    spark = _spark()
    layer = "silver_to_gold"

    def dim_station():
        stations = spark.table(cfg["silver_stations"])
        info = spark.table(cfg["silver_station_info"])
        dim = build_dim_station(stations, info, ctx["batch_id"])
        n_linked = dim.where("has_trip_history AND has_live_feed").count()
        res = merge_into(cfg["dim_station"], dim, ["station_key"])
        return {"rows_read": stations.count() + info.count(), **res,
                "message": f"{n_linked} stations linked to the live GBFS feed"}

    def dim_date():
        trips = spark.table(cfg["silver_trips"]).select(F.col("trip_date").alias("d"))
        polls = spark.table(cfg["silver_gbfs"]).select(F.to_date(F.from_utc_timestamp("poll_timestamp", NYC_TZ)).alias("d"))
        lo, hi = trips.unionByName(polls).agg(F.min("d"), F.max("d")).collect()[0]
        if lo is None:
            raise ValueError("Silver has no trips or GBFS snapshots yet")
        # whole calendar years, so Power BI time-intelligence functions (YTD, same period last year) work
        start, end = f"{lo.year}-01-01", f"{hi.year}-12-31"
        dim = build_dim_date(start, end, ctx["batch_id"])
        res = merge_into(cfg["dim_date"], dim, ["date_key"])
        return {"rows_read": dim.count(), **res, "message": f"calendar {start} to {end}"}

    def dim_time():
        dim = build_dim_time(ctx["batch_id"])
        return {"rows_read": 24, **merge_into(cfg["dim_time"], dim, ["time_key"])}

    def dim_rider_type():
        dim = build_dim_rider_type(spark.table(cfg["silver_trips"]), ctx["batch_id"])
        return {"rows_read": dim.count(), **merge_into(cfg["dim_rider_type"], dim, ["rider_type_key"])}

    return [run_step(cfg, ctx, layer, name, fn) for name, fn in
            [("dim_station", dim_station), ("dim_date", dim_date), ("dim_time", dim_time), ("dim_rider_type", dim_rider_type)]]
