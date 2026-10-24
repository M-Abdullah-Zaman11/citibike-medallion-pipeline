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
    Columns the table already has but this file lacks (added by an earlier schema evolution) are filled with NULL;
    columns the table does not have are only written when evolve=True (mergeSchema).
    """
    spark = _spark()
    if spark.catalog.tableExists(table):
        table_cols = spark.table(table).columns
        for c in table_cols:
            if c not in df.columns:
                df = df.withColumn(c, F.lit(None).cast("string"))
        new_cols = [c for c in df.columns if c not in table_cols]
        df = df.select(*table_cols, *new_cols)
        (df.write.format("delta").mode("overwrite")
           .option("replaceWhere", f"source_file = '{source_file}'")
           .option("mergeSchema", "true" if (evolve and new_cols) else "false")
           .saveAsTable(table))
    else:
        df.write.format("delta").saveAsTable(table)

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
