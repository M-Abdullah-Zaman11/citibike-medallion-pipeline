"""
Local unit tests for the pure-Spark logic in notebooks/01_common.py
(schema-on-read, drift handling, casting, quarantine rules).

The Delta MERGE / Unity Catalog parts only exist inside Databricks and are verified there
with notebooks/99_validation.py.

Run (needs Java 17+):  pip install pyspark pytest && pytest -q tests
"""
import os
import shutil
import sys
from pathlib import Path

import pytest

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
from pyspark.sql import SparkSession  # noqa: E402

COMMON = Path(__file__).resolve().parents[1] / "notebooks" / "01_common.py"
HEADER = ("ride_id,rideable_type,started_at,ended_at,start_station_name,start_station_id,end_station_name,"
          "end_station_id,start_lat,start_lng,end_lat,end_lng,member_casual")
GOOD1 = "F29629035B8BAAB2,classic_bike,2026-03-15 12:13:34.479,2026-03-15 12:17:05.525,W 15 St & 6 Ave,5989.02,E 10 St & 5 Ave,5872.08,40.738,-73.996,40.733,-73.995,member"
GOOD2 = "B5788172D67F8E85,electric_bike,2026-03-30 09:49:44.666,2026-03-30 09:58:10.050,Canal St & Rutgers St,5303.08,Hudson St & Reade St,5359.10,40.714,-73.989,40.716,-74.009,member"
BAD = {
    "duplicate":       GOOD1,
    "no_station_ok":   "NOSTATION00000001,electric_bike,2026-03-16 08:00:00.000,2026-03-16 08:10:00.000,,,,,40.7,-73.9,40.71,-73.91,casual",
    "no_location":     "NOLOC0000000001,electric_bike,2026-03-16 08:00:00.000,2026-03-16 08:10:00.000,,,,,,,,,casual",
    "bad_timestamp":   "BADTS00000000001,classic_bike,not-a-date,2026-03-16 08:10:00.000,A,1.01,B,2.02,40.7,-73.9,40.71,-73.91,member",
    "negative":        "NEG0000000000001,classic_bike,2026-03-16 09:00:00.000,2026-03-16 08:00:00.000,A,1.01,B,2.02,40.7,-73.9,40.71,-73.91,member",
    "too_long":        "LONG000000000001,classic_bike,2026-03-16 09:00:00.000,2026-03-18 09:00:00.000,A,1.01,B,2.02,40.7,-73.9,40.71,-73.91,member",
    "no_id":           ",classic_bike,2026-03-16 09:00:00.000,2026-03-16 09:10:00.000,A,1.01,B,2.02,40.7,-73.9,40.71,-73.91,member",
    "bad_member":      "SUB0000000000001,classic_bike,2026-03-16 09:00:00.000,2026-03-16 09:10:00.000,A,1.01,B,2.02,40.7,-73.9,40.71,-73.91,subscriber",
    "truncated":       "SHORT00000000001,classic_bike,2026-03-16 09:00:00.000",
    "text_coordinate": "TEXTLAT000000001,classic_bike,2026-03-16 09:00:00.000,2026-03-16 09:10:00.000,A,1.01,B,2.02,N/A,-73.9,40.71,-73.91,member",
}


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    yield s
    s.stop()


@pytest.fixture(scope="session")
def lib(spark):
    ns = {}
    exec(compile(COMMON.read_text(), str(COMMON), "exec"), ns)
    return ns


def write_csv(path: Path, lines):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")
    return f"file://{path}"


def as_bronze(spark, lib, csv_url, tmp_path, name, evolve=False):
    """Read raw CSV -> add metadata -> round-trip through parquet (stands in for the Delta Bronze table)."""
    df, drift = lib["read_raw_csv"](csv_url, lib["TRIP_COLUMNS"], lib["TRIP_RAW_SCHEMA"], evolve)
    df = lib["add_bronze_metadata"](df, name, "test-batch", "full", "202603")
    out = str(tmp_path / f"bronze_{name}")
    shutil.rmtree(out, ignore_errors=True)
    df.write.parquet(out)
    return spark.read.parquet(out), drift


def test_month_range(lib):
    assert lib["month_range"]("202511", "202602") == ["202511", "202512", "202601", "202602"]
    with pytest.raises(ValueError):
        lib["month_range"]("2026-03", "202604")


def test_silver_types_and_rules(spark, lib, tmp_path):
    url = write_csv(tmp_path / "ok.csv", [HEADER, GOOD1, GOOD2, *BAD.values()])
    bronze, drift = as_bronze(spark, lib, url, tmp_path, "ok")
    assert drift["conforming"]
    valid, quarantine = lib["transform_trips_silver"](bronze, "test-batch")

    types = dict(valid.dtypes)
    assert types["started_at"] == "timestamp" and types["start_lat"] == "double"
    assert types["trip_duration_seconds"] == "bigint" and types["trip_date"] == "date"
    assert types["start_station_id"] == "string", "station ids are identifiers, not numbers"

    ids = {r.ride_id for r in valid.collect()}
    assert ids == {"F29629035B8BAAB2", "B5788172D67F8E85", "NOSTATION00000001"}   # duplicate collapsed, no-station-with-coords kept
    assert valid.where("ride_id = 'F29629035B8BAAB2'").first().trip_duration_seconds == 211

    reasons = {r.record_key: r.quarantine_reason for r in quarantine.collect()}
    assert "invalid_start_location" in reasons["NOLOC0000000001"]
    assert reasons["BADTS00000000001"] == "invalid_started_at"
    assert reasons["NEG0000000000001"] == "negative_duration"
    assert reasons["LONG000000000001"] == "duration_over_limit"
    assert reasons["SUB0000000000001"] == "invalid_member_type"
    assert "corrupt_record" in reasons["SHORT00000000001"]
    assert reasons[None] == "missing_ride_id"
    assert reasons["TEXTLAT000000001"] == "invalid_coordinates", "a text value in a number column must be quarantined, not silently NULLed"


def test_row_hash_is_stable_across_runs(spark, lib, tmp_path):
    url = write_csv(tmp_path / "ok2.csv", [HEADER, GOOD1, GOOD2])
    bronze, _ = as_bronze(spark, lib, url, tmp_path, "ok2")
    h1 = {r.ride_id: r.row_hash for r in lib["transform_trips_silver"](bronze, "run-1")[0].collect()}
    h2 = {r.ride_id: r.row_hash for r in lib["transform_trips_silver"](bronze, "run-2")[0].collect()}
    assert h1 == h2, "same data must hash the same, otherwise MERGE would update unchanged rows"


def test_drift_extra_column_captured_or_evolved(spark, lib, tmp_path):
    url = write_csv(tmp_path / "extra.csv", [HEADER + ",new_flag", GOOD1 + ",Y", GOOD2 + ",N"])
    df, drift = lib["read_raw_csv"](url, lib["TRIP_COLUMNS"], lib["TRIP_RAW_SCHEMA"], evolve_schema=False)
    assert drift["extra"] == ["new_flag"] and "new_flag" not in df.columns
    assert '"new_flag":"Y"' in df.first()["_extra_columns"]
    df2, d2 = lib["read_raw_csv"](url, lib["TRIP_COLUMNS"], lib["TRIP_RAW_SCHEMA"], evolve_schema=True)
    assert "new_flag" in df2.columns and d2["evolved"]


def test_drift_missing_column_is_quarantined_not_fatal(spark, lib, tmp_path):
    cut = lambda s: s.rsplit(",", 1)[0]
    url = write_csv(tmp_path / "missing.csv", [cut(HEADER), cut(GOOD1), cut(GOOD2)])
    bronze, drift = as_bronze(spark, lib, url, tmp_path, "missing")
    assert drift["missing"] == ["member_casual"]
    valid, quarantine = lib["transform_trips_silver"](bronze, "test-batch")
    assert valid.count() == 0 and quarantine.count() == 2


def test_drift_reordered_columns_read_by_name(spark, lib, tmp_path):
    cols = HEADER.split(",")
    move = lambda line: ",".join([line.split(",")[-1]] + line.split(",")[:-1])
    url = write_csv(tmp_path / "reordered.csv", [",".join([cols[-1]] + cols[:-1]), move(GOOD1), move(GOOD2)])
    df, drift = lib["read_raw_csv"](url, lib["TRIP_COLUMNS"], lib["TRIP_RAW_SCHEMA"])
    assert not drift["conforming"] and not drift["missing"] and not drift["extra"]
    row = df.where("ride_id = 'F29629035B8BAAB2'").first()
    assert row["member_casual"] == "member" and row["start_station_id"] == "5989.02"


def test_gbfs_flag_formats_and_rules(spark, lib, tmp_path):
    gh = "poll_timestamp_utc,station_id,num_bikes_available,num_ebikes_available,num_docks_available,is_installed,is_renting,is_returning,last_reported"
    url = write_csv(tmp_path / "g.csv", [gh,
        "2026-10-09T22:23:59.341349+00:00,abc-1,5,2,10,1,1,1,1760048000",
        "2026-10-09T22:23:59.341349+00:00,abc-2,0,0,12,True,False,True,1760048001",
        "2026-10-09T22:23:59.341349+00:00,,3,1,4,1,1,1,1760048002",
        "2026-10-09T22:23:59.341349+00:00,abc-3,-4,1,4,1,1,1,1760048002"])
    df, _ = lib["read_raw_csv"](url, lib["GBFS_COLUMNS"], lib["GBFS_RAW_SCHEMA"])
    df = lib["add_bronze_metadata"](df, "g.csv", "b", "incremental")
    out = str(tmp_path / "bronze_g"); df.write.parquet(out)
    valid, quarantine = lib["transform_gbfs_silver"](spark.read.parquet(out), "b")
    rows = {r.station_id: r for r in valid.collect()}
    assert rows["abc-1"].is_renting is True and rows["abc-2"].is_renting is False
    assert dict(valid.dtypes)["poll_timestamp"] == "timestamp" and dict(valid.dtypes)["num_bikes_available"] == "int"
    reasons = {r.record_key: r.quarantine_reason for r in quarantine.collect()}
    assert reasons[None] == "missing_station_id" and reasons["abc-3"] == "invalid_bike_count"
