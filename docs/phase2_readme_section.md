

---

# Phase 2 — Core Pipeline (Bronze & Silver)

PySpark pipeline on **Databricks Free Edition (serverless)**, storing Delta tables in Unity Catalog under the `workspace` catalog.

```
 laptop ──upload──▶ Volume landing/            Bronze (strings, lineage)          Silver (typed, validated)
 fetch_samples.py     trips/<YYYYMM>/*.csv ──▶  bronze_trips ───────────────────▶   silver_trips ──▶ silver_stations
 poll_gbfs.py         gbfs/*.csv           ──▶  bronze_gbfs_station_status ─────▶   silver_gbfs_station_status
                                                                                    quarantine_records (bad rows + reason)
                       every step of every layer ──▶ citibike_ops.pipeline_execution_logs
```

| Notebook | Purpose |
|---|---|
| `notebooks/00_setup.py` | Creates schemas, landing volume and every table with explicit column types (safe to re-run) |
| `notebooks/01_common.py` | Shared library (`%run` by the others): contracts/schemas, drift handling, Silver rules, `MERGE` helper, audit logger |
| `notebooks/02_raw_to_bronze_trips.py` | Raw CSV → `bronze_trips` |
| `notebooks/03_bronze_to_silver_trips.py` | `bronze_trips` → `silver_trips`, `silver_stations`, `quarantine_records` |
| `notebooks/04_gbfs_raw_to_bronze.py` | Live GBFS CSV → `bronze_gbfs_station_status` |
| `notebooks/05_gbfs_bronze_to_silver.py` | GBFS Bronze → `silver_gbfs_station_status` |
| `notebooks/99_validation.py` | Duplicate checks, `load_timestamp` coverage, audit-log evidence |
| `tests/test_transformations.py` | Local unit tests for schema-on-read, drift, casting and quarantine rules |

## Engineering requirements — how each is met

| Requirement | Implementation |
|---|---|
| Explicit schema-on-read | `StructType` contracts in `01_common.py`; `inferSchema` is never used. Where a file's header differs from the contract, an explicit `StructType` is built from that file's own header before reading. |
| Strict types | Bronze keeps every source field as `STRING` so nothing can fail on read. Casting happens into Silver using `try_cast`, so a bad value becomes NULL and is quarantined instead of crashing the batch. Station IDs stay `STRING` (they are identifiers such as `5989.02`). |
| `load_timestamp` everywhere | Present in every Bronze, Silver, quarantine and log table; checked in `99_validation.py`. |
| Idempotency | **Bronze:** each file's rows are replaced atomically (`replaceWhere` on `source_file`); GBFS uses an insert-only `MERGE`. **Silver:** `MERGE INTO` on the primary key, updating only when `row_hash` changed. Re-running the same data gives `rows_inserted = 0`, `rows_updated = 0`. |
| Parameterized backfills | Notebook widgets: month range, load type, landing path, batch id (no hardcoded dates or paths). |
| Schema drift | Detected per file by comparing the header with the contract. *Extra columns:* stored as JSON in `_extra_columns`, or added as real columns with `mergeSchema` when `evolve_schema=true`. *Missing columns:* filled with NULL; rows then fail validation and are quarantined. *Reordered columns:* read by name. *Type changes:* land safely as strings and are quarantined if they no longer cast. *Malformed rows:* kept in `_corrupt_record` and quarantined. |
| Fault tolerance | Each file / month / date is its own step. A failure is logged and the remaining steps still run. `fail_on_error=true` raises an error only at the end. |
| Audit logging | `citibike_ops.pipeline_execution_logs`: one row per step with layer, parameter/file, start and end time, status, rows read / inserted / updated / quarantined, and a message (including drift notes). |

## Data dictionary

Primary keys are **logical** (documented and enforced by the `MERGE` keys) rather than database constraints.

### `citibike_bronze.bronze_trips` — logical PK: (`source_file`, `ride_id`)

| Column | Type | Description |
|---|---|---|
| ride_id | STRING | Trip identifier from the source |
| rideable_type | STRING | `classic_bike` / `electric_bike` |
| started_at, ended_at | STRING | Raw timestamps, e.g. `2026-03-15 12:13:34.479` |
| start_station_name, start_station_id | STRING | Start station (id e.g. `5989.02`) |
| end_station_name, end_station_id | STRING | End station |
| start_lat, start_lng, end_lat, end_lng | STRING | Raw coordinates |
| member_casual | STRING | `member` / `casual` |
| _corrupt_record | STRING | Raw text of a row that could not be parsed (NULL when the row parsed) |
| _extra_columns | STRING | JSON of unexpected columns captured by drift handling (NULL if none) |
| source_file | STRING | Full path of the file the row came from |
| source_month | STRING | `YYYYMM` of the landing folder |
| batch_id | STRING | Pipeline batch that loaded the row |
| load_type | STRING | `full` or `incremental` |
| load_timestamp | TIMESTAMP | When the row was ingested |

### `citibike_bronze.bronze_gbfs_station_status` — logical PK: (`station_id`, `poll_timestamp_utc`)

Same pattern: `poll_timestamp_utc, station_id, num_bikes_available, num_ebikes_available, num_docks_available, is_installed, is_renting, is_returning, last_reported` (all STRING) plus `_corrupt_record, _extra_columns, source_file, batch_id, load_type, load_timestamp`.

### `citibike_silver.silver_trips` — PK: `ride_id`

| Column | Type | Rule |
|---|---|---|
| ride_id | STRING | Trimmed, required, unique |
| rideable_type | STRING | Lower-cased |
| started_at, ended_at | TIMESTAMP | `try_cast`; must parse |
| start_station_name, end_station_name | STRING | Trimmed |
| start_station_id, end_station_id | STRING | Identifier, not a number. May be NULL if coordinates exist |
| start_lat, start_lng, end_lat, end_lng | DOUBLE | `try_cast` |
| member_casual | STRING | Lower-cased; must be in the accepted list (`member`, `casual`) |
| trip_duration_seconds | BIGINT | `ended_at − started_at`; must be 0 to 24 h |
| trip_date | DATE | Date of `started_at` |
| trip_hour | INT | Hour of `started_at` |
| day_of_week | INT | 1 = Sunday … 7 = Saturday |
| row_hash | STRING | SHA-256 of the business columns; decides whether `MERGE` updates a row |
| source_file, source_month, batch_id | STRING | Lineage |
| load_timestamp | TIMESTAMP | When the row was last inserted or changed |

### `citibike_silver.silver_stations` — PK: `station_id`
`station_id STRING, station_name STRING, latitude DOUBLE, longitude DOUBLE, row_hash STRING, batch_id STRING, load_timestamp TIMESTAMP`.
Derived from valid trips: name is the latest seen, coordinates are the average of observed positions.

### `citibike_silver.silver_gbfs_station_status` — PK: (`station_id`, `poll_timestamp`)
`station_id STRING, poll_timestamp TIMESTAMP, num_bikes_available INT, num_ebikes_available INT, num_docks_available INT, is_installed BOOLEAN, is_renting BOOLEAN, is_returning BOOLEAN, last_reported_ts TIMESTAMP, row_hash STRING, source_file STRING, batch_id STRING, load_timestamp TIMESTAMP`.
Flags accept `1/0` and `True/False`; `last_reported` is converted from epoch seconds.

### `citibike_silver.quarantine_records` — PK: (`entity`, `record_hash`)
`entity STRING ('trips'|'gbfs'), record_hash STRING, record_key STRING, source_file STRING, batch_id STRING, quarantine_reason STRING, raw_record STRING (JSON of the original row), load_timestamp TIMESTAMP`.

Quarantine reasons — trips: `corrupt_record, missing_ride_id, invalid_started_at, invalid_ended_at, negative_duration, duration_over_limit, invalid_start_location, invalid_end_location, invalid_member_type`. GBFS: `corrupt_record, missing_station_id, invalid_poll_timestamp, invalid_bike_count, invalid_dock_count`.

### `citibike_ops.pipeline_execution_logs` — PK: (`run_id`, `parameter`, `start_time`)
`run_id, batch_id, pipeline_layer, notebook, load_type, parameter (file / month / date), start_time, end_time, duration_seconds, status (SUCCESS|FAILURE), rows_read, rows_inserted, rows_updated, rows_quarantined, message, load_timestamp`.

## Execution guide

**One-time setup:** import the notebooks into one Databricks workspace folder, run `00_setup`, and upload CSV files into the volume `workspace.citibike_raw.landing` using the folder layout `trips/<YYYYMM>/` and `gbfs/`.

**Standard incremental load** (a new month has been published, e.g. October 2026):
1. On your laptop run `python scripts/fetch_samples.py`; it downloads only the next unseen month.
2. Upload that month's CSVs to `landing/trips/202610/`.
3. Run `02_raw_to_bronze_trips` with `load_type=incremental`, `start_month=202610`, `end_month=202610`.
4. Run `03_bronze_to_silver_trips` with the same three values.
5. Run `99_validation` and check the audit log.

**Backfill / full load** (any historical window, repeatable at will):
1. Upload each month to `landing/trips/<YYYYMM>/`.
2. Run `02` then `03` with `load_type=full`, `start_month=202603`, `end_month=202608`.
3. Re-run only `03` for a range to rebuild Silver from Bronze (for example after changing a validation rule); Bronze is untouched.

**Live GBFS:** upload the latest `station_status_history.csv` to `landing/gbfs/`, then run `04_gbfs_raw_to_bronze` and `05_gbfs_bronze_to_silver`. For a backfill, give `05` a `start_date` and `end_date` (`YYYY-MM-DD`).

**Demonstrating idempotency:** run `03` twice for the same month. The second run logs `rows_inserted = 0` and `rows_updated = 0`, and `99_validation` shows no duplicate keys.

**Demonstrating schema drift:** upload a copy of one CSV with an extra column added to `landing/trips/209901/`, then run `02` with `start_month=end_month=209901`, once with `evolve_schema=false` (extra data captured as JSON) and once with `true` (column added). Both runs finish with `SUCCESS` and the log message reports `SCHEMA DRIFT`.

**Local tests:** `pip install pyspark pytest` (Java 17+ required), then `pytest -q tests`.
