# Citi Bike Medallion Pipeline

Semester project — end-to-end Spark data pipeline (Bronze → Silver → Gold) for NYC Citi Bike trip data, culminating in a Power BI dashboard.

## Team

- Muhammad Abdullah 22L7545
- Shaheer Asif 22L6396

## Project Summary

This project builds an automated Medallion Architecture pipeline on Apache Spark (Databricks Community Edition) using NYC Citi Bike's public trip-history data. See `docs/Phase1_Project_Proposal.docx` for the full Phase 1 proposal, covering domain selection, data volume estimates, PII analysis, the Bronze/Silver/Gold modeling plan, and the BI dashboard design.

## Repository Structure

```
├── data/
│   ├── samples/          # Trimmed sample files (full-load + incremental + GBFS) committed to the repo
│   ├── raw/              # Full downloaded trip-history files (gitignored -- not committed)
│   ├── gbfs/             # Accumulated hourly GBFS station-status snapshots (gitignored -- not committed)
│   └── manifest.json     # Tracks which months are already in the baseline, so re-runs fetch new months only
├── notebooks/            # Databricks / Jupyter notebooks for Bronze, Silver, Gold processing
├── scripts/
│   ├── fetch_samples.py  # Downloads recent monthly trip data from Citi Bike's S3 bucket; builds baseline + incremental
│   └── poll_gbfs.py      # Polls the live GBFS station-status feed hourly and appends a snapshot
├── docs/                 # Project proposal and supporting documentation
└── README.md
```

## Data Sources

- **Full load:** the most recent 6 complete months of Citi Bike trip data at the time the pipeline is first run (auto-detected by `fetch_samples.py` — never a fixed past year, so the baseline is always "recent history")
  (`https://s3.amazonaws.com/tripdata/<YYYYMM>-citibike-tripdata.csv.zip` or `.zip`, depending on which the source uses for that month)
- **Incremental load:** whichever new month Citi Bike publishes after the baseline was set. `fetch_samples.py` tracks this via `data/manifest.json` and only pulls the next unseen month, so re-running it monthly ingests genuinely new data rather than replaying old months.
- **Live incremental stream:** Citi Bike's GBFS `station_status` feed, polled hourly by `scripts/poll_gbfs.py`, giving real time-varying data (bikes/docks available per station) independent of the monthly trip-history release cycle.

## Getting the Data

The full raw files are too large to commit to GitHub, so only trimmed samples live in `data/samples/`.

**Monthly trip data (full load, then incremental on each re-run):**
```bash
pip install requests
python scripts/fetch_samples.py
```
First run: downloads the 6 most recent complete months into `data/raw/full_load/` and records them in `data/manifest.json`.
Later runs (e.g., once a month): downloads only the next newly published month into `data/raw/incremental/` and updates the manifest.

**Live GBFS station status (run hourly):**
```bash
python scripts/poll_gbfs.py
```
Appends one row per station to `data/gbfs/station_status_history.csv`. See the scheduling notes at the bottom of that file for setting it up as an hourly Windows Task Scheduler job or cron job so it accumulates real data over several days.

## Pipeline Layers

| Layer | Purpose |
|---|---|
| Bronze | Raw trip records as ingested, partitioned by year/month, with ingestion metadata |
| Silver | Cleansed, typed, deduplicated `silver_trips` fact table + `silver_stations` reference table |
| Gold | Star schema (`fact_trips`, `dim_station`, `dim_date`, `dim_time`, `dim_rider_type`) and aggregate tables for BI |

## Dashboard

Built in Power BI on top of the Gold layer. See `docs/Phase1_Project_Proposal.docx`, Section 5, for the full dashboard outline and business questions.

## Infrastructure & Cost Management

Developed on Databricks Community Edition, with Azure for Students as a fallback. See `docs/Phase1_Project_Proposal.docx`, Section 6, for the FinOps plan.

## Hourly GBFS polling (Windows)

1. Find your Python path: `where python`
2. Register the hourly task (replace both paths with your own):

```
schtasks /create /sc hourly /tn "CitiBikeGBFSPoll" /tr "\"C:\path\to\python.exe\" \"C:\path\to\repo\scripts\poll_gbfs.py\"" /st 00:00
```

3. Check it exists: `schtasks /query /tn "CitiBikeGBFSPoll"`
4. Remove it when no longer needed: `schtasks /delete /tn "CitiBikeGBFSPoll" /f`

The laptop must be powered on and awake for the task to run; polls missed while asleep are simply skipped.
