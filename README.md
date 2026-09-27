# Citi Bike Medallion Pipeline

Semester project — end-to-end Spark data pipeline (Bronze → Silver → Gold) for NYC Citi Bike trip data, culminating in a Power BI dashboard.

## Team

- [Name 1]
- [Name 2]

## Project Summary

This project builds an automated Medallion Architecture pipeline on Apache Spark (Databricks Community Edition) using NYC Citi Bike's public trip-history data. See `docs/Phase1_Project_Proposal.docx` for the full Phase 1 proposal, covering domain selection, data volume estimates, PII analysis, the Bronze/Silver/Gold modeling plan, and the BI dashboard design.

## Repository Structure

```
├── data/
│   ├── samples/        # Trimmed sample files (full-load + incremental) committed to the repo
│   └── raw/             # Full downloaded files (gitignored -- not committed)
├── notebooks/           # Databricks / Jupyter notebooks for Bronze, Silver, Gold processing
├── scripts/
│   └── fetch_samples.py # Downloads real data from Citi Bike's S3 bucket and generates samples
├── docs/                # Project proposal and supporting documentation
└── README.md
```

## Data Source

- **Full load:** Citi Bike monthly trip data, January 2024 – December 2024
  (`https://s3.amazonaws.com/tripdata/YYYYMM-citibike-tripdata.csv.zip`)
- **Incremental load:** January 2025 trip data (same source, next published month)

## Getting the Data

The full raw files are too large to commit to GitHub, so only trimmed samples live in `data/samples/`. To pull the real files for local development:

```bash
pip install requests
python scripts/fetch_samples.py
```

This downloads the full-load and incremental-load months into `data/raw/` (gitignored) and regenerates the trimmed samples in `data/samples/`.

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
