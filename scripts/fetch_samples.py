"""
Fetch Citi Bike full-load and incremental-load sample data for the repo.

Run this on your own machine (not inside a restricted sandbox) since it needs
access to s3.amazonaws.com.

What it does:
  1. Downloads the full-load baseline months (default: Jan 2024 - Dec 2024)
     and the incremental-load month (default: Jan 2025) from Citi Bike's
     public S3 bucket.
  2. Unzips each file.
  3. Writes a trimmed sample (first N rows, default 50,000) of the FIRST
     full-load month and of the incremental month into data/samples/,
     since GitHub blocks files over 100 MB and full monthly files can be
     several hundred MB uncompressed.
  4. Leaves the full unzipped CSVs in data/raw/ (gitignored) for your local
     Spark pipeline to actually read from -- these are NOT meant to be
     committed to GitHub.

Usage:
    pip install requests
    python scripts/fetch_samples.py
"""

import io
import os
import zipfile
import requests

BASE_URL = "https://s3.amazonaws.com/tripdata"

FULL_LOAD_MONTHS = [f"2024{m:02d}" for m in range(1, 13)]   # Jan-Dec 2024
INCREMENTAL_MONTH = "202501"                                 # Jan 2025

RAW_DIR = "data/raw"
SAMPLE_DIR = "data/samples"
SAMPLE_ROWS = 50_000


def download_and_extract(yyyymm, dest_dir):
    fname = f"{yyyymm}-citibike-tripdata.csv.zip"
    url = f"{BASE_URL}/{fname}"
    print(f"Downloading {url} ...")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        os.makedirs(dest_dir, exist_ok=True)
        zf.extractall(dest_dir)
        extracted = [n for n in zf.namelist() if n.endswith(".csv")]
    print(f"  Extracted: {extracted}")
    return [os.path.join(dest_dir, n) for n in extracted]


def write_sample(csv_path, out_path, n_rows=SAMPLE_ROWS):
    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f_in:
        header = f_in.readline()
        lines = [f_in.readline() for _ in range(n_rows)]
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f_out:
        f_out.write(header)
        f_out.writelines(l for l in lines if l)
    print(f"  Sample written: {out_path}")


def main():
    print("== Full load (baseline) ==")
    first_month_csvs = None
    for i, yyyymm in enumerate(FULL_LOAD_MONTHS):
        csvs = download_and_extract(yyyymm, os.path.join(RAW_DIR, "full_load", yyyymm))
        if i == 0:
            first_month_csvs = csvs

    if first_month_csvs:
        write_sample(
            first_month_csvs[0],
            os.path.join(SAMPLE_DIR, f"full_load_sample_{FULL_LOAD_MONTHS[0]}.csv"),
        )

    print("\n== Incremental load ==")
    inc_csvs = download_and_extract(INCREMENTAL_MONTH, os.path.join(RAW_DIR, "incremental", INCREMENTAL_MONTH))
    if inc_csvs:
        write_sample(
            inc_csvs[0],
            os.path.join(SAMPLE_DIR, f"incremental_load_sample_{INCREMENTAL_MONTH}.csv"),
        )

    print("\nDone. Full raw files are in data/raw/ (gitignored).")
    print("Trimmed samples for the repo are in data/samples/.")


if __name__ == "__main__":
    main()
