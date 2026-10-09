"""
Fetch Citi Bike full-load and incremental-load data for the repo.

Run this on your own machine (needs access to s3.amazonaws.com), not inside a
restricted sandbox or CI runner that blocks outbound traffic to S3.

Design notes (fixes from TA review):
  - Citi Bike's monthly files have used more than one naming convention over
    the years (some months are "<yyyymm>-citibike-tripdata.csv.zip", others
    are "<yyyymm>-citibike-tripdata.zip"). Instead of hardcoding one pattern,
    this script tries a list of candidate filenames per month and uses
    whichever one actually exists.
  - Months with more than ~1 million trips are split into multiple CSVs
    inside a single zip. This script concatenates all CSVs found in a
    month's zip rather than assuming exactly one.
  - The FULL LOAD baseline is the most recent N complete months as of
    whenever you run this script (default N=6), not a fixed past year --
    so "full load" always means "recent history," never a replay of 2024.
  - A manifest file (data/manifest.json) records which months have already
    been pulled into the baseline. Re-running this script later will only
    download the next month NOT already in the manifest and will file it as
    INCREMENTAL, which is what makes the incremental load genuinely new
    data rather than a relabeled historical month.

Usage:
    pip install requests
    python scripts/fetch_samples.py
"""

import io
import json
import os
import zipfile
from datetime import date

import requests

BASE_URL = "https://s3.amazonaws.com/tripdata"
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(REPO_ROOT, "data", "raw")
SAMPLE_DIR = os.path.join(REPO_ROOT, "data", "samples")
MANIFEST_PATH = os.path.join(REPO_ROOT, "data", "manifest.json")
SAMPLE_ROWS = 50_000
BASELINE_MONTHS = 6  # per TA guidance: start smaller to stay within free-tier limits


def candidate_filenames(yyyymm):
    """Return the filename patterns to try, in order, for a given month."""
    return [
        f"{yyyymm}-citibike-tripdata.csv.zip",
        f"{yyyymm}-citibike-tripdata.zip",
    ]


def try_download(yyyymm):
    """Try each candidate filename for this month; return (bytes, filename) or (None, None)."""
    for fname in candidate_filenames(yyyymm):
        url = f"{BASE_URL}/{fname}"
        try:
            resp = requests.get(url, timeout=120)
        except requests.RequestException:
            continue
        if resp.status_code == 200 and resp.content[:2] == b"PK":  # valid zip signature
            print(f"  Found: {url}")
            return resp.content, fname
        print(f"  Not found (status {resp.status_code}): {url}")
    return None, None


def extract_all_csvs(zip_bytes, dest_dir):
    """Extract every CSV in the zip (handles multi-part months) into dest_dir."""
    os.makedirs(dest_dir, exist_ok=True)
    extracted = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for name in zf.namelist():
            if name.lower().endswith(".csv") and "__MACOSX" not in name and not os.path.basename(name).startswith("."):
                zf.extract(name, dest_dir)
                extracted.append(os.path.join(dest_dir, name))
    return extracted


def write_sample(csv_path, out_path, n_rows=SAMPLE_ROWS):
    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f_in:
        header = f_in.readline()
        lines = [f_in.readline() for _ in range(n_rows)]
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f_out:
        f_out.write(header)
        f_out.writelines(l for l in lines if l)
    print(f"  Sample written: {out_path}")


def load_manifest():
    if os.path.exists(MANIFEST_PATH):
        with open(MANIFEST_PATH) as f:
            return json.load(f)
    return {"baseline_months": []}


def save_manifest(manifest):
    os.makedirs(os.path.dirname(MANIFEST_PATH), exist_ok=True)
    with open(MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, indent=2)


def find_available_recent_months(count):
    """Search backwards from last month until `count` real months are found."""
    today = date.today()
    y, m = today.year, today.month - 1  # last month is usually the newest published
    if m == 0:
        y, m = y - 1, 12

    found = []
    probe_y, probe_m = y, m
    attempts = 0
    while len(found) < count and attempts < count + 6:  # small buffer for gaps
        yyyymm = f"{probe_y}{probe_m:02d}"
        print(f"Checking {yyyymm} ...")
        content, fname = try_download(yyyymm)
        if content:
            found.append((yyyymm, content, fname))
        probe_m -= 1
        if probe_m == 0:
            probe_m, probe_y = 12, probe_y - 1
        attempts += 1
    return found  # newest first


def main():
    manifest = load_manifest()
    baseline_set = set(manifest["baseline_months"])

    if not baseline_set:
        print(f"== No baseline yet. Pulling {BASELINE_MONTHS + 1} most recent months ==")
        print(f"   (oldest {BASELINE_MONTHS} = full-load baseline, newest 1 = first incremental batch)")
        months = find_available_recent_months(BASELINE_MONTHS + 1)
        months = list(reversed(months))  # oldest first

        if len(months) < BASELINE_MONTHS + 1:
            print(f"\nOnly found {len(months)} months. Check your internet connection and rerun.")
            return

        baseline, incremental = months[:-1], months[-1]

        first_sample_written = False
        for yyyymm, content, fname in baseline:
            csvs = extract_all_csvs(content, os.path.join(RAW_DIR, "full_load", yyyymm))
            print(f"  {yyyymm}: {len(csvs)} CSV part(s) extracted (full load)")
            manifest["baseline_months"].append(yyyymm)
            if not first_sample_written and csvs:
                write_sample(csvs[0], os.path.join(SAMPLE_DIR, f"full_load_sample_{yyyymm}.csv"))
                first_sample_written = True

        yyyymm, content, fname = incremental
        csvs = extract_all_csvs(content, os.path.join(RAW_DIR, "incremental", yyyymm))
        print(f"  {yyyymm}: {len(csvs)} CSV part(s) extracted (incremental)")
        if csvs:
            write_sample(csvs[0], os.path.join(SAMPLE_DIR, f"incremental_load_sample_{yyyymm}.csv"))
        manifest["baseline_months"].append(yyyymm)

        save_manifest(manifest)
        print(f"\nIngested months: {manifest['baseline_months']}")

    else:
        print(f"== Baseline already set: {manifest['baseline_months']} ==")
        print("== Checking for a new month to ingest as INCREMENTAL ==")
        latest_baseline = max(manifest["baseline_months"])
        y, m = int(latest_baseline[:4]), int(latest_baseline[4:])
        m += 1
        if m == 13:
            m, y = 1, y + 1
        candidate = f"{y}{m:02d}"

        content, fname = try_download(candidate)
        if content:
            csvs = extract_all_csvs(content, os.path.join(RAW_DIR, "incremental", candidate))
            print(f"  {candidate}: {len(csvs)} CSV part(s) extracted -- NEW incremental month")
            if csvs:
                write_sample(csvs[0], os.path.join(SAMPLE_DIR, f"incremental_load_sample_{candidate}.csv"))
            manifest["baseline_months"].append(candidate)
            save_manifest(manifest)
        else:
            print(f"  {candidate} is not published yet. Nothing new to ingest -- try again later.")

    print("\nDone. Full raw files are in data/raw/ (gitignored).")
    print("Trimmed samples for the repo are in data/samples/.")


if __name__ == "__main__":
    main()
