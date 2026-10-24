"""
Cut a small sample (default 50,000 rows) from every month you already downloaded with fetch_samples.py,
so a multi-month BACKFILL can be demonstrated in Databricks without uploading gigabytes.

Output goes to data/raw/backfill_samples/<YYYYMM>/backfill_sample_<YYYYMM>.csv  (inside data/raw, so it is gitignored).
Months that already have a committed sample (the first full-load month and the incremental month) are skipped.

Usage (from the repo root):
    python scripts/make_backfill_samples.py
"""

import glob
import json
import os

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(REPO_ROOT, "data", "raw")
SAMPLES_DIR = os.path.join(REPO_ROOT, "data", "samples")
MANIFEST = os.path.join(REPO_ROOT, "data", "manifest.json")
OUT_DIR = os.path.join(RAW_DIR, "backfill_samples")
SAMPLE_ROWS = 50_000


def already_sampled_months():
    months = set()
    for path in glob.glob(os.path.join(SAMPLES_DIR, "*_load_sample_*.csv")):
        months.add(os.path.basename(path).rsplit("_", 1)[1].replace(".csv", ""))
    return months


def first_csv_for(month):
    for kind in ("full_load", "incremental"):
        parts = sorted(glob.glob(os.path.join(RAW_DIR, kind, month, "*.csv")))
        if parts:
            return parts[0]
    return None


def main():
    with open(MANIFEST) as f:
        months = json.load(f)["baseline_months"]
    skip = already_sampled_months()
    written = []
    for month in sorted(months):
        if month in skip:
            continue
        src = first_csv_for(month)
        if not src:
            print(f"{month}: raw files not found, skipped")
            continue
        out_folder = os.path.join(OUT_DIR, month)
        os.makedirs(out_folder, exist_ok=True)
        out = os.path.join(out_folder, f"backfill_sample_{month}.csv")
        with open(src, "r", encoding="utf-8", errors="ignore") as fin, open(out, "w", encoding="utf-8", newline="") as fout:
            fout.write(fin.readline())
            for _ in range(SAMPLE_ROWS):
                line = fin.readline()
                if not line:
                    break
                fout.write(line)
        written.append(out)
        print(f"{month}: {out}  ({os.path.getsize(out) / 1e6:.1f} MB)")
    print(f"\nDone. {len(written)} sample file(s) created in {OUT_DIR}")
    print("Upload each one into Databricks: Catalog > workspace > citibike_raw > landing > trips > <YYYYMM> (create the folder).")


if __name__ == "__main__":
    main()
