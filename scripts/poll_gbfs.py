"""
Poll Citi Bike's live GBFS station_status feed and append a timestamped
snapshot to a running CSV. This is the project's live, real-time data source
that genuinely changes hour to hour, as distinct from the monthly trip-history
full/incremental loads.

Run this once per hour on a schedule (see scheduling notes at the bottom of
this file). Each run appends one row per station to
data/gbfs/station_status_history.csv, so over a few days of runs you build a
real time-series of bike/dock availability across the system.

Usage:
    pip install requests
    python scripts/poll_gbfs.py
"""

import csv
import os
from datetime import datetime, timezone

import requests

GBFS_AUTODISCOVERY_URL = "https://gbfs.citibikenyc.com/gbfs/gbfs.json"
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(REPO_ROOT, "data", "gbfs")
OUT_FILE = os.path.join(OUT_DIR, "station_status_history.csv")
SAMPLE_FILE = os.path.join(REPO_ROOT, "data", "samples", "gbfs_station_status_sample.csv")

FIELDS = [
    "poll_timestamp_utc",
    "station_id",
    "num_bikes_available",
    "num_ebikes_available",
    "num_docks_available",
    "is_installed",
    "is_renting",
    "is_returning",
    "last_reported",
]


def resolve_station_status_url():
    """Use GBFS auto-discovery rather than a hardcoded path, since GBFS feed
    layouts (with/without a language subfolder) vary by version and provider."""
    resp = requests.get(GBFS_AUTODISCOVERY_URL, timeout=30)
    resp.raise_for_status()
    manifest = resp.json()

    feeds = manifest["data"].get("en", manifest["data"]).get("feeds", [])
    for feed in feeds:
        if feed.get("name") == "station_status":
            return feed["url"]
    raise RuntimeError("station_status feed not found in GBFS auto-discovery manifest")


def fetch_station_status():
    url = resolve_station_status_url()
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    return payload["data"]["stations"]


def append_snapshot(stations):
    os.makedirs(OUT_DIR, exist_ok=True)
    file_exists = os.path.exists(OUT_FILE)
    poll_ts = datetime.now(timezone.utc).isoformat()

    rows = []
    for s in stations:
        vehicle_types = s.get("vehicle_types_available", [])
        ebikes = sum(
            v.get("count", 0) for v in vehicle_types if "ebike" in v.get("vehicle_type_id", "").lower()
        )
        rows.append({
            "poll_timestamp_utc": poll_ts,
            "station_id": s.get("station_id"),
            "num_bikes_available": s.get("num_bikes_available"),
            "num_ebikes_available": ebikes,
            "num_docks_available": s.get("num_docks_available"),
            "is_installed": s.get("is_installed"),
            "is_renting": s.get("is_renting"),
            "is_returning": s.get("is_returning"),
            "last_reported": s.get("last_reported"),
        })

    with open(OUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)
    print(f"Appended {len(rows)} station rows at {poll_ts} -> {OUT_FILE}")

    # Write a small committed sample the first time only (first 200 stations of the first poll)
    if not os.path.exists(SAMPLE_FILE):
        os.makedirs(os.path.dirname(SAMPLE_FILE), exist_ok=True)
        with open(SAMPLE_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(rows[:200])
        print(f"Sample written -> {SAMPLE_FILE}")


def main():
    stations = fetch_station_status()
    append_snapshot(stations)


if __name__ == "__main__":
    main()

# ---------------------------------------------------------------------------
# Scheduling: see the README ("Hourly GBFS polling") for the exact Windows
# Task Scheduler command. Paths in this script are resolved relative to the
# repo, so it writes to the right place even when launched by Task Scheduler.
# ---------------------------------------------------------------------------
