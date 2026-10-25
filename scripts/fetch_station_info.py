"""
Download Citi Bike's GBFS station_information feed and save it as a dated CSV snapshot.

Why: the live station_status feed (poll_gbfs.py) identifies stations by a UUID / 19-digit id, while the monthly
trip files use short ids such as "5989.02". station_information lists both ids (station_id + short_name) plus dock
capacity, so the pipeline can link live availability to the same stations as the trips.

Station details change rarely, so run this about once a week (or before a pipeline run):
    pip install requests
    python scripts/fetch_station_info.py

Output: data/station_info/station_information_<YYYYMMDD>.csv
Upload it to the Databricks volume folder  landing/station_info/  and run notebook 06 (or the full pipeline).
"""
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

GBFS_DISCOVERY_URL = "https://gbfs.citibikenyc.com/gbfs/gbfs.json"
OUT_DIR = Path(__file__).resolve().parent.parent / "data" / "station_info"
COLUMNS = ["snapshot_timestamp_utc", "station_id", "short_name", "name", "lat", "lon", "capacity", "region_id"]


def feed_url(name: str) -> str:
    """Resolve a feed URL from the GBFS discovery file instead of hard-coding it."""
    discovery = requests.get(GBFS_DISCOVERY_URL, timeout=30)
    discovery.raise_for_status()
    data = discovery.json()["data"]
    language = data.get("en") or next(iter(data.values()))
    for feed in language["feeds"]:
        if feed["name"] == name:
            return feed["url"]
    raise RuntimeError(f"Feed '{name}' not listed in {GBFS_DISCOVERY_URL}")


def main() -> int:
    url = feed_url("station_information")
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    stations = response.json()["data"]["stations"]

    now = datetime.now(timezone.utc)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_file = OUT_DIR / f"station_information_{now:%Y%m%d}.csv"

    with out_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        for s in stations:
            writer.writerow({
                "snapshot_timestamp_utc": now.isoformat(),
                "station_id": s.get("station_id"),
                "short_name": s.get("short_name"),
                "name": s.get("name"),
                "lat": s.get("lat"),
                "lon": s.get("lon"),
                "capacity": s.get("capacity"),
                "region_id": s.get("region_id"),
            })

    with_short = sum(1 for s in stations if s.get("short_name"))
    print(f"Saved {len(stations)} stations ({with_short} with a trip-style short_name) to {out_file}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
