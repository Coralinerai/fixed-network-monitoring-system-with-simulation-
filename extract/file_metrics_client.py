"""
File-based metrics client — reads directly from JSON files.
Replaces OpenTSDB client for the simulation repo.

Filters datapoints by time window so the simulation DAG
can load data hour by hour, day by day.
"""
import os
import json
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def fetch_all_metrics(metrics_dir: str, window_start: datetime = None, window_end: datetime = None) -> list:
    """
    Read all metric JSON files and return raw metric objects
    filtered to the given time window.

    Each JSON file contains a list of metric objects (one per component/device).
    Each object has a 'dps' dict of {timestamp: value} pairs.

    Simulation mode: pass window_start and window_end to get only that hour's data.
    No args        : returns all datapoints from all files.
    """
    if not os.path.isdir(metrics_dir):
        logger.error("Metrics directory not found: %s", metrics_dir)
        return []

    files = [f for f in os.listdir(metrics_dir) if f.endswith(".json")]
    logger.info("Reading %d metric files from %s", len(files), metrics_dir)

    ws_ts = int(window_start.timestamp()) if window_start else None
    we_ts = int(window_end.timestamp()) if window_end else None

    all_raw = []

    for filename in files:
        filepath = os.path.join(metrics_dir, filename)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            logger.error("Failed to read %s: %s", filepath, e)
            continue

        if isinstance(data, dict):
            data = [data]

        for obj in data:
            if not isinstance(obj, dict):
                continue

            dps = obj.get("dps", {})

            if ws_ts and we_ts:
                # filter datapoints to the time window
                filtered_dps = {
                    ts: val for ts, val in dps.items()
                    if ws_ts <= int(ts) < we_ts
                }
                if filtered_dps:
                    all_raw.append({**obj, "dps": filtered_dps})
            else:
                if dps:
                    all_raw.append(obj)

    logger.info("Fetched %d metric series for window %s → %s",
                len(all_raw),
                window_start.strftime("%Y-%m-%d %H:%M") if window_start else "all",
                window_end.strftime("%Y-%m-%d %H:%M") if window_end else "all")

    return all_raw