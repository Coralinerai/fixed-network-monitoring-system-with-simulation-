"""
Standalone ingest script — run once.
Reads all metric JSON files and writes datapoints into OpenTSDB.
"""
import os
import json
import logging
import time
import requests
from confi import OPENTSDB_HOST, METRICS_DIR

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BATCH_SIZE = 10


def wait_for_opentsdb():
    """Block until OpenTSDB is stable — responds 3 times in a row."""
    logger.info("Waiting for OpenTSDB to be ready...")
    consecutive = 0
    while consecutive < 3:
        try:
            response = requests.get(f"{OPENTSDB_HOST}/version", timeout=5)
            if response.status_code == 200:
                consecutive += 1
                logger.info("OpenTSDB responding (%d/3)...", consecutive)
                time.sleep(10)
            else:
                consecutive = 0
                time.sleep(15)
        except requests.RequestException:
            consecutive = 0
            logger.info("OpenTSDB not ready yet, retrying in 15s...")
            time.sleep(15)
    logger.info("OpenTSDB is stable and ready")


def build_payloads(metric_obj: dict) -> list:
    metric = metric_obj.get("metric", "")
    tags   = metric_obj.get("tags", {})
    dps    = metric_obj.get("dps", {})

    if not metric or not dps:
        return []

    if not tags:
        logger.warning("Skipping metric %s — no tags", metric)
        return []

    return [
        {
            "metric":    metric,
            "timestamp": int(ts),
            "value":     value,
            "tags":      tags,
        }
        for ts, value in dps.items()
    ]


def send_batch(batch: list, retries: int = 3) -> bool:
    for attempt in range(retries):
        try:
            payload  = json.dumps(batch)
            response = requests.post(
                f"{OPENTSDB_HOST}/api/put",
                data=payload,
                headers={
                    "Content-Type":   "application/json",
                    "Content-Length": str(len(payload)),
                    "Connection":     "close"
                },
                timeout=60
            )
            if response.status_code in (200, 204):
                return True
            logger.warning(
                "Attempt %d — status %d: %s",
                attempt + 1,
                response.status_code,
                response.text[:200]
            )
        except requests.RequestException as e:
            logger.warning("Attempt %d — request failed: %s", attempt + 1, e)
        time.sleep(5 * (attempt + 1))
    logger.error("Batch failed after %d attempts", retries)
    return False


def ingest_file(filepath: str) -> int:
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.error("Failed to read %s: %s", filepath, e)
        return 0

    if isinstance(data, dict):
        data = [data]

    payloads = []
    for obj in data:
        payloads.extend(build_payloads(obj))

    sent = 0
    for i in range(0, len(payloads), BATCH_SIZE):
        batch = payloads[i:i + BATCH_SIZE]
        if send_batch(batch):
            sent += len(batch)

    return sent


def run():
    wait_for_opentsdb()

    if not os.path.isdir(METRICS_DIR):
        logger.error("Directory not found: %s", METRICS_DIR)
        return

    files = [f for f in os.listdir(METRICS_DIR) if f.endswith(".json")]
    logger.info("Found %d metric files", len(files))

    total = 0
    for filename in files:
        sent = ingest_file(os.path.join(METRICS_DIR, filename))
        logger.info("%s: %d datapoints sent", filename, sent)
        total += sent
        time.sleep(2)  # give HBase time to breathe between files

    logger.info("Done. Total datapoints sent to OpenTSDB: %d", total)


if __name__ == "__main__":
    run()