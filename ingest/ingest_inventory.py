"""
Standalone ingest script — run once.
Reads all inventory files from the elasticsearch_data folder
and writes one document per file into OpenSearch inventory index.

File naming pattern:
    inv_ls-df-cfxr-h-24.12_ls-df_1.0.0_2026-04-06_12-00
    (no .json extension, date is at position -2 and -1 when split by _)
"""
import os
import json
import logging
import requests
import urllib3
from confi import *

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

INVENTORY_INDEX = "inventory-daily"
BATCH_SIZE      = 50


def get_session() -> requests.Session:
    session        = requests.Session()
    session.auth   = (OPENSEARCH_USER, OPENSEARCH_PASSWORD)
    session.verify = False
    return session


def create_index(session: requests.Session):
    url      = f"{OPENSEARCH_HOST}/{INVENTORY_INDEX}"
    response = session.put(url, json={
        "settings": {"number_of_shards": 1, "number_of_replicas": 0}
    })
    if response.status_code in (200, 400):
        logger.info(f"Index '{INVENTORY_INDEX}' ready")
    else:
        logger.error(f"Could not create index: {response.status_code} {response.text[:200]}")


def parse_date_from_filename(filename: str) -> str | None:
    """
    Extract date from filename pattern:
        inv_ls-df-cfxr-h-24.12_ls-df_1.0.0_2026-04-06_12-00
    Date is the second-to-last segment when split by "_":
        [..., "2026-04-06", "12-00"]
    Returns "2026-04-06" or None if pattern doesn't match.
    """
    try:
        parts = filename.replace(".json", "").split("_")
        for part in reversed(parts):
            segments = part.split("-")
            if len(segments) == 3 and len(segments[0]) == 4:
                return part
    except Exception:
        pass
    return None


def parse_inventory_file(filepath: str, filename: str) -> list:
    """Returns a list of dicts, one per device."""
    collection_date = parse_date_from_filename(filename)
    if not collection_date:
        logger.warning(f"Could not parse date from filename: {filename}")
        return []

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Failed to read {filepath}: {e}")
        return []

    if not isinstance(data, list):
        data = [data]

    results = []
    for item in data:
        av_meta  = item.get("deviceAVmetadata", {})
        inv_meta = item.get("inventorymetadata", {})

        device_name = av_meta.get("device-id")
        if not device_name:
            continue

        inv_data   = item.get("inventorydata", {})
        onus_block = inv_data.get("bbf-fiber-onu-emulated-mount:onus", {})
        onu_list   = onus_block.get("onu", [])

        results.append({
            "device_name":       device_name,
            "collection_date":   collection_date,
            "ip_address":        av_meta.get("ip-address"),
            "online_state":      av_meta.get("reachable", "").lower() == "up",
            "hardware_type":     av_meta.get("hardware-type"),
            "software_version":  av_meta.get("active-software"),
            "last_seen":         av_meta.get("reachable-last-change"),
            "device_type":       inv_meta.get("type"),
            "collection_status": inv_meta.get("collectionstatus"),
            "onu_count":         len(onu_list),
        })

    return results


def send_bulk(session: requests.Session, docs: list) -> int:
    url  = f"{OPENSEARCH_HOST}/{INVENTORY_INDEX}/_bulk"
    body = ""
    for doc in docs:
        doc_id = f"{doc['device_name']}_{doc['collection_date']}"
        body  += json.dumps({"index": {"_id": doc_id}}) + "\n"
        body  += json.dumps(doc) + "\n"

    try:
        response = session.post(
            url,
            data=body,
            headers={"Content-Type": "application/x-ndjson"},
            timeout=30
        )
        result = response.json()
        if result.get("errors"):
            failed = sum(
                1 for item in result.get("items", [])
                if item.get("index", {}).get("error")
            )
            return len(docs) - failed
        return len(docs)
    except Exception as e:
        logger.error(f"Bulk request failed: {e}")
        return 0

def is_inventory_file(filename: str) -> bool:
    return filename.startswith("inv_")


def run():
    session = get_session()
    create_index(session)

    if not os.path.isdir(INVENTORY_DIR):
        logger.error(f"Directory not found: {INVENTORY_DIR}")
        return

    files = [f for f in os.listdir(INVENTORY_DIR) if is_inventory_file(f)]
    logger.info(f"Found {len(files)} inventory files")

    docs = []
    for filename in files:
        filepath = os.path.join(INVENTORY_DIR, filename)
        device_docs = parse_inventory_file(filepath, filename)
        docs.extend(device_docs)
    if not docs:
        logger.warning("No valid inventory documents to ingest")
        return

    logger.info(f"Ingesting {len(docs)} inventory documents")

    total = 0
    for i in range(0, len(docs), BATCH_SIZE):
        batch  = docs[i:i + BATCH_SIZE]
        sent   = send_bulk(session, batch)
        total += sent

    logger.info(f"Done. Total inventory documents sent: {total}")


if __name__ == "__main__":
    run()