"""
Standalone ingest script — run once.
Reads alarms-active.json and writes all documents into OpenSearch.
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

BATCH_SIZE = 100


def get_session() -> requests.Session:
    session        = requests.Session()
    session.auth   = (OPENSEARCH_USER, OPENSEARCH_PASSWORD)
    session.verify = False
    return session


def create_index(session: requests.Session):
    url      = f"{OPENSEARCH_HOST}/{OPENSEARCH_ALARMS_INDEX}"
    response = session.put(url, json={
        "settings": {"number_of_shards": 1, "number_of_replicas": 0}
    })
    if response.status_code in (200, 400):
        logger.info(f"Index '{OPENSEARCH_ALARMS_INDEX}' ready")
    else:
        logger.error(f"Could not create index: {response.status_code} {response.text[:200]}")


def send_bulk(session: requests.Session, docs: list) -> int:
    url  = f"{OPENSEARCH_HOST}/{OPENSEARCH_ALARMS_INDEX}/_bulk"
    body = ""
    for doc in docs:
        doc_id = doc.get("objectId", "")
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
            logger.warning(f"{failed} documents failed in this batch")
            return len(docs) - failed
        return len(docs)
    except Exception as e:
        logger.error(f"Bulk request failed: {e}")
        return 0


def load_file() -> list:
    if not os.path.isfile(ALARMS_FILE):
        logger.error(f"File not found: {ALARMS_FILE}")
        return []

    with open(ALARMS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        hits = data.get("hits", {}).get("hits", [])
        if hits:
            return [hit.get("_source", hit) for hit in hits]

    return []


def run():
    session = get_session()
    create_index(session)

    docs = load_file()
    if not docs:
        logger.warning("No documents to ingest")
        return

    logger.info(f"Ingesting {len(docs)} alarm documents into OpenSearch")

    total = 0
    for i in range(0, len(docs), BATCH_SIZE):
        batch  = docs[i:i + BATCH_SIZE]
        sent   = send_bulk(session, batch)
        total += sent
        logger.info(f"Batch {i // BATCH_SIZE + 1}: {sent}/{len(batch)} sent")

    logger.info(f"Done. Total documents sent to OpenSearch: {total}")


if __name__ == "__main__":
    run()
