import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def parse_timestamp(ts_string: str) -> datetime | None:
    if not ts_string:
        return None
    try:
        return datetime.fromisoformat(ts_string.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def transform_device(raw: dict) -> tuple | None:
    """
    Transform one raw inventory document from OpenSearch into a clean tuple.

    Input — flat dict already extracted by ingest_inventory.py:
   {
    "device_name":       "DEV_DEMO01",
    "collection_date":   "2026-01-01",
    "ip_address":        "10.0.0.1",
    "online_state":      true,
    "hardware_type":     "DEMO-HW-TYPE",
    "software_version":  "V1.0.0.000",
    "last_seen":         "2026-01-01T00:00:00.000+00:00",
    "device_type":       "DEMO-HW-TYPE-1.0",
    "collection_status": "SUCCESS",
    "onu_count":         10
}

    Output tuple matches inventory table column order:
        (device_name, collection_date, ip_address, online_state,
         hardware_type, software_version, device_type,
         collection_status, last_seen, onu_count)
    """
    device_name = raw.get("device_name")
    if not device_name:
        logger.warning("Skipping inventory doc with no device_name")
        return None

    collection_date = raw.get("collection_date")
    if not collection_date:
        logger.warning(f"Skipping {device_name} — no collection_date")
        return None

    last_seen = parse_timestamp(raw.get("last_seen"))

    return (
        device_name,
        collection_date,
        raw.get("ip_address"),
        raw.get("online_state", False),
        raw.get("hardware_type"),
        raw.get("software_version"),
        raw.get("device_type"),
        raw.get("collection_status"),
        last_seen,
        raw.get("onu_count", 0),
    )


def transform_all_inventory(raw_list: list) -> list:
    results = []
    skipped = 0

    for raw in raw_list:
        row = transform_device(raw)
        if row is None:
            skipped += 1
            continue
        results.append(row)

    logger.info(f"Transformed {len(results)} inventory rows, skipped {skipped}")
    return results
