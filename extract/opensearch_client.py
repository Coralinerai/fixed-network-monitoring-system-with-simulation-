import requests
import logging
import urllib3
from datetime import datetime, timedelta, timezone
from config import OPENSEARCH_HOST, OPENSEARCH_USER, OPENSEARCH_PASSWORD, OPENSEARCH_ALARMS_INDEX, INVENTORY_INDEX

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)


def get_session() -> requests.Session:
    session        = requests.Session()
    session.auth   = (OPENSEARCH_USER, OPENSEARCH_PASSWORD)
    session.verify = False
    return session


def build_range_query(timestamp_field: str, start: datetime, end: datetime) -> dict:
    """Build an OpenSearch range query between two datetimes."""
    return {
        "query": {
            "range": {
                timestamp_field: {
                    "gte": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "lte": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
            }
        }
    }


def build_date_query(timestamp_field: str, date: str) -> dict:
    """Build an OpenSearch query for a specific date (YYYY-MM-DD)."""
    start = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end   = start + timedelta(days=1) - timedelta(seconds=1)
    return build_range_query(timestamp_field, start, end)


def paginate(index: str, query: dict = None) -> list:
    """
    Fetch documents from an index using pagination.

    Args:
        index : OpenSearch index name
        query : optional range query to filter by time window.
                if None, fetches all documents.
    """
    session  = get_session()
    all_docs = []
    size     = 1000
    from_    = 0

    while True:
        url    = f"{OPENSEARCH_HOST}/{index}/_search"
        params = {"size": size, "from": from_}

        try:
            response = session.get(url, params=params, json=query, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch from {index}: {e}")
            break

        hits = response.json().get("hits", {}).get("hits", [])

        for hit in hits:
            doc           = hit.get("_source", {})
            doc["_es_id"] = hit.get("_id")
            all_docs.append(doc)

        if len(hits) < size:
            break

        from_ += size

    logger.info(f"Fetched {len(all_docs)} documents from {index}")
    return all_docs

def fetch_active_alarms(window_start: datetime = None, window_end: datetime = None) -> list:
    if window_start and window_end:
        query = {
            "query": {
                "range": {
                    "raisedTime": {
                        "gte": window_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "lte": window_end.strftime("%Y-%m-%dT%H:%M:%SZ")
                    }
                }
            }
        }
        return paginate(OPENSEARCH_ALARMS_INDEX, query=query)
    return paginate(OPENSEARCH_ALARMS_INDEX)

def fetch_inventory(sim_date: str = None) -> list:
    """
    Fetch inventory documents.

    Simulation mode: pass sim_date (YYYY-MM-DD) to filter by collection_date.
    Live mode      : pass sim_date = today's date.
    No args        : fetches all documents.
    """
    if sim_date:
        query = build_date_query("collection_date", sim_date)
        return paginate(INVENTORY_INDEX, query=query)
    return paginate(INVENTORY_INDEX)