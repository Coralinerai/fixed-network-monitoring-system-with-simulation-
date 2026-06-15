import requests
import logging
from datetime import datetime
from config import OPENTSDB_HOST

logger = logging.getLogger(__name__)


def fetch_all_metric_names() -> list:
    """Discover all metric names stored in OpenTSDB."""
    url = f"{OPENTSDB_HOST}/api/suggest"
    params = {
        "type": "metrics",
        "max":  100000,
    }
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error("Failed to fetch metric names: %s", e)
        return []


def fetch_metric(metric_name: str, window_start: datetime = None, window_end: datetime = None) -> list:
    """
    Fetch all series for a given metric across all tag combinations.

    Simulation mode: pass window_start and window_end to fetch a specific window.
    Live mode      : pass window_start = now - 15min, window_end = now.
    No args        : fetches all historical data.
    """
    url = f"{OPENTSDB_HOST}/api/query"

    if window_start and window_end:
        start = window_start.strftime("%Y/%m/%d-%H:%M:%S")
        end   = window_end.strftime("%Y/%m/%d-%H:%M:%S")
    else:
        start = "1y-ago"
        end   = None

    params = {
        "start": start,
        "m":     f"none:{metric_name}{{}}",
    }
    if end:
        params["end"] = end

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            return []
        return data
    except requests.RequestException as e:
        logger.warning("Failed to fetch metric %s: %s", metric_name, e)
        return []


def fetch_all_metrics(window_start: datetime = None, window_end: datetime = None) -> list:
    """
    Fetch all metrics for a given time window.

    Simulation mode: pass window_start and window_end.
    Live mode      : pass window_start = now - 15min, window_end = now.
    No args        : fetches all historical data.
    """
    metric_names = fetch_all_metric_names()
    if not metric_names:
        logger.error("No metrics found in OpenTSDB")
        return []

    logger.info("Discovered %d metrics in OpenTSDB", len(metric_names))

    all_results = []
    for metric_name in metric_names:
        results = fetch_metric(metric_name, window_start, window_end)
        if isinstance(results, list):
            all_results.extend(results)
        elif isinstance(results, dict):
            all_results.append(results)

    logger.info("Total metric series fetched: %d", len(all_results))
    return all_results