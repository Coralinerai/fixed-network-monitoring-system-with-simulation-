import logging
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extract.file_metrics_client import fetch_all_metrics
from transform.transform_metrics import transform_all_metrics
from load.pg_loader import upsert_metrics

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run(window_start: str = None, window_end: str = None):
    logger.info("=== Starting metrics pipeline ===")

    # parse window strings to datetime if provided
    ws = datetime.fromisoformat(window_start.replace("Z", "+00:00")) if window_start else None
    we = datetime.fromisoformat(window_end.replace("Z", "+00:00")) if window_end else None

    # get metrics directory from config
    from config import METRICS_DIR

    raw = fetch_all_metrics(METRICS_DIR, window_start=ws, window_end=we)
    if not raw:
        logger.warning("No metrics found for window %s → %s, skipping", window_start, window_end)
        return

    data = transform_all_metrics(raw)
    if not data:
        logger.warning("No valid metric rows after transform, skipping")
        return

    upsert_metrics([r.to_tuple() for r in data])
    logger.info("=== Metrics pipeline complete — %d rows loaded ===", len(data))


if __name__ == "__main__":
    run()