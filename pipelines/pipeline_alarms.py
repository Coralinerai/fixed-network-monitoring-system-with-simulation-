import logging
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extract.opensearch_client import fetch_active_alarms
from transform.transform_alarms import transform_all_alarms
from load.pg_loader import refresh_alarms

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run(window_start: str = None, window_end: str = None):
    logger.info("=== Starting alarms pipeline ===")

    ws = datetime.fromisoformat(window_start.replace("Z", "+00:00")) if window_start else None
    we = datetime.fromisoformat(window_end.replace("Z", "+00:00")) if window_end else None

    raw  = fetch_active_alarms(window_start=ws, window_end=we)
    data = transform_all_alarms(raw)
    refresh_alarms(data)

    logger.info("=== Alarms pipeline complete ===")


if __name__ == "__main__":
    run()