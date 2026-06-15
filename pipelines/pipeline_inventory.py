import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extract.opensearch_client import fetch_inventory
from transform.transform_inventory import transform_all_inventory
from load.pg_loader import upsert_inventory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run(window_start: str = None, window_end: str = None, sim_date: str = None):
    logger.info("=== Starting inventory pipeline ===")

    raw = fetch_inventory(sim_date=sim_date)
    if not raw:
        logger.warning("No inventory documents fetched from OpenSearch, skipping")
        return

    data = transform_all_inventory(raw)
    if not data:
        logger.warning("No valid inventory rows after transform, skipping")
        return

    upsert_inventory(data)
    logger.info("=== Inventory pipeline complete ===")


if __name__ == "__main__":
    run()