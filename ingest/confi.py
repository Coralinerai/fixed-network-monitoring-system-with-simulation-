import os
from dotenv import load_dotenv

load_dotenv()

# PostgreSQL
PG_CONN = os.getenv("PG_CONN", "postgresql://admin:password@localhost:5432/network_dashboard")
#Opensearch
OPENSEARCH_HOST         = os.getenv("OPENSEARCH_HOST",         "https://opensearch-node:9200")
OPENSEARCH_USER         = os.getenv("OPENSEARCH_USER",         "admin")
OPENSEARCH_PASSWORD     = os.getenv("OPENSEARCH_PASSWORD",     "")
OPENSEARCH_ALARMS_INDEX = os.getenv("OPENSEARCH_ALARMS_INDEX", "alarms-active")
INVENTORY_INDEX         = os.getenv("INVENTORY_INDEX",         "inventory-daily")
#Opentsdb host 
OPENTSDB_HOST = os.getenv("OPENTSDB_HOST", "http://opentsdb:4242")

# Paths to your data directories
METRICS_DIR  = os.getenv("METRICS_DIR",  "./data/metrics")   # folder with all metric JSON files
ALARMS_FILE  = os.getenv("ALARMS_FILE",  "./data/elasticsearch_data/alarms-active.json")
# ingest/config.py
INVENTORY_DIR = os.getenv("INVENTORY_DIR", "./data/elasticsearch_data/inventory")
