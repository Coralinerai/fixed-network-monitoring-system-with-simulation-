from ingest.ingest_inventory import run as ingest_inventory
from ingest.ingest_alarms import run as ingest_alarms
from ingest.ingest_metrics import run as ingest_metrics

def main():
    print("Starting ingestion...")

    ingest_inventory()
    ingest_alarms()
    ingest_metrics()

    print("Ingestion completed!")

if __name__ == "__main__":
    main()