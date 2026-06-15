# Network Dashboard Pipeline

Reads metric JSON files and alarm JSON files, transforms them into relational tables in PostgreSQL, exposed via Grafana.

---

## Project Structure

```
network_dashboard/
├── config.py                          # paths and DB connection string
│
├── extract/
│   └── file_reader.py                 # reads metric files + alarm file from disk
│
├── transform/
│   ├── transform_metrics.py           # OpenTSDB JSON → metric tuples
│   ├── transform_alarms.py            # ES alarm JSON → alarm tuples
│   └── transform_inventory.py         # derives devices from alarms → inventory tuples
│
├── load/
│   └── pg_loader.py                   # all PostgreSQL insert/upsert functions
│
├── pipelines/
│   ├── pipeline_inventory.py          # extract → transform → load for inventory
│   ├── pipeline_alarms.py             # extract → transform → load for alarms
│   └── pipeline_metrics.py            # extract → transform → load for metrics
│
├── dags/
│   └── network_dag.py                 # Airflow DAG
│
├── grafana/
│   └── provisioning/datasources/
│       └── postgres.yml               # auto-configures Grafana datasource
│
├── data/                              # your data goes here (gitignored)
│   ├── metrics/                       # all metric JSON files
│   │   ├── activePath.LS_AWALA.json
│   │   ├── adminState.LS_AWALA.json
│   │   └── ...
│   └── elasticsearch_data/
│       └── alarms-active.json
│
├── init_db.py                         # run once to create tables
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── .gitignore
```

---

## Data Folder Setup

Your data files are gitignored. Each team member must place their data locally:

```
network_dashboard/
└── data/
    ├── metrics/                  ← copy your metrics_data folder contents here
    └── elasticsearch_data/       ← copy your elasticsearch_data folder contents here
```

---

## Setup

### 1. Clone and configure
```bash
git clone <your-repo>
cd network_dashboard
cp .env.example .env
# edit .env — set POSTGRES_PASSWORD at minimum
```

### 2. Place your data files
```bash
mkdir -p data/metrics data/elasticsearch_data
# copy all metric JSON files into data/metrics/
# copy alarms-active.json into data/elasticsearch_data/
```

### 3. Start everything
```bash
docker compose up --build
```

### 4. Initialize the database (first time only)
```bash
docker compose exec airflow-webserver python init_db.py
```

### 5. Access services
- Airflow  → http://localhost:8080  (admin / admin)
- Grafana  → http://localhost:3000  (admin / admin)
- Postgres → localhost:5432

### 6. Test pipelines manually before enabling the DAG
```bash
docker compose exec airflow-webserver python pipelines/pipeline_inventory.py
docker compose exec airflow-webserver python pipelines/pipeline_alarms.py
docker compose exec airflow-webserver python pipelines/pipeline_metrics.py
```

### 7. Enable the DAG in Airflow UI
Open http://localhost:8080 → enable `network_dashboard` → trigger manually to test.

---

## Without Docker (local development)

```bash
pip install -r requirements.txt
cp .env.example .env
# set PG_CONN to your local postgres
# set METRICS_DIR and ALARMS_FILE to absolute paths on your machine
python init_db.py
python pipelines/pipeline_inventory.py
python pipelines/pipeline_alarms.py
python pipelines/pipeline_metrics.py
```

---

## Grafana Queries

**Active service-affecting alarms:**
```sql
SELECT device_name, alarm_type, severity, raised_at
FROM active_alarms
WHERE is_service_affecting = true
ORDER BY raised_at DESC;
```

**Alarm count per device:**
```sql
SELECT device_name, COUNT(*) as total, severity
FROM active_alarms
GROUP BY device_name, severity
ORDER BY total DESC;
```

**Latest CPU idle per device:**
```sql
SELECT device_name, component, value, collected_at
FROM metrics
WHERE metric_name = 'percent-cpu-idle'
ORDER BY device_name;
```

**Latest optical Rx power:**
```sql
SELECT device_name, component, value as rx_power_dbm, collected_at
FROM metrics
WHERE metric_name = 'current'
ORDER BY device_name;
```

**Device online status:**
```sql
SELECT device_name, network_name, online_state, updated_at
FROM inventory
ORDER BY device_name;
```
