# Network Dashboard — Time Travel Simulation

Replays 7 days (April 6–12, 2026) of historical Nokia OLT network data through a full ETL pipeline — extracting metrics from raw JSON files and alarms/inventory from OpenSearch, transforming them, loading into PostgreSQL, and visualizing in Grafana.

---

## Project Structure

```
Time_travel/
├── config.py                          # paths, OpenSearch + Postgres connection settings
│
├── extract/
│   ├── file_metrics_client.py         # reads raw metric JSON files, filters by time window
│   └── opensearch_client.py           # fetches alarms (accumulated) + inventory (per day)
│
├── transform/
│   ├── transform_metrics.py           # raw JSON → MetricRow (classifies OPTICAL/HARDWARE/
│   │                                   #   TRAFFIC/DROPS/FEC_ERRORS, divides OPTICAL by 10)
│   ├── transform_alarms.py            # OpenSearch alarm docs → alarm tuples
│   └── transform_inventory.py         # OpenSearch inventory docs → inventory tuples
│                                       #   (processes ALL devices per file)
│
├── load/
│   └── pg_loader.py                   # upsert_metrics, refresh_alarms, upsert_inventory,
│                                       #   set_simulation_date, set_simulation_window,
│                                       #   clear_day_data, save_report, create_tables
│
├── pipelines/
│   ├── pipeline_inventory.py          # extract → transform → load, updates simulation_state
│   ├── pipeline_alarms.py             # extract (accumulated) → transform → refresh
│   └── pipeline_metrics.py            # extract (file-based, windowed) → transform → load
│
├── reports/
│   ├── __init__.py
│   └── generate_report.py             # standalone daily health report generator
│                                       #   (CLI: python reports/generate_report.py 2026-04-09)
│
├── dags/
│   ├── network_dag.py                 # ETL DAG — runs per 1-hour window (triggered by simulation_dag)
│   └── simulation_dag.py              # orchestrates one full day: prepare_day → 24 windows → report
│
├── grafana/
│   └── provisioning/
│       ├── datasources/
│       │   └── postgres.yml           # auto-configures Grafana PostgreSQL datasource
│       └── dashboards/
│           ├── dashboard.yaml         # provisioning config
│           ├── dashboard_overview.json    # device status, alarms, KPIs (uid: network-overview)
│           ├── dashboard_optical.json     # PON port RX/TX power, TX bias (uid: optical-health)
│           ├── dashboard_hardware.json    # CPU, free/used memory (uid: hardware-resources)
│           ├── dashboard_traffic.json     # inbound/outbound throughput, drops (uid: traffic-drops)
│           └── dashboard_report.json      # daily network report (uid: daily-report)
│
├── ingest/
│   ├── ingest_inventory.py            # one-time: loads inventory JSON files into OpenSearch
│   └── ingest_metrics.py              # one-time: loads metric JSON files (if applicable)
│
├── data/                              # gitignored — place your data files here
│   ├── metrics/                       # raw OpenTSDB-format metric JSON files per device
│   └── elasticsearch_data/
│       ├── alarms/                    # alarm documents
│       └── inventory/                 # inv_*.json — 5 devices per file, 7 days
│
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── .gitignore
```

---

## Architecture

```
                  ┌─────────────────────┐
                  │  Raw JSON files      │
                  │  (data/metrics/)     │──────┐
                  └─────────────────────┘      │
                                                  ▼
┌─────────────────────┐              ┌──────────────────┐
│  OpenSearch          │              │   EXTRACT         │
│  - alarms-active     │─────────────▶│   TRANSFORM       │
│  - inventory-daily   │              │   LOAD            │
└─────────────────────┘              └──────────────────┘
                                                  │
                                                  ▼
                                       ┌──────────────────┐
                                       │   PostgreSQL      │
                                       │  - metrics        │
                                       │  - active_alarms  │
                                       │  - inventory      │
                                       │  - simulation_state│
                                       │  - daily_reports  │
                                       └──────────────────┘
                                                  │
                                                  ▼
                                       ┌──────────────────┐
                                       │   Grafana         │
                                       │  5 dashboards     │
                                       └──────────────────┘

         ▲
         │ orchestrates
┌─────────────────────┐
│  Apache Airflow      │
│  simulation_dag      │──▶ network_dag (×24 windows/day)
└─────────────────────┘
```

**Note:** OpenTSDB is bypassed entirely in this simulation — metrics are read directly from raw JSON files via `file_metrics_client.py`. This was a deliberate design decision after HBase/OpenTSDB instability with large historical datasets.

---

## Database Schema

```sql
simulation_state (id, sim_date DATE, current_window TIMESTAMP)
metrics (device_name, category, metric_name, component, collected_at, value)
  PK (device_name, metric_name, component, collected_at)
active_alarms (alarm_id PK, device_name, component, severity, alarm_type,
               is_service_affecting, alarm_text, raised_at, acknowledged)
inventory (device_name, collection_date, ip_address, online_state,
           hardware_type, software_version, device_type,
           collection_status, last_seen, onu_count)
  PK (device_name, collection_date)
daily_reports (report_date PK, report_text, generated_at)
```

---

## Setup

### 1. Clone and configure
```bash
git clone <your-repo>
cd Time_travel
cp .env.example .env
# edit .env — set OPENSEARCH_PASSWORD and POSTGRES_PASSWORD
```

### 2. Place your data files
```bash
mkdir -p data/metrics data/elasticsearch_data/inventory data/elasticsearch_data/alarms
# copy raw metric JSON files into data/metrics/
# copy inv_*.json files into data/elasticsearch_data/inventory/
# copy alarm JSON files into data/elasticsearch_data/alarms/
```

### 3. Start everything
```bash
docker compose up --build -d
```

### 4. One-time ingestion into OpenSearch
```bash
docker compose run --rm ingest python ingest/ingest_inventory.py
docker compose run --rm ingest python ingest/ingest_metrics.py   # if applicable
```

Verify:
```bash
curl --noproxy '*' -k -u admin:${OPENSEARCH_PASSWORD} "https://localhost:9200/inventory-daily/_search?size=50"
```
Expect 35 documents (5 devices × 7 days).

### 5. Initialize the database
The `init_db` task in `network_dag.py` runs `CREATE TABLE IF NOT EXISTS` on every run, so the schema initializes automatically on first trigger. To run manually:
```bash
docker compose run --rm -e PYTHONPATH=/app airflow-scheduler python -c "
from load.pg_loader import create_tables
create_tables()
"
```

### 6. Access services
- Airflow  → http://localhost:8080  (admin / admin)
- Grafana  → http://localhost:3000  (admin / admin)
- Postgres → localhost:5432
- OpenSearch → https://localhost:9200

---

## Running the Simulation

The simulation replays **one full day at a time**. Trigger `network_simulation` from the Airflow UI with config:

```json
{"sim_date": "2026-04-06"}
```

### What happens
1. **`prepare_day`** — clears that day's `metrics` and `inventory`, sets `simulation_state.sim_date`
2. **24 hourly windows** — each triggers `network_dashboard` DAG with `window_start`/`window_end`:
   - `load_inventory` — fetches that day's inventory snapshot, updates `simulation_state`
   - `load_alarms` — fetches alarms with `raisedTime <= window_end` (accumulated, upserted — never deleted between windows)
   - `load_metrics` — reads raw JSON files filtered to the 1-hour window
3. **`daily_report`** — calls `reports/generate_report.py`, saves to `daily_reports` table (wrapped in try/except — never fails the DAG)

Run the days **in order** (April 6 → 12) so alarms accumulate correctly day over day. Duration: ~12–18 min per day.

### Resetting the simulation
```sql
TRUNCATE metrics;
TRUNCATE inventory;
TRUNCATE active_alarms;
TRUNCATE daily_reports;
UPDATE simulation_state SET sim_date = '2026-04-06', current_window = NULL;
```

---

## Dashboards

All 5 dashboards share a `device` variable (dropdown, defaults to **All**) and a fixed time range (`2026-04-06` → `2026-04-13`) so the X-axis always covers the simulation period.

| Dashboard | UID | Contents |
|---|---|---|
| Network Overview | `network-overview` | KPI stat cards, device status table (with links to other dashboards), alarms by severity pie chart, active alarms table |
| Optical Health | `optical-health` | PON port status table, RX power, TX power, TX bias |
| Hardware Resources | `hardware-resources` | CPU usage %, free memory, used memory |
| Traffic & Drops | `traffic-drops` | Inbound/outbound throughput (bps, computed via `LAG()`), packet drops delta |
| Daily Report | `daily-report` | Available reports list, full report text |

### Device filter
Uses `${device:sqlstring}` with the variable query:
```sql
SELECT device_name AS __text, device_name AS __value FROM inventory GROUP BY device_name ORDER BY device_name
```
applied as `WHERE device_name IN (${device:sqlstring})`.

### Throughput formula
```
throughput_bps = (value_t - value_{t-1}) * 8 / (t - t_{-1} in seconds)
```

### Optical value scaling
Raw OPTICAL values are stored ×10 in the source data and divided by 10 in `transform_metrics.py`. A value of `-40 dBm` indicates **no signal** (dead PON port).

---

## Daily Report

Generate or regenerate a report for any simulated day:
```bash
docker compose run --rm -e PYTHONPATH=/app airflow-scheduler python reports/generate_report.py 2026-04-09
```

Includes: device status, alarm breakdown (total / service-affecting / by type), CPU & memory per device, dead/active PON ports, top interfaces by packet drops, peak traffic, FEC/BIP errors, and metrics coverage counts.

---

## Useful Queries

**Check loaded date range:**
```sql
SELECT MIN(collected_at), MAX(collected_at), COUNT(*) FROM metrics WHERE DATE(collected_at) = '2026-04-09';
```

**Current simulation state:**
```sql
SELECT * FROM simulation_state;
```

**Active alarms accumulated up to current sim day:**
```sql
SELECT device_name, alarm_type, severity, raised_at
FROM active_alarms
WHERE DATE(raised_at) <= (SELECT sim_date FROM simulation_state LIMIT 1)
ORDER BY is_service_affecting DESC, raised_at DESC;
```

**Dead PON ports for current day:**
```sql
SELECT DISTINCT device_name, component
FROM metrics
WHERE category = 'OPTICAL' AND metric_name LIKE '%rx-power%' AND value = -40
  AND DATE(collected_at) = (SELECT sim_date FROM simulation_state LIMIT 1);
```

---

## Differences from the Live (Original) Repo

| | Simulation | Live |
|---|---|---|
| Metrics source | Raw JSON files (`file_metrics_client.py`) | OpenTSDB (`opentsdb_client.py`) |
| Schedule | Manual trigger, `schedule=None` | `*/15 * * * *` |
| Date filter | `simulation_state.sim_date` | `CURRENT_DATE` |
| Grafana time range | Fixed `2026-04-06` → `2026-04-13` | `now-24h` → `now` |
| Day boundary | `prepare_day` clears metrics/inventory | No clearing — data accumulates |
| Alarm accumulation | `raisedTime <= window_end`, upsert | OpenSearch is source of truth, full refresh every run |
| Dashboard UIDs | `network-overview`, `optical-health`, etc. | `live-network-overview`, `live-optical-health`, etc. |
