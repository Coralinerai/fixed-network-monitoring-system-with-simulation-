from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from load.pg_loader import create_tables, set_simulation_date, set_simulation_window
from pipelines.pipeline_inventory import run as run_inventory
from pipelines.pipeline_alarms    import run as run_alarms
from pipelines.pipeline_metrics   import run as run_metrics

default_args = {
    "owner":            "network-team",
    "retries":          2,
    "retry_delay":      timedelta(minutes=1),
    "email_on_failure": False,
}


def run_inventory_task(**context):
    conf         = context["dag_run"].conf or {}
    window_start = conf.get("window_start")
    window_end   = conf.get("window_end")
    sim_date     = conf.get("sim_date")

    # update simulation state every window so Grafana shows current data
    if sim_date:
        set_simulation_date(sim_date)
    if window_end:
        set_simulation_window(window_end)

    run_inventory(window_start=window_start, window_end=window_end, sim_date=sim_date)


def run_alarms_task(**context):
    conf         = context["dag_run"].conf or {}
    window_start = conf.get("window_start")
    window_end   = conf.get("window_end")
    run_alarms(window_start=window_start, window_end=window_end)


def run_metrics_task(**context):
    conf         = context["dag_run"].conf or {}
    window_start = conf.get("window_start")
    window_end   = conf.get("window_end")
    run_metrics(window_start=window_start, window_end=window_end)


with DAG(
    dag_id       = "network_dashboard",
    description  = "Extract from OpenSearch and JSON files, load into PostgreSQL",
    schedule     = None,
    start_date   = datetime(2026, 1, 1),
    catchup      = False,
    default_args = default_args,
    tags         = ["network", "dashboard"],
) as dag:

    task_init_db = PythonOperator(
        task_id         = "init_db",
        python_callable = create_tables,
    )

    task_inventory = PythonOperator(
        task_id         = "load_inventory",
        python_callable = run_inventory_task,
        provide_context = True,
    )

    task_alarms = PythonOperator(
        task_id         = "load_alarms",
        python_callable = run_alarms_task,
        provide_context = True,
    )

    task_metrics = PythonOperator(
        task_id         = "load_metrics",
        python_callable = run_metrics_task,
        provide_context = True,
    )

    task_init_db >> task_inventory >> [task_alarms, task_metrics]