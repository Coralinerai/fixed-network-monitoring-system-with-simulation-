"""
Simulation DAG — triggers network_dashboard DAG for each 1-hour window
for a single day (triggered manually with config).

Usage: Trigger with config {"sim_date": "2026-04-06"}
If no config provided, defaults to 2026-04-06.

Flow per day:
  1. prepare_day  — clears that day's data, sets simulation_state
  2. window_00 to window_23 — loads data hour by hour
  3. daily_report — generates and saves full day report (never fails)
"""
import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from load.pg_loader import get_connection, set_simulation_date, clear_day_data, save_report

logger = logging.getLogger(__name__)

SIMULATION_DATES = [
    "2026-04-06",
    "2026-04-07",
    "2026-04-08",
    "2026-04-09",
    "2026-04-10",
    "2026-04-11",
    "2026-04-12",
]


def get_1h_windows(date_str: str) -> list:
    day_start = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    windows = []
    current = day_start
    while current.date() == day_start.date():
        window_end = current + timedelta(hours=1)
        windows.append((
            current.strftime("%Y-%m-%dT%H:%M:%SZ"),
            window_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        ))
        current = window_end
    return windows


def prepare_day(**context):
    """Clear that day's data and set simulation_state before windows run."""
    conf = context["dag_run"].conf or {}
    sim_date = conf.get("sim_date", SIMULATION_DATES[0])
    clear_day_data(sim_date)
    set_simulation_date(sim_date)
    logger.info("Day %s prepared — data cleared, simulation date set", sim_date)


def advance_and_report(**context):
    """Generate daily report using standalone generate_report module."""
    conf = context["dag_run"].conf or {}
    sim_date = conf.get("sim_date", SIMULATION_DATES[0])
    set_simulation_date(sim_date)

    try:
        from reports.generate_report import generate_report
        report = generate_report(sim_date)
        save_report(sim_date, report)
        logger.info("Report saved for %s", sim_date)
    except Exception as e:
        logger.error("Report generation failed: %s", e)
        save_report(sim_date, f"REPORT GENERATION FAILED FOR {sim_date}\nError: {e}")
        logger.info("Saved error report for %s", sim_date)


default_args = {
    "owner": "network-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
}

with DAG(
    dag_id       = "network_simulation",
    description  = "Simulate one day — trigger with {sim_date: YYYY-MM-DD}",
    schedule     = None,
    start_date   = datetime(2026, 1, 1),
    catchup      = False,
    default_args = default_args,
    tags         = ["network", "simulation"],
) as dag:

    sample_date = SIMULATION_DATES[0]
    windows = get_1h_windows(sample_date)
    day_tasks = []

    prepare_task = PythonOperator(
        task_id         = "prepare_day",
        python_callable = prepare_day,
        provide_context = True,
    )

    for idx, (w_start, w_end) in enumerate(windows):
        trigger_task = TriggerDagRunOperator(
            task_id             = f"window_{idx:02d}_{w_start[11:16].replace(':', '')}",
            trigger_dag_id      = "network_dashboard",
            conf                = {
                "window_start": "{{ dag_run.conf.get('sim_date', '2026-04-06') }}T" + w_start[11:],
                "window_end":   "{{ dag_run.conf.get('sim_date', '2026-04-06') }}T" + w_end[11:],
                "sim_date":     "{{ dag_run.conf.get('sim_date', '2026-04-06') }}",
            },
            wait_for_completion = True,
            poke_interval       = 10,
            reset_dag_run       = True,
        )
        day_tasks.append(trigger_task)

    report_task = PythonOperator(
        task_id         = "daily_report",
        python_callable = advance_and_report,
        provide_context = True,
    )

    # prepare → windows → report
    prepare_task >> day_tasks[0]
    for i in range(len(day_tasks) - 1):
        day_tasks[i] >> day_tasks[i + 1]
    day_tasks[-1] >> report_task