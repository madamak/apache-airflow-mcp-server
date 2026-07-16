"""E2E fixture: DAG whose task always fails, with one retry.

Gives the suite a run in state `failed`, a task instance with try_number 2,
and log lines carrying a recognizable error marker for filter_level tests.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG

try:  # Airflow 3
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2
    from airflow.operators.python import PythonOperator

FAILURE_MARKER = "E2E_FAILURE_MARKER"


def _boom() -> None:
    print("about to fail on purpose")
    raise ValueError(f"{FAILURE_MARKER}: transform exploded (fixture failure)")


with DAG(
    dag_id="e2e_failure",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["e2e"],
):
    PythonOperator(
        task_id="transform",
        python_callable=_boom,
        retries=1,
        retry_delay=timedelta(seconds=3),
    )
