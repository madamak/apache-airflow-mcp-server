"""E2E fixture: trivially succeeding DAG (happy-path reads, write-tool targets)."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG

try:  # Airflow 3
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2
    from airflow.operators.python import PythonOperator


def _ok(message: str) -> str:
    print(f"E2E_SUCCESS_MARKER: {message}")
    return "ok"


with DAG(
    dag_id="e2e_success",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["e2e"],
):
    PythonOperator(
        task_id="say_ok",
        python_callable=_ok,
        op_kwargs={"message": "rendered {{ dag.dag_id }} {{ run_id }}"},
    )
