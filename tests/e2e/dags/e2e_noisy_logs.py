"""E2E fixture: DAG emitting thousands of log lines.

Exercises tail_lines, max_bytes truncation, and error-level filtering on a
log large enough that caps actually bite. Line format is deterministic so
tests can assert on markers.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG

try:  # Airflow 3
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2
    from airflow.operators.python import PythonOperator

TOTAL_LINES = 3000
ERROR_EVERY = 500  # lines 500, 1000, ... carry an ERROR marker


def _spew() -> None:
    for i in range(1, TOTAL_LINES + 1):
        if i % ERROR_EVERY == 0:
            print(f"ERROR E2E_NOISY_ERROR line {i:05d} something went sideways")
        else:
            print(f"INFO e2e noisy filler line {i:05d} lorem ipsum dolor sit amet")
    print("E2E_NOISY_LAST_LINE")


with DAG(
    dag_id="e2e_noisy_logs",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["e2e"],
):
    PythonOperator(task_id="spew", python_callable=_spew)
