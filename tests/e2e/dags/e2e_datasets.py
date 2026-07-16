"""E2E fixture: dataset (Airflow 2) / asset (Airflow 3) producer + consumer.

Producer publishes an event on completion; the consumer is scheduled on the
dataset, so a successful producer run yields a dataset event AND an
automatically triggered consumer run.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG

try:  # Airflow 3
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2
    from airflow.operators.python import PythonOperator

DATASET_URI = "file://e2e/orders"

try:  # Airflow 3
    from airflow.sdk import Asset

    _DS = Asset(name="e2e_orders", uri=DATASET_URI)
except ImportError:  # Airflow 2
    from airflow.datasets import Dataset

    _DS = Dataset(DATASET_URI)


def _produce() -> str:
    print("E2E_DATASET_PRODUCED")
    return "produced"


def _consume() -> str:
    print("E2E_DATASET_CONSUMED")
    return "consumed"


with DAG(
    dag_id="e2e_dataset_producer",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["e2e"],
):
    PythonOperator(task_id="produce", python_callable=_produce, outlets=[_DS])

with DAG(
    dag_id="e2e_dataset_consumer",
    schedule=[_DS],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["e2e"],
):
    PythonOperator(task_id="consume", python_callable=_consume)
