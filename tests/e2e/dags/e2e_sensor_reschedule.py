"""E2E fixture: reschedule-mode sensor.

Pokes False until ~12s after the run started, then succeeds on its next poke.
The reschedule interval leaves enough time for standalone's scheduler to process
the executor event before the task becomes eligible again. This protects the
version-dependent ``try_number`` behavior documented in the README (Airflow
2.10+ no longer increments it for each reschedule).
"""

from __future__ import annotations

from datetime import datetime, timezone

from airflow import DAG

try:  # Airflow 3
    from airflow.providers.standard.sensors.python import PythonSensor
except ImportError:  # Airflow 2
    from airflow.sensors.python import PythonSensor

WAIT_SECONDS = 12
RESCHEDULE_INTERVAL_SECONDS = 20


def _ready(**context) -> bool:
    started = context["dag_run"].start_date
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    print(f"e2e sensor poke: {elapsed:.1f}s elapsed (need {WAIT_SECONDS}s)")
    return elapsed > WAIT_SECONDS


with DAG(
    dag_id="e2e_sensor_reschedule",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["e2e"],
):
    PythonSensor(
        task_id="wait_for_time",
        python_callable=_ready,
        mode="reschedule",
        poke_interval=RESCHEDULE_INTERVAL_SECONDS,
        timeout=300,
    )
