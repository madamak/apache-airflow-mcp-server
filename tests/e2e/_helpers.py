"""Shared constants and helpers for the E2E suite (imported as a plain module,
since tests/ is not a package)."""

from __future__ import annotations

import asyncio
import os
import time

E2E_BASE_URL = os.getenv("E2E_AIRFLOW_BASE_URL")
E2E_API_VERSION = os.getenv("E2E_AIRFLOW_API_VERSION", "v1")

INSTANCE = "e2e-local"
ALIAS_INSTANCE = "e2e-alias"  # same server registered under 127.0.0.1 (mismatch tests)

SEED_DAGS = [
    "e2e_success",
    "e2e_failure",
    "e2e_noisy_logs",
    "e2e_sensor_reschedule",
    "e2e_dataset_producer",
    "e2e_dataset_consumer",
]

TERMINAL = {"success", "failed"}

WRITE_TOOLS = {
    "airflow_trigger_dag",
    "airflow_clear_task_instances",
    "airflow_clear_dag_run",
    "airflow_pause_dag",
    "airflow_unpause_dag",
}


def seed_run_id(dag_id: str) -> str:
    return f"e2e_seed_{dag_id}"


def unique_run_id(prefix: str) -> str:
    return f"{prefix}_{int(time.time() * 1000)}"


async def call(client, tool: str, **args):
    """Call a tool and return its structured payload."""
    result = await client.call_tool(tool, args)
    return result.data


async def wait_run_state(
    client, dag_id: str, dag_run_id: str, *, until: set[str] = TERMINAL, timeout: int = 180
) -> str:
    """Poll a run through the MCP tools until it reaches a state in `until`."""
    deadline = time.monotonic() + timeout
    state = None
    while time.monotonic() < deadline:
        payload = await call(
            client, "airflow_get_dag_run", instance=INSTANCE, dag_id=dag_id, dag_run_id=dag_run_id
        )
        state = payload["dag_run"]["state"]
        if state in until:
            return state
        await asyncio.sleep(3)
    raise AssertionError(f"run {dag_id}/{dag_run_id} stuck in state={state} after {timeout}s")
