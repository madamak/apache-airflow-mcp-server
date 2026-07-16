"""E2E: write tools — trigger, clear (dry-run and real), pause/unpause.

Each test creates its own runs so the seeded fixtures stay untouched for the
read/log tests regardless of execution order.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone

import pytest
from _helpers import INSTANCE, WRITE_TOOLS, call, seed_run_id, unique_run_id, wait_run_state

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


async def _trigger_and_finish(client, dag_id: str = "e2e_success") -> str:
    run_id = unique_run_id(f"e2e_test_{dag_id}")
    payload = await call(
        client, "airflow_trigger_dag", instance=INSTANCE, dag_id=dag_id, dag_run_id=run_id
    )
    assert payload["dag_run_id"] == run_id
    assert await wait_run_state(client, dag_id, run_id) == "success"
    return run_id


async def _wait_task_attempt(
    client,
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    minimum_try_number: int,
    terminal_state: str,
    timeout: int = 180,
):
    """Wait for a cleared task to execute again, not merely for its run's old terminal state."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = await call(
            client,
            "airflow_get_task_instance",
            instance=INSTANCE,
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
        )
        if (
            last["attempts"]["try_number"] >= minimum_try_number
            and last["task_instance"]["state"] == terminal_state
        ):
            return last
        await asyncio.sleep(3)
    raise AssertionError(
        f"task {dag_id}/{dag_run_id}/{task_id} did not reach "
        f"try>={minimum_try_number}, state={terminal_state}; last={last}"
    )


async def test_write_tools_are_registered(client):
    tools = {t.name for t in await client.list_tools()}
    assert WRITE_TOOLS <= tools


async def test_trigger_dag_runs_to_success(client):
    await _trigger_and_finish(client)


async def test_trigger_dag_with_conf(client):
    run_id = unique_run_id("e2e_test_conf")
    await call(
        client,
        "airflow_trigger_dag",
        instance=INSTANCE,
        dag_id="e2e_success",
        dag_run_id=run_id,
        conf={"source": "e2e"},
    )
    await wait_run_state(client, "e2e_success", run_id)
    payload = await call(
        client, "airflow_get_dag_run", instance=INSTANCE, dag_id="e2e_success", dag_run_id=run_id
    )
    assert payload["dag_run"].get("conf") == {"source": "e2e"}


async def test_clear_dag_run_dry_run_then_real(client):
    run_id = await _trigger_and_finish(client)

    dry = await call(
        client,
        "airflow_clear_dag_run",
        instance=INSTANCE,
        dag_id="e2e_success",
        dag_run_id=run_id,
        dry_run=True,
    )
    assert dry["dry_run"] is True
    assert dry["cleared"], "dry run should report the task instances it would clear"
    state = await call(
        client, "airflow_get_dag_run", instance=INSTANCE, dag_id="e2e_success", dag_run_id=run_id
    )
    assert state["dag_run"]["state"] == "success", "dry_run must not mutate the run"

    real = await call(
        client,
        "airflow_clear_dag_run",
        instance=INSTANCE,
        dag_id="e2e_success",
        dag_run_id=run_id,
        dry_run=False,
    )
    assert real["dry_run"] is False
    # Poll the task attempt: polling only the run would accept its pre-clear
    # terminal state before the scheduler observes the clear.
    ti = await _wait_task_attempt(
        client,
        dag_id="e2e_success",
        dag_run_id=run_id,
        task_id="say_ok",
        minimum_try_number=2,
        terminal_state="success",
    )
    assert ti["attempts"]["try_number"] >= 2


async def test_clear_task_instances_dry_run(client):
    run_id = await _trigger_and_finish(client)
    payload = await call(
        client,
        "airflow_clear_task_instances",
        instance=INSTANCE,
        dag_id="e2e_success",
        task_ids=["say_ok"],
        dry_run=True,
    )
    assert payload["dag_id"] == "e2e_success"
    assert payload["dry_run"] is True
    assert payload["cleared"], f"expected dry-run to list clearable TIs (run {run_id})"


async def test_clear_task_instances_real_reruns_test_created_failure(client):
    dag_id = "e2e_failure"
    task_id = "transform"
    run_id = unique_run_id("e2e_test_task_clear")
    # The clear-task endpoint has no dag_run_id filter on Airflow 2. Give this
    # test-created run its own logical date so the destructive call is scoped to
    # exactly this run on both API families.
    run_date = (
        (datetime.now(timezone.utc) - timedelta(seconds=1)).replace(microsecond=0).isoformat()
    )
    await call(
        client,
        "airflow_trigger_dag",
        instance=INSTANCE,
        dag_id=dag_id,
        dag_run_id=run_id,
        logical_date=run_date,
    )
    assert await wait_run_state(client, dag_id, run_id) == "failed"

    before = await call(
        client,
        "airflow_get_task_instance",
        instance=INSTANCE,
        dag_id=dag_id,
        dag_run_id=run_id,
        task_id=task_id,
    )
    before_try = before["attempts"]["try_number"]
    assert before_try >= 2, "fixture has retries=1 and must exhaust both attempts"
    seed_before = await call(
        client,
        "airflow_get_task_instance",
        instance=INSTANCE,
        dag_id=dag_id,
        dag_run_id=seed_run_id(dag_id),
        task_id=task_id,
    )

    cleared = await call(
        client,
        "airflow_clear_task_instances",
        instance=INSTANCE,
        dag_id=dag_id,
        task_ids=[task_id],
        start_date=run_date,
        end_date=run_date,
        dry_run=False,
        reset_dag_runs=True,
    )
    assert cleared["dry_run"] is False
    assert cleared["cleared"], "real clear should report the targeted failed task instance"
    cleared_body = cleared["cleared"]
    if isinstance(cleared_body, dict):
        cleared_rows = cleared_body.get("task_instances", cleared_body.get("cleared", []))
    else:
        cleared_rows = cleared_body
    assert cleared_rows, f"clear response did not contain task instances: {cleared_body}"
    assert {row["dag_run_id"] for row in cleared_rows} == {run_id}

    after = await _wait_task_attempt(
        client,
        dag_id=dag_id,
        dag_run_id=run_id,
        task_id=task_id,
        minimum_try_number=before_try + 1,
        terminal_state="failed",
    )
    assert after["attempts"]["try_number"] > before_try
    seed_after = await call(
        client,
        "airflow_get_task_instance",
        instance=INSTANCE,
        dag_id=dag_id,
        dag_run_id=seed_run_id(dag_id),
        task_id=task_id,
    )
    assert seed_after["task_instance"]["state"] == seed_before["task_instance"]["state"]
    assert seed_after["attempts"]["try_number"] == seed_before["attempts"]["try_number"]


async def test_pause_unpause_roundtrip(client):
    try:
        paused = await call(client, "airflow_pause_dag", instance=INSTANCE, dag_id="e2e_success")
        assert paused["is_paused"] is True
        dag = await call(client, "airflow_get_dag", instance=INSTANCE, dag_id="e2e_success")
        assert dag["dag"]["is_paused"] is True
    finally:
        unpaused = await call(
            client, "airflow_unpause_dag", instance=INSTANCE, dag_id="e2e_success"
        )
        assert unpaused["is_paused"] is False
