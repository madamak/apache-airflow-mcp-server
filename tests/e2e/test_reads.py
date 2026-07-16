"""E2E: read tools against the seeded live instance."""

from __future__ import annotations

import json
import os

import pytest
from _helpers import ALIAS_INSTANCE, E2E_API_VERSION, INSTANCE, SEED_DAGS, call, seed_run_id

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


async def test_list_instances(client):
    payload = await call(client, "airflow_list_instances")
    expected_instances = {INSTANCE, ALIAS_INSTANCE}
    if E2E_API_VERSION == "v2":
        bearer_instance = os.environ.get("E2E_BEARER_INSTANCE")
        assert bearer_instance, "Airflow 3 harness must register a direct bearer instance"
        expected_instances.add(bearer_instance)
    assert set(payload["instances"]) == expected_instances
    assert payload["default_instance"] == INSTANCE
    assert payload["request_id"]


async def test_describe_instance_redacts_secrets(client, base_url):
    payload = await call(client, "airflow_describe_instance", instance=INSTANCE)
    assert payload["host"].startswith(base_url)
    assert payload["auth_type"] == "basic"
    dumped = json.dumps(payload).lower()
    assert "password" not in dumped
    assert "token" not in dumped


async def test_list_dags_contains_fixtures(client, base_url):
    payload = await call(client, "airflow_list_dags", instance=INSTANCE)
    by_id = {d["dag_id"]: d for d in payload["dags"]}
    for dag_id in SEED_DAGS:
        assert dag_id in by_id, f"fixture DAG {dag_id} missing from live listing"
        assert by_id[dag_id]["ui_url"].startswith(base_url)
        assert by_id[dag_id]["is_paused"] is False


async def test_get_dag(client):
    payload = await call(client, "airflow_get_dag", instance=INSTANCE, dag_id="e2e_failure")
    assert payload["dag"]["dag_id"] == "e2e_failure"
    assert payload["ui_url"]


async def test_list_dag_runs_state_filter(client):
    failed = await call(
        client, "airflow_list_dag_runs", instance=INSTANCE, dag_id="e2e_failure", state=["failed"]
    )
    run_ids = {r["dag_run_id"] for r in failed["dag_runs"]}
    assert seed_run_id("e2e_failure") in run_ids

    succeeded = await call(
        client, "airflow_list_dag_runs", instance=INSTANCE, dag_id="e2e_failure", state=["success"]
    )
    assert seed_run_id("e2e_failure") not in {r["dag_run_id"] for r in succeeded["dag_runs"]}


async def test_get_dag_run(client):
    payload = await call(
        client,
        "airflow_get_dag_run",
        instance=INSTANCE,
        dag_id="e2e_failure",
        dag_run_id=seed_run_id("e2e_failure"),
    )
    assert payload["dag_run"]["state"] == "failed"


async def test_list_task_instances_filters(client):
    run = seed_run_id("e2e_failure")
    failed = await call(
        client,
        "airflow_list_task_instances",
        instance=INSTANCE,
        dag_id="e2e_failure",
        dag_run_id=run,
        state=["failed"],
    )
    assert [ti["task_id"] for ti in failed["task_instances"]] == ["transform"]

    by_task = await call(
        client,
        "airflow_list_task_instances",
        instance=INSTANCE,
        dag_id="e2e_failure",
        dag_run_id=run,
        task_ids=["transform"],
    )
    assert by_task["count"] == 1

    none = await call(
        client,
        "airflow_list_task_instances",
        instance=INSTANCE,
        dag_id="e2e_failure",
        dag_run_id=run,
        task_ids=["does_not_exist"],
    )
    assert none["count"] == 0


async def test_get_task_instance_failure_attempts(client):
    payload = await call(
        client,
        "airflow_get_task_instance",
        instance=INSTANCE,
        dag_id="e2e_failure",
        dag_run_id=seed_run_id("e2e_failure"),
        task_id="transform",
    )
    assert payload["task_instance"]["state"] == "failed"
    # retries=1 -> two attempts; attempts.try_number is the authoritative log input
    assert payload["attempts"]["try_number"] == 2


async def test_get_task_instance_includes_rendered_fields(client):
    payload = await call(
        client,
        "airflow_get_task_instance",
        instance=INSTANCE,
        dag_id="e2e_success",
        dag_run_id=seed_run_id("e2e_success"),
        task_id="say_ok",
        include_rendered=True,
    )
    rendered = payload["rendered_fields"]
    assert rendered["truncated"] is False
    assert rendered["bytes_returned"] > 0
    # This value contains runtime context; static operator metadata cannot satisfy it.
    assert rendered["fields"]["op_args"] == []
    assert rendered["fields"]["op_kwargs"] == {
        "message": "rendered e2e_success e2e_seed_e2e_success"
    }


async def test_get_task_instance_sensor(client):
    payload = await call(
        client,
        "airflow_get_task_instance",
        instance=INSTANCE,
        dag_id="e2e_sensor_reschedule",
        dag_run_id=seed_run_id("e2e_sensor_reschedule"),
        task_id="wait_for_time",
    )
    assert payload["task_instance"]["state"] == "success"
    # Airflow >=2.10 no longer increments try_number per reschedule; just assert sanity.
    assert payload["attempts"]["try_number"] >= 1


async def test_dataset_events(client):
    payload = await call(
        client, "airflow_dataset_events", instance=INSTANCE, dataset_uri="file://e2e/orders"
    )
    assert payload["count"] >= 1
    assert payload["events"]
    id_field = "asset_id" if E2E_API_VERSION == "v2" else "dataset_id"
    assert all(event.get(id_field) is not None for event in payload["events"])
