"""E2E: UI URL resolution, SSRF guard, and instance-mismatch protection."""

from __future__ import annotations

import pytest
from _helpers import ALIAS_INSTANCE, E2E_API_VERSION, INSTANCE, call, seed_run_id
from fastmcp.exceptions import ToolError

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


async def test_resolve_grid_url(client, base_url):
    run = seed_run_id("e2e_failure")
    if E2E_API_VERSION == "v2":
        url = f"{base_url}/dags/e2e_failure"
    else:
        url = f"{base_url}/dags/e2e_failure/grid?dag_run_id={run}"
    payload = await call(client, "airflow_resolve_url", url=url)
    assert payload["instance"] == INSTANCE
    assert payload["dag_id"] == "e2e_failure"
    assert payload.get("dag_run_id") == (None if E2E_API_VERSION == "v2" else run)


async def test_read_via_ui_url(client, base_url):
    run = seed_run_id("e2e_failure")
    if E2E_API_VERSION == "v2":
        url = f"{base_url}/dags/e2e_failure/runs/{run}"
    else:
        url = f"{base_url}/dags/e2e_failure/grid?dag_run_id={run}"
    payload = await call(client, "airflow_get_dag_run", ui_url=url)
    assert payload["dag_run"]["state"] == "failed"


@pytest.mark.skipif(E2E_API_VERSION != "v2", reason="Airflow 3 UI route")
async def test_resolve_airflow3_task_url(client, base_url):
    run = seed_run_id("e2e_failure")
    url = f"{base_url}/dags/e2e_failure/runs/{run}/tasks/transform?try_number=2"
    payload = await call(client, "airflow_resolve_url", url=url)
    assert payload == {
        "instance": INSTANCE,
        "route": "task",
        "dag_id": "e2e_failure",
        "dag_run_id": run,
        "task_id": "transform",
        "try_number": 2,
        "request_id": payload["request_id"],
    }


async def test_ssrf_guard_rejects_unknown_host(client):
    with pytest.raises(ToolError) as exc:
        await call(
            client, "airflow_resolve_url", url="https://evil.example.com/dags/e2e_failure/grid"
        )
    assert "NOT_FOUND" in str(exc.value)


async def test_instance_mismatch_guard(client, base_url):
    # e2e-alias registers the same server under 127.0.0.1; passing its URL with
    # an explicit conflicting instance must fail rather than guess.
    alias_url = base_url.replace("localhost", "127.0.0.1")
    assert alias_url != base_url, "harness must expose the server via localhost"
    url = f"{alias_url}/dags/e2e_failure/grid"
    resolved = await call(client, "airflow_resolve_url", url=url)
    assert resolved["instance"] == ALIAS_INSTANCE

    with pytest.raises(ToolError) as exc:
        await call(client, "airflow_get_dag", instance=INSTANCE, ui_url=url)
    assert "INSTANCE_MISMATCH" in str(exc.value)
