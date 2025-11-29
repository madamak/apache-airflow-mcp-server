from urllib.parse import quote

import pytest

from airflow_mcp.errors import AirflowToolError
from airflow_mcp.url_utils import build_airflow_ui_url, resolve_and_validate


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch):
    # Use example instances; provide env vars for placeholders
    from pathlib import Path

    current = Path(__file__).resolve()
    examples = current.parent.parent / "examples" / "instances.yaml"
    monkeypatch.setenv("AIRFLOW_MCP_INSTANCES_FILE", str(examples))
    monkeypatch.setenv("AIRFLOW_INSTANCE_DATA_STG_USERNAME", "u")
    monkeypatch.setenv("AIRFLOW_INSTANCE_DATA_STG_PASSWORD", "p")
    monkeypatch.setenv("AIRFLOW_INSTANCE_ML_STG_USERNAME", "u")
    monkeypatch.setenv("AIRFLOW_INSTANCE_ML_STG_PASSWORD", "p")


def test_build_grid_url():
    url = build_airflow_ui_url("data-stg", "grid", "my_dag", dag_run_id="dr-1")
    assert isinstance(url, str)
    assert "/dags/my_dag/grid" in url
    assert "dag_run_id=dr-1" in url


def test_build_task_url():
    url = build_airflow_ui_url(
        "data-stg", "task", "my_dag", dag_run_id="dr1", task_id="task_a"
    )
    assert isinstance(url, str)
    assert url.endswith("/dags/my_dag/task?task_id=task_a&dag_run_id=dr1")


def test_build_log_url():
    url = build_airflow_ui_url(
        "data-stg", "log", "my_dag", dag_run_id="dr1", task_id="task_1", try_number=3
    )
    assert isinstance(url, str)
    assert "/dags/my_dag/dagRuns/dr1/taskInstances/task_1/logs/3" in url


def test_build_url_encodes_plus_in_dag_run_id():
    dag_run_id = "scheduled__2025-11-04T12:15:00+00:00"
    encoded = quote(dag_run_id, safe="")
    grid_url = build_airflow_ui_url("data-stg", "grid", "my_dag", dag_run_id=dag_run_id)
    assert f"dag_run_id={encoded}" in grid_url

    log_url = build_airflow_ui_url(
        "data-stg",
        "log",
        "my_dag",
        dag_run_id=dag_run_id,
        task_id="task_1",
        try_number=1,
    )
    assert f"/dagRuns/{encoded}/" in log_url


def test_resolve_and_validate_precedence(monkeypatch: pytest.MonkeyPatch):
    host = "https://airflow.data-stg.example.com"
    url = f"{host}/dags/d1/grid"

    ok = resolve_and_validate(ui_url=url, instance="data-stg")
    assert isinstance(ok, object) and getattr(ok, "instance", None) == "data-stg"

    with pytest.raises(AirflowToolError) as mismatch:
        resolve_and_validate(ui_url=url, instance="ml-stg")
    assert "INSTANCE_MISMATCH" in str(mismatch.value)
    assert mismatch.value.code == "INVALID_INPUT"

    with pytest.raises(AirflowToolError) as exc:
        resolve_and_validate(ui_url=None, instance=None)
    assert exc.value.code == "INVALID_INPUT"


def test_build_url_invalid_identifiers():
    # dag_id with newline should fail
    with pytest.raises(AirflowToolError) as exc1:
        build_airflow_ui_url("data-stg", "grid", "bad\ndag")
    assert exc1.value.code == "INVALID_INPUT"
    # dag_run_id with space should fail (strict pattern)
    with pytest.raises(AirflowToolError) as exc2:
        build_airflow_ui_url(
            "data-stg", "log", "dag", dag_run_id="dr one", task_id="task", try_number=1
        )
    assert exc2.value.code == "INVALID_INPUT"
