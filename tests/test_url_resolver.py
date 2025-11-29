from urllib.parse import quote

import pytest
from test_tools_readonly import _payload

from airflow_mcp import tools as airflow_tools
from airflow_mcp.errors import AirflowToolError


@pytest.fixture(autouse=True)
def _set_registry(monkeypatch: pytest.MonkeyPatch):
    # Minimal registry with two hosts
    monkeypatch.setenv(
        "AIRFLOW_MCP_INSTANCES_FILE",
        __file__.replace("test_url_resolver.py", "../examples/instances.yaml"),
    )
    # Provide envs for example placeholders, if any are present
    monkeypatch.setenv("AIRFLOW_INSTANCE_DATA_STG_USERNAME", "u")
    monkeypatch.setenv("AIRFLOW_INSTANCE_DATA_STG_PASSWORD", "p")
    monkeypatch.setenv("AIRFLOW_INSTANCE_ML_STG_USERNAME", "u")
    monkeypatch.setenv("AIRFLOW_INSTANCE_ML_STG_PASSWORD", "p")


def test_resolve_grid_url(monkeypatch: pytest.MonkeyPatch):
    host = "https://airflow.data-stg.example.com"
    url = f"{host}/dags/my_dag/grid?dag_run_id=scheduled__2025-01-15"
    out = airflow_tools.resolve_url(url)
    payload = _payload(out)
    assert payload["route"] == "grid"
    assert payload["dag_id"] == "my_dag"
    assert payload["dag_run_id"] == "scheduled__2025-01-15"


def test_resolve_grid_url_with_plus_in_dag_run(monkeypatch: pytest.MonkeyPatch):
    host = "https://airflow.data-stg.example.com"
    dag_run_id = "scheduled__2025-11-04T12:15:00+00:00"
    encoded = quote(dag_run_id, safe="")
    url = f"{host}/dags/my_dag/grid?dag_run_id={encoded}"
    out = airflow_tools.resolve_url(url)
    payload = _payload(out)
    assert payload["route"] == "grid"
    assert payload["dag_run_id"] == dag_run_id


def test_resolve_dag_run_url(monkeypatch: pytest.MonkeyPatch):
    host = "https://airflow.data-stg.example.com"
    url = f"{host}/dags/my_dag/dagRuns/scheduled__2025-01-15"
    out = airflow_tools.resolve_url(url)
    payload = _payload(out)
    assert payload["route"] == "dag_run"
    assert payload["dag_run_id"] == "scheduled__2025-01-15"


def test_resolve_task_url(monkeypatch: pytest.MonkeyPatch):
    host = "https://airflow.data-stg.example.com"
    url = f"{host}/dags/my_dag/task?task_id=t1&dag_run_id=dr1"
    out = airflow_tools.resolve_url(url)
    payload = _payload(out)
    assert payload["route"] == "task"
    assert payload["task_id"] == "t1"
    assert payload["dag_run_id"] == "dr1"


def test_resolve_logs_url(monkeypatch: pytest.MonkeyPatch):
    host = "https://airflow.data-stg.example.com"
    url = f"{host}/dags/my_dag/dagRuns/dr1/taskInstances/task_a/logs/2"
    out = airflow_tools.resolve_url(url)
    payload = _payload(out)
    assert payload["route"] == "log"
    assert payload["task_id"] == "task_a"
    assert payload["try_number"] == 2


def test_resolver_errors(monkeypatch: pytest.MonkeyPatch):
    # invalid scheme
    with pytest.raises(AirflowToolError) as exc1:
        airflow_tools.resolve_url("ftp://host/path")
    assert exc1.value.code == "INVALID_INPUT"

    with pytest.raises(AirflowToolError) as exc_missing_scheme:
        airflow_tools.resolve_url("airflow-2")
    assert exc_missing_scheme.value.code == "INVALID_INPUT"
    assert "full http(s) URL" in str(exc_missing_scheme.value)

    # unknown host
    with pytest.raises(AirflowToolError) as exc2:
        airflow_tools.resolve_url("https://unknown.example/dags/d/grid")
    assert exc2.value.code == "NOT_FOUND"

    # invalid dag identifier (with control character)
    host = "https://airflow.data-stg.example.com"
    with pytest.raises(AirflowToolError) as exc3:
        airflow_tools.resolve_url(f"{host}/dags/bad%0Adag/grid")
    assert exc3.value.code == "INVALID_INPUT"


def test_resolve_url_with_encoded_identifiers(monkeypatch: pytest.MonkeyPatch):
    """Test that URL-encoded identifiers are properly decoded."""
    host = "https://airflow.data-stg.example.com"

    # DAG ID with encoded dot
    url = f"{host}/dags/my%2Edag/grid"
    out = airflow_tools.resolve_url(url)
    payload = _payload(out)
    assert payload["dag_id"] == "my.dag"
    assert payload["route"] == "grid"

    # Task ID with special characters (+ encoded as %2B)
    url2 = f"{host}/dags/my_dag/dagRuns/scheduled__2025-01-15/taskInstances/task%2Bspecial/logs/1"
    out2 = airflow_tools.resolve_url(url2)
    payload2 = _payload(out2)
    assert payload2["task_id"] == "task+special"
    assert payload2["try_number"] == 1
    assert payload2["route"] == "log"

    # Query parameters with encoded values
    url3 = f"{host}/dags/my_dag/task?task_id=task%2Esuffix&dag_run_id=dr%3Aencoded"
    out3 = airflow_tools.resolve_url(url3)
    payload3 = _payload(out3)
    assert payload3["task_id"] == "task.suffix"
    assert payload3["dag_run_id"] == "dr:encoded"
    assert payload3["route"] == "task"
