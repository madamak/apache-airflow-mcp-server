from pathlib import Path

import pytest
from test_tools_readonly import _install_fake_airflow_client, _payload

from airflow_mcp import tools as airflow_tools
from airflow_mcp.client_factory import get_client_factory
from airflow_mcp.errors import AirflowToolError


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch):
    _install_fake_airflow_client(monkeypatch)
    get_client_factory()._cache.clear()
    examples = Path(__file__).resolve().parent.parent / "examples" / "instances.yaml"
    monkeypatch.setenv("AIRFLOW_MCP_INSTANCES_FILE", str(examples))
    monkeypatch.setenv("AIRFLOW_INSTANCE_DATA_STG_USERNAME", "u")
    monkeypatch.setenv("AIRFLOW_INSTANCE_DATA_STG_PASSWORD", "p")
    monkeypatch.setenv("AIRFLOW_INSTANCE_ML_STG_USERNAME", "u")
    monkeypatch.setenv("AIRFLOW_INSTANCE_ML_STG_PASSWORD", "p")
    yield


def _get_cached_api_client(instance: str):
    # Access cached ApiClient from the factory for assertions
    return get_client_factory().get_api_client(instance)


def test_trigger_dag_with_conf_and_logical_date():
    out = airflow_tools.trigger_dag(
        instance="data-stg",
        dag_id="dag_a",
        dag_run_id="manual__001",
        logical_date="2025-01-01T00:00:00Z",
        conf='{"foo": "bar"}',
        note="triggered from test",
    )
    payload = _payload(out)
    assert payload["dag_run_id"] == "manual__001"
    assert payload["ui_url"].endswith("/dags/dag_a/dagRuns/manual__001")
    client = _get_cached_api_client("data-stg")
    dag_run_model = client.last_post_dag_run
    assert getattr(dag_run_model, "dag_run_id", None) == "manual__001"
    assert getattr(dag_run_model, "logical_date", None) == "2025-01-01T00:00:00Z"
    conf_value = getattr(dag_run_model, "conf", None)
    assert conf_value == {"foo": "bar"}
    assert getattr(dag_run_model, "note", None) == "triggered from test"


def test_trigger_dag_invalid_conf_raises():
    with pytest.raises(AirflowToolError) as exc:
        airflow_tools.trigger_dag(instance="data-stg", dag_id="dag_a", conf="not-json")
    assert exc.value.code == "INVALID_INPUT"


def test_trigger_dag_invalid_logical_date():
    with pytest.raises(AirflowToolError) as exc:
        airflow_tools.trigger_dag(
            instance="data-stg", dag_id="dag_a", logical_date="2025-13-01"
        )
    assert exc.value.code == "INVALID_INPUT"


def test_clear_task_instances_normalizes_payload():
    out = airflow_tools.clear_task_instances(
        instance="data-stg",
        dag_id="dag_a",
        task_ids=["task_a", "task_b"],
        include_upstream=True,
        dry_run=True,
    )
    payload = _payload(out)
    assert payload["dag_id"] == "dag_a"
    cleared = payload["cleared"]
    assert cleared["task_ids"] == ["task_a", "task_b"]
    assert cleared["include_upstream"] is True
    assert cleared["dry_run"] is True
    client = _get_cached_api_client("data-stg")
    clear_model = client.last_clear_task_instances
    assert getattr(clear_model, "task_ids", None) == ["task_a", "task_b"]
    assert getattr(clear_model, "dry_run", None) is True


def test_clear_task_instances_invalid_ids():
    with pytest.raises(AirflowToolError) as exc:
        airflow_tools.clear_task_instances(
            instance="data-stg", dag_id="dag_a", task_ids="not-a-list"
        )
    assert exc.value.code == "INVALID_INPUT"


def test_clear_dag_run_clears_all_tasks():
    """Test clear_dag_run with default config (extended params disabled for 2.5.x compat)."""
    out = airflow_tools.clear_dag_run(
        instance="data-stg",
        dag_id="dag_a",
        dag_run_id="dr1",
        include_upstream=True,
        dry_run=False,
    )
    payload = _payload(out)
    assert payload["dag_id"] == "dag_a"
    assert payload["dag_run_id"] == "dr1"
    cleared = payload["cleared"]
    assert isinstance(cleared, dict)
    client = _get_cached_api_client("data-stg")
    clear_model = client.last_clear_dag_run
    # With default config (extended params disabled), include_upstream should NOT be sent
    if isinstance(clear_model, dict):
        assert "include_upstream" not in clear_model
        assert clear_model.get("dry_run") is False
    else:
        assert (
            not hasattr(clear_model, "include_upstream")
            or getattr(clear_model, "include_upstream", None) is None
        )
        assert getattr(clear_model, "dry_run", None) is False


def test_clear_dag_run_with_extended_params(monkeypatch: pytest.MonkeyPatch):
    """Test clear_dag_run with extended params enabled (Airflow ≥2.6 mode)."""
    from airflow_mcp import config as airflow_config

    monkeypatch.setattr(airflow_config.config, "enable_extended_clear_params", True)

    out = airflow_tools.clear_dag_run(
        instance="data-stg",
        dag_id="dag_a",
        dag_run_id="dr1",
        include_upstream=True,
        include_downstream=False,
        dry_run=False,
    )
    payload = _payload(out)
    assert payload["dag_id"] == "dag_a"
    assert payload["dag_run_id"] == "dr1"
    cleared = payload["cleared"]
    assert isinstance(cleared, dict)
    client = _get_cached_api_client("data-stg")
    clear_model = client.last_clear_dag_run
    # With extended params enabled, parameters should be passed through
    if isinstance(clear_model, dict):
        assert clear_model.get("include_upstream") is True
        assert clear_model.get("include_downstream") is False
        assert clear_model.get("dry_run") is False
    else:
        assert getattr(clear_model, "include_upstream", None) is True
        assert getattr(clear_model, "include_downstream", None) is False
        assert getattr(clear_model, "dry_run", None) is False


def test_pause_and_unpause_dag():
    pause_out = _payload(airflow_tools.pause_dag(instance="data-stg", dag_id="dag_a"))
    assert pause_out["is_paused"] is True
    assert pause_out["ui_url"].endswith("/dags/dag_a/grid")
    client = _get_cached_api_client("data-stg")
    patch_payload = client.last_patch_dag
    assert patch_payload["update_mask"] == ["is_paused"]
    dag_model = patch_payload["dag"]
    assert getattr(dag_model, "is_paused", None) is True

    unpause_out = _payload(airflow_tools.unpause_dag(instance="data-stg", dag_id="dag_a"))
    assert unpause_out["is_paused"] is False
    patch_payload = client.last_patch_dag
    dag_model = patch_payload["dag"]
    assert getattr(dag_model, "is_paused", None) is False
