"""Tests for experimental Airflow 3 (API v2) support.

These tests emulate the apache-airflow-client 3.x codegen surface (top-level API
classes, /api/v2 paths baked into the client, JWT auth) using fakes whose method
names and signatures were verified against apache-airflow-client 3.2.2.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_tools_readonly import _payload

from airflow_mcp import client_factory as cf
from airflow_mcp import tools as airflow_tools
from airflow_mcp.errors import AirflowToolError
from airflow_mcp.registry import reset_registry_cache
from airflow_mcp.url_utils import build_airflow_ui_url, parse_airflow_ui_url


class _Obj:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def to_dict(self):
        return {k: v for k, v in self.__dict__.items() if not k.startswith("_")}


def _field(body, name):
    """Read a field from a request body that may be a dict or a model object."""
    if hasattr(body, "to_dict"):
        # Real pydantic models serialize anyOf wrappers back to plain values
        return body.to_dict().get(name)
    if isinstance(body, dict):
        return body.get(name)
    return getattr(body, name, None)


class _Configuration:
    def __init__(self, host: str, **kwargs) -> None:
        self.host = host
        self.access_token = kwargs.get("access_token")
        self.verify_ssl = True


class _ApiClient:
    def __init__(self, configuration) -> None:
        self.configuration = configuration
        self.calls: dict[str, object] = {}


class _DAGApi:
    def __init__(self, api_client) -> None:
        self._c = api_client

    def get_dags(self, limit=100, offset=0):
        return _Obj(dags=[_Obj(dag_id="dag_a", is_paused=False)], total_entries=1)

    def get_dag(self, dag_id):
        return _Obj(dag_id=dag_id, is_paused=False)

    def patch_dag(self, dag_id, dag_patch_body=None, update_mask=None):
        self._c.calls["patch_dag"] = {"dag_id": dag_id, "body": dag_patch_body}
        return _Obj(dag_id=dag_id, is_paused=_field(dag_patch_body, "is_paused"))


class _DagRunApi:
    def __init__(self, api_client) -> None:
        self._c = api_client

    def get_dag_runs(self, dag_id, **kwargs):
        # Mirror the 3.1+ codegen: order_by is Optional[List[StrictStr]]
        order_by = kwargs.get("order_by")
        if order_by is not None and not isinstance(order_by, list):
            raise ValueError("order_by must be a list of strings")
        self._c.calls["get_dag_runs"] = {"dag_id": dag_id, **kwargs}
        return _Obj(
            dag_runs=[
                _Obj(
                    dag_run_id="run_1",
                    state="success",
                    start_date="2026-01-01T00:00:00Z",
                    end_date="2026-01-01T01:00:00Z",
                    logical_date="2026-01-01T00:00:00Z",
                )
            ],
            total_entries=1,
        )

    def get_dag_run(self, dag_id, dag_run_id):
        return _Obj(dag_run_id=dag_run_id, state="failed")

    def trigger_dag_run(self, dag_id, trigger_dag_run_post_body=None):
        self._c.calls["trigger_dag_run"] = {
            "dag_id": dag_id,
            "body": trigger_dag_run_post_body,
        }
        return _Obj(dag_run_id="manual__triggered", state="queued")

    def clear_dag_run(self, dag_id, dag_run_id, dag_run_clear_body=None):
        self._c.calls["clear_dag_run"] = {
            "dag_id": dag_id,
            "dag_run_id": dag_run_id,
            "body": dag_run_clear_body,
        }
        return _Obj(task_instances=[], total_entries=0)


class _TaskInstanceApi:
    def __init__(self, api_client) -> None:
        self._c = api_client

    def get_task_instances(self, dag_id, dag_run_id, limit=100, offset=0, state=None, task_id=None):
        # Signature mirrors the 3.x codegen: `task_id` is a single Optional[StrictStr].
        if task_id is not None and not isinstance(task_id, str):
            raise ValueError("task_id must be a string")
        self._c.calls["get_task_instances"] = {
            "dag_id": dag_id,
            "dag_run_id": dag_run_id,
            "state": state,
            "task_id": task_id,
        }
        return _Obj(
            task_instances=[
                _Obj(task_id="extract", state="failed", try_number=2),
                _Obj(task_id="load", state="success", try_number=1),
            ],
            total_entries=2,
        )

    def get_task_instance(self, dag_id, dag_run_id, task_id):
        return _Obj(
            task_id=task_id,
            state="failed",
            try_number=2,
            start_date="2026-01-01T00:00:00Z",
            end_date="2026-01-01T00:10:00Z",
            hostname="worker-1",
            operator="PythonOperator",
            queue="default",
            pool="default_pool",
            priority_weight=1,
            rendered_fields={"bash_command": "echo hi"},
        )

    def get_log_without_preload_content(self, dag_id, dag_run_id, task_id, try_number):
        # Mirrors the real 3.x codegen: returns the raw HTTP response; the JSON body
        # carries structured fields (level, logger, ...) that the client's model
        # deserialization would strip.
        body = {
            "content": [
                {
                    "timestamp": "2026-01-01T00:00:00Z",
                    "event": "task started",
                    "level": "info",
                },
                {
                    "timestamp": "2026-01-01T00:00:01Z",
                    "event": "boom",
                    "level": "error",
                    "logger": "airflow.task",
                },
            ],
            "continuation_token": None,
        }
        return SimpleNamespace(status=200, reason="OK", data=json.dumps(body).encode("utf-8"))

    def post_clear_task_instances(self, dag_id, clear_task_instances_body=None):
        self._c.calls["post_clear_task_instances"] = {
            "dag_id": dag_id,
            "body": clear_task_instances_body,
        }
        return _Obj(task_instances=[], total_entries=0)


class _TaskApi:
    def __init__(self, api_client) -> None:
        self._c = api_client

    def get_task(self, dag_id, task_id):
        return _Obj(task_id=task_id, retries=3, retry_delay="0:05:00", owner="data-eng")


class _AssetApi:
    def __init__(self, api_client) -> None:
        self._c = api_client

    def get_assets(self, uri_pattern=None, limit=100, offset=0):
        if offset:
            return _Obj(assets=[], total_entries=1)
        return _Obj(
            assets=[_Obj(id=42, uri="s3://bucket/data", name="data")],
            total_entries=1,
        )

    def get_asset_events(self, asset_id=None, limit=50):
        self._c.calls["get_asset_events"] = {"asset_id": asset_id, "limit": limit}
        return _Obj(
            asset_events=[_Obj(asset_id=asset_id, extra={"rows": 10})],
            total_entries=1,
        )


_FAKE_V3_MODULE = SimpleNamespace(
    Configuration=_Configuration,
    ApiClient=_ApiClient,
    DAGApi=_DAGApi,
    DagRunApi=_DagRunApi,
    TaskInstanceApi=_TaskInstanceApi,
    TaskApi=_TaskApi,
    AssetApi=_AssetApi,
)


@pytest.fixture()
def v2_instance(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> str:
    yaml_text = (
        "af3:\n"
        "  host: https://airflow3.example.com/\n"
        "  api_version: v2\n"
        "  verify_ssl: true\n"
        "  auth:\n"
        "    type: bearer\n"
        "    token: jwt-token-123\n"
    )
    p = tmp_path / "instances.yaml"
    p.write_text(yaml_text, encoding="utf-8")
    monkeypatch.setenv("AIRFLOW_MCP_INSTANCES_FILE", str(p))
    monkeypatch.delenv("AIRFLOW_MCP_DEFAULT_INSTANCE", raising=False)
    reset_registry_cache()
    monkeypatch.setattr(cf, "_import_airflow_client_v3", lambda: _FAKE_V3_MODULE)
    cf.get_client_factory()._cache.clear()
    yield "af3"
    cf.get_client_factory()._cache.clear()
    reset_registry_cache()


def test_api_family_detection(v2_instance: str):
    factory = cf.get_client_factory()
    assert factory.get_api_family(v2_instance) == "v2"


def test_base_url_has_no_api_suffix_for_v2(v2_instance: str):
    factory = cf.get_client_factory()
    client = factory.get_api_client(v2_instance)
    # 3.x client paths already include /api/v2; host must stay bare
    assert client.configuration.host == "https://airflow3.example.com"
    assert client.configuration.access_token == "jwt-token-123"


def test_v2_with_2x_client_raises_actionable_error(
    v2_instance: str, monkeypatch: pytest.MonkeyPatch
):
    # A 2.x-shaped module has no top-level DAGApi
    monkeypatch.setattr(cf, "_import_airflow_client_v3", lambda: SimpleNamespace())
    factory = cf.get_client_factory()
    factory._cache.clear()
    with pytest.raises(AirflowToolError) as exc:
        factory.get_api_client(v2_instance)
    assert exc.value.code == "CONFIG_ERROR"
    assert "apache-airflow-client>=3" in str(exc.value)


def test_basic_auth_exchanges_credentials_for_jwt(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    yaml_text = (
        "af3-basic:\n"
        "  host: https://airflow3.example.com\n"
        "  api_version: v2\n"
        "  auth:\n"
        "    type: basic\n"
        "    username: admin\n"
        "    password: secret\n"
    )
    p = tmp_path / "instances.yaml"
    p.write_text(yaml_text, encoding="utf-8")
    monkeypatch.setenv("AIRFLOW_MCP_INSTANCES_FILE", str(p))
    reset_registry_cache()
    monkeypatch.setattr(cf, "_import_airflow_client_v3", lambda: _FAKE_V3_MODULE)

    fetched: dict[str, object] = {}

    def _fake_fetch(host, username, password, *, verify_ssl, timeout):
        fetched.update(host=host, username=username, password=password, verify_ssl=verify_ssl)
        return "jwt-from-login"

    monkeypatch.setattr(cf, "_fetch_jwt_token", _fake_fetch)
    factory = cf.get_client_factory()
    factory._cache.clear()
    try:
        client = factory.get_api_client("af3-basic")
        assert client.configuration.access_token == "jwt-from-login"
        assert fetched["username"] == "admin"
        assert fetched["host"] == "https://airflow3.example.com"
    finally:
        factory._cache.clear()
        reset_registry_cache()


def test_trigger_dag_uses_v2_endpoint(v2_instance: str):
    out = airflow_tools.trigger_dag(
        instance=v2_instance, dag_id="etl", conf={"key": "val"}, note="incident"
    )
    payload = _payload(out)
    assert payload["dag_run_id"] == "manual__triggered"
    factory = cf.get_client_factory()
    client = factory.get_api_client(v2_instance)
    call = client.calls["trigger_dag_run"]
    assert call["dag_id"] == "etl"
    assert _field(call["body"], "conf") == {"key": "val"}
    assert _field(call["body"], "note") == "incident"
    # Airflow 3 UI URL shape
    assert payload["ui_url"] == ("https://airflow3.example.com/dags/etl/runs/manual__triggered")


def test_pause_dag_uses_dag_patch_body(v2_instance: str):
    out = airflow_tools.pause_dag(instance=v2_instance, dag_id="etl")
    payload = _payload(out)
    assert payload["is_paused"] is True
    client = cf.get_client_factory().get_api_client(v2_instance)
    assert _field(client.calls["patch_dag"]["body"], "is_paused") is True


def test_clear_dag_run_uses_v2_body(v2_instance: str):
    out = airflow_tools.clear_dag_run(
        instance=v2_instance, dag_id="etl", dag_run_id="run_1", dry_run=True
    )
    payload = _payload(out)
    assert payload["dag_id"] == "etl"
    client = cf.get_client_factory().get_api_client(v2_instance)
    call = client.calls["clear_dag_run"]
    assert _field(call["body"], "dry_run") is True


def test_clear_task_instances_moved_to_task_instance_api(v2_instance: str):
    out = airflow_tools.clear_task_instances(
        instance=v2_instance, dag_id="etl", task_ids=["extract"], dry_run=True
    )
    payload = _payload(out)
    assert payload["dag_id"] == "etl"
    client = cf.get_client_factory().get_api_client(v2_instance)
    call = client.calls["post_clear_task_instances"]
    assert _field(call["body"], "task_ids") == ["extract"]
    assert _field(call["body"], "dry_run") is True


def test_clear_dag_run_rejects_unsupported_options_on_v2(v2_instance: str):
    with pytest.raises(AirflowToolError) as exc:
        airflow_tools.clear_dag_run(
            instance=v2_instance,
            dag_id="etl",
            dag_run_id="run_1",
            include_downstream=True,
            dry_run=False,
        )
    assert exc.value.code == "INVALID_INPUT"
    assert "include_downstream" in str(exc.value)


def test_clear_task_instances_rejects_subdag_options_on_v2(v2_instance: str):
    with pytest.raises(AirflowToolError) as exc:
        airflow_tools.clear_task_instances(instance=v2_instance, dag_id="etl", include_subdags=True)
    assert exc.value.code == "INVALID_INPUT"


def test_list_task_instances_forwards_single_task_id_filter(v2_instance: str):
    # The 3.x codegen exposes `task_id` as a single strict str; a list must not be
    # forwarded (it would fail pydantic validation on the real client).
    out = airflow_tools.list_task_instances(
        instance=v2_instance,
        dag_id="etl",
        dag_run_id="run_1",
        task_ids=["extract"],
    )
    payload = _payload(out)
    assert payload["count"] == 1
    assert payload["task_instances"][0]["task_id"] == "extract"
    client = cf.get_client_factory().get_api_client(v2_instance)
    assert client.calls["get_task_instances"]["task_id"] == "extract"


def test_list_task_instances_multi_task_ids_filtered_client_side(v2_instance: str):
    out = airflow_tools.list_task_instances(
        instance=v2_instance,
        dag_id="etl",
        dag_run_id="run_1",
        task_ids=["extract", "load"],
    )
    payload = _payload(out)
    assert payload["count"] == 2
    client = cf.get_client_factory().get_api_client(v2_instance)
    # Multi-value filter must not be forwarded to the single-value param
    assert client.calls["get_task_instances"]["task_id"] is None


def test_trigger_body_always_includes_logical_date(v2_instance: str):
    airflow_tools.trigger_dag(instance=v2_instance, dag_id="etl")
    client = cf.get_client_factory().get_api_client(v2_instance)
    body = client.calls["trigger_dag_run"]["body"]
    # Airflow 3.0.x requires the key to be present (nullable); it must not be stripped
    if isinstance(body, dict):
        assert "logical_date" in body and body["logical_date"] is None
    else:
        assert hasattr(body, "logical_date")


def test_list_dag_runs_maps_execution_date_to_logical_date(v2_instance: str):
    out = airflow_tools.list_dag_runs(
        instance=v2_instance, dag_id="etl", order_by="execution_date", descending=True
    )
    payload = _payload(out)
    assert payload["count"] == 1
    client = cf.get_client_factory().get_api_client(v2_instance)
    # 3.1+ codegen expects order_by as a list
    assert client.calls["get_dag_runs"]["order_by"] == ["-logical_date"]


def test_get_task_instance_logs_normalizes_structured_content(v2_instance: str):
    out = airflow_tools.get_task_instance_logs(
        instance=v2_instance,
        dag_id="etl",
        dag_run_id="run_1",
        task_id="extract",
        try_number=2,
    )
    payload = _payload(out)
    assert "task started" in payload["log"]
    assert "boom" in payload["log"]
    # Structured entries render one per line with timestamp + level + event + extras
    lines = payload["log"].splitlines()
    assert len(lines) == 2
    assert "ERROR" in lines[1]
    assert "logger=airflow.task" in lines[1]


def test_get_task_instance_logs_filter_level_matches_structured_level(v2_instance: str):
    # The error line's text has no ERROR token; only the structured `level` field
    # marks it. Filtering must still find it (regression: the client's model
    # deserialization used to strip `level` before formatting).
    out = airflow_tools.get_task_instance_logs(
        instance=v2_instance,
        dag_id="etl",
        dag_run_id="run_1",
        task_id="extract",
        try_number=2,
        filter_level="error",
    )
    payload = _payload(out)
    assert payload["match_count"] == 1
    assert "boom" in payload["log"]
    assert "task started" not in payload["log"]


def test_get_task_instance_uses_task_api_for_config(v2_instance: str):
    out = airflow_tools.get_task_instance(
        instance=v2_instance, dag_id="etl", dag_run_id="run_1", task_id="extract"
    )
    payload = _payload(out)
    assert payload["task_config"]["retries"] == 3
    assert payload["task_config"]["owner"] == "data-eng"
    assert payload["attempts"]["try_number"] == 2
    # Airflow 3 task UI URL
    assert payload["ui_url"]["log"] == (
        "https://airflow3.example.com/dags/etl/runs/run_1/tasks/extract?try_number=2"
    )


def test_dataset_events_resolves_asset_by_uri(v2_instance: str):
    out = airflow_tools.dataset_events(
        instance=v2_instance, dataset_uri="s3://bucket/data", limit=10
    )
    payload = _payload(out)
    assert payload["count"] == 1
    client = cf.get_client_factory().get_api_client(v2_instance)
    assert client.calls["get_asset_events"]["asset_id"] == 42


def test_build_ui_urls_v2(v2_instance: str):
    assert (
        build_airflow_ui_url(v2_instance, "grid", "etl") == "https://airflow3.example.com/dags/etl"
    )
    assert (
        build_airflow_ui_url(v2_instance, "dag_run", "etl", dag_run_id="run_1")
        == "https://airflow3.example.com/dags/etl/runs/run_1"
    )
    assert build_airflow_ui_url(
        v2_instance, "task", "etl", dag_run_id="run_1", task_id="extract"
    ) == ("https://airflow3.example.com/dags/etl/runs/run_1/tasks/extract")


def test_parse_airflow3_ui_urls(v2_instance: str):
    parsed = parse_airflow_ui_url("https://airflow3.example.com/dags/etl/runs/run_1")
    assert parsed.instance == v2_instance
    assert parsed.route == "dag_run"
    assert parsed.dag_id == "etl"
    assert parsed.dag_run_id == "run_1"

    parsed_task = parse_airflow_ui_url(
        "https://airflow3.example.com/dags/etl/runs/run_1/tasks/extract?try_number=2"
    )
    assert parsed_task.route == "task"
    assert parsed_task.task_id == "extract"
    assert parsed_task.try_number == 2

    parsed_dag = parse_airflow_ui_url("https://airflow3.example.com/dags/etl")
    assert parsed_dag.route == "grid"
    assert parsed_dag.dag_id == "etl"
