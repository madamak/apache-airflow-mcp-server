"""Tests for experimental Airflow 3 (API v2) support.

These tests emulate the apache-airflow-client 3.x codegen surface (top-level API
classes, /api/v2 paths baked into the client, JWT auth) using fakes whose method
names and signatures were verified against apache-airflow-client 3.2.2.
"""

from __future__ import annotations

import base64
import json
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_tools_readonly import _payload

from airflow_mcp import client_factory as cf
from airflow_mcp import tools as airflow_tools
from airflow_mcp.errors import AirflowToolError
from airflow_mcp.registry import reset_registry_cache
from airflow_mcp.tools import _common
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

    def clear_dag_run_without_preload_content(self, dag_id, dag_run_id, dag_run_clear_body=None):
        self._c.calls["clear_dag_run"] = {
            "dag_id": dag_id,
            "dag_run_id": dag_run_id,
            "body": dag_run_clear_body,
            "raw": True,
        }
        body = {"task_instances": [], "total_entries": 0}
        return SimpleNamespace(status=200, reason="OK", data=json.dumps(body).encode("utf-8"))


class _TaskInstanceApi:
    def __init__(self, api_client) -> None:
        self._c = api_client

    def get_task_instances(self, dag_id, dag_run_id, limit=100, offset=0, state=None, task_id=None):
        # Signature mirrors the 3.x codegen: `task_id` is a single Optional[StrictStr].
        if task_id is not None and not isinstance(task_id, str):
            raise ValueError("task_id must be a string")
        call = {
            "dag_id": dag_id,
            "dag_run_id": dag_run_id,
            "state": state,
            "task_id": task_id,
        }
        self._c.calls["get_task_instances"] = call
        self._c.calls.setdefault("get_task_instances_all", []).append(call)
        rows = [
            _Obj(task_id="extract", state="failed", try_number=2),
            _Obj(task_id="load", state="success", try_number=1),
        ]
        if task_id is not None:
            rows = [r for r in rows if r.task_id == task_id]
        return _Obj(task_instances=rows[offset : offset + limit], total_entries=len(rows))

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

    def get_log_without_preload_content(
        self, dag_id, dag_run_id, task_id, try_number, full_content=None
    ):
        # Mirrors the real 3.x codegen: returns the raw HTTP response; the JSON body
        # carries structured fields (level, logger, ...) that the client's model
        # deserialization would strip.
        self._c.calls["get_log_without_preload_content"] = {"full_content": full_content}
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

    def get_assets(self, uri_pattern=None, limit=100, offset=0, only_active=None):
        self._c.calls["get_assets"] = {"uri_pattern": uri_pattern, "only_active": only_active}
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


def _make_jwt(exp_epoch: float) -> str:
    header = base64.urlsafe_b64encode(b'{"alg":"HS256"}').rstrip(b"=").decode()
    payload = (
        base64.urlsafe_b64encode(json.dumps({"exp": exp_epoch}).encode()).rstrip(b"=").decode()
    )
    return f"{header}.{payload}.signature"


def test_token_refresh_deadline_honors_jwt_exp():
    # exp in 5 minutes: refresh must be scheduled before it, not at the 3600s default
    short_lived = _make_jwt(time.time() + 300)
    deadline = cf._token_refresh_deadline(short_lived)
    assert deadline - time.monotonic() <= 300 - cf._TOKEN_REFRESH_MARGIN_SECONDS + 1
    # opaque (non-JWT) token: fall back to the configured interval
    opaque = cf._token_refresh_deadline("not-a-jwt")
    assert opaque - time.monotonic() > 300


def test_token_refresh_deadline_short_lived_token_stays_before_expiry():
    # Token lifetime (20s) is shorter than the refresh margin: the deadline must
    # still land before exp instead of being floored past it.
    token = _make_jwt(time.time() + 20)
    deadline = cf._token_refresh_deadline(token)
    remaining = deadline - time.monotonic()
    assert 0 < remaining < 20


@pytest.fixture()
def v2_basic_factory(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
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
    monkeypatch.setattr(cf, "_fetch_jwt_token", lambda *a, **k: "initial-jwt")
    factory = cf.get_client_factory()
    factory._cache.clear()
    yield factory
    factory._cache.clear()
    reset_registry_cache()


def test_jwt_refresh_credential_rejection_fails_fast(
    v2_basic_factory, monkeypatch: pytest.MonkeyPatch
):
    factory = v2_basic_factory
    factory.get_api_client("af3-basic")
    bundle = factory._cache["af3-basic"]
    bundle.token_expires_at = time.monotonic() - 1

    def _rejected(*a, **k):
        raise AirflowToolError("bad credentials", code="AUTH_FAILED", context={"status": 401})

    monkeypatch.setattr(cf, "_fetch_jwt_token", _rejected)
    with pytest.raises(AirflowToolError) as excinfo:
        factory.get_api_client("af3-basic")
    assert excinfo.value.code == "AUTH_FAILED"
    # Backoff still applies so the endpoint is not hammered on every call
    assert bundle.token_expires_at > time.monotonic()


def test_jwt_refresh_transient_failure_keeps_previous_token(
    v2_basic_factory, monkeypatch: pytest.MonkeyPatch
):
    factory = v2_basic_factory
    factory.get_api_client("af3-basic")
    bundle = factory._cache["af3-basic"]
    bundle.token_expires_at = time.monotonic() - 1

    def _unreachable(*a, **k):
        raise AirflowToolError("connection refused", code="AUTH_FAILED")

    monkeypatch.setattr(cf, "_fetch_jwt_token", _unreachable)
    client = factory.get_api_client("af3-basic")
    assert client.configuration.access_token == "initial-jwt"
    assert bundle.token_expires_at > time.monotonic()


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
    assert call["raw"] is True


def test_clear_dag_run_raw_404_is_normalized_and_releases_connection(
    v2_instance: str, monkeypatch: pytest.MonkeyPatch
):
    released = False

    class RawResponse:
        status = 404
        reason = "Not Found"
        data = b'{"detail":"missing run"}'

        def release_conn(self):
            nonlocal released
            released = True

    monkeypatch.setattr(
        _DagRunApi,
        "clear_dag_run_without_preload_content",
        lambda self, *args, **kwargs: RawResponse(),
    )
    with pytest.raises(AirflowToolError) as exc:
        airflow_tools.clear_dag_run(
            instance=v2_instance, dag_id="etl", dag_run_id="missing", dry_run=False
        )
    assert exc.value.code == "NOT_FOUND"
    assert exc.value.context["status"] == 404
    assert released is True


def test_clear_dag_run_invalid_raw_json_is_masked_and_releases_connection(
    v2_instance: str, monkeypatch: pytest.MonkeyPatch
):
    released = False

    class RawResponse:
        status = 200
        reason = "OK"
        data = b"not-json"

        def release_conn(self):
            nonlocal released
            released = True

    monkeypatch.setattr(
        _DagRunApi,
        "clear_dag_run_without_preload_content",
        lambda self, *args, **kwargs: RawResponse(),
    )
    with pytest.raises(AirflowToolError) as exc:
        airflow_tools.clear_dag_run(
            instance=v2_instance, dag_id="etl", dag_run_id="run_1", dry_run=True
        )
    assert exc.value.code == "INTERNAL_ERROR"
    assert "invalid JSON" in str(exc.value)
    assert released is True


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


def test_clear_task_ids_wrapper_is_used_when_available(monkeypatch: pytest.MonkeyPatch):
    wrapped: list[str] = []

    class Wrapper:
        def __init__(self, *, actual_instance: str) -> None:
            wrapped.append(actual_instance)

    monkeypatch.setattr(
        _common,
        "import_module",
        lambda _name: SimpleNamespace(ClearTaskInstancesBodyTaskIdsInner=Wrapper),
    )
    monkeypatch.setattr(_common, "_build_v3_model", lambda _module, _class, payload: payload)

    body = _common._build_clear_task_instances_body(task_ids=["extract", "load"])

    assert wrapped == ["extract", "load"]
    assert all(isinstance(item, Wrapper) for item in body["task_ids"])


def test_clear_task_ids_fall_back_only_when_wrapper_is_absent(
    monkeypatch: pytest.MonkeyPatch,
):
    def missing_wrapper(_name: str):
        raise ModuleNotFoundError

    monkeypatch.setattr(_common, "import_module", missing_wrapper)
    monkeypatch.setattr(_common, "_build_v3_model", lambda _module, _class, payload: payload)

    body = _common._build_clear_task_instances_body(task_ids=["extract"])

    assert body["task_ids"] == ["extract"]


def test_clear_task_ids_wrapper_validation_errors_propagate(
    monkeypatch: pytest.MonkeyPatch,
):
    class BrokenWrapper:
        def __init__(self, *, actual_instance: str) -> None:
            raise ValueError(actual_instance)

    monkeypatch.setattr(
        _common,
        "import_module",
        lambda _name: SimpleNamespace(ClearTaskInstancesBodyTaskIdsInner=BrokenWrapper),
    )

    with pytest.raises(ValueError, match="extract"):
        _common._build_clear_task_instances_body(task_ids=["extract"])


def test_clear_operations_default_to_dry_run_on_v2(v2_instance: str):
    run_payload = _payload(
        airflow_tools.clear_dag_run(instance=v2_instance, dag_id="etl", dag_run_id="run_1")
    )
    task_payload = _payload(
        airflow_tools.clear_task_instances(
            instance=v2_instance,
            dag_id="etl",
            task_ids=["extract"],
        )
    )
    client = cf.get_client_factory().get_api_client(v2_instance)
    assert _field(client.calls["clear_dag_run"]["body"], "dry_run") is True
    assert _field(client.calls["post_clear_task_instances"]["body"], "dry_run") is True
    assert run_payload["dry_run"] is True
    assert task_payload["dry_run"] is True


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


def test_list_task_instances_multi_task_ids_queried_per_task(v2_instance: str):
    out = airflow_tools.list_task_instances(
        instance=v2_instance,
        dag_id="etl",
        dag_run_id="run_1",
        task_ids=["extract", "load"],
    )
    payload = _payload(out)
    assert payload["count"] == 2
    assert {ti["task_id"] for ti in payload["task_instances"]} == {"extract", "load"}
    client = cf.get_client_factory().get_api_client(v2_instance)
    # Each requested task_id is queried server-side via the single-value param
    queried = [c["task_id"] for c in client.calls["get_task_instances_all"]]
    assert queried == ["extract", "load"]


def test_list_task_instances_multi_task_ids_finds_matches_beyond_first_page(
    v2_instance: str, monkeypatch: pytest.MonkeyPatch
):
    # A server that ignores the task_id filter still returns paged results;
    # a match beyond the first server page (row 151 here) must still be found
    # and rows must not be double-counted across the per-task queries.
    rows = [_Obj(task_id=f"task_{i:03d}", state="success", try_number=1) for i in range(150)]
    rows.append(_Obj(task_id="needle", state="failed", try_number=1))

    def paged(self, dag_id, dag_run_id, limit=100, offset=0, state=None, task_id=None):
        return _Obj(task_instances=rows[offset : offset + limit], total_entries=len(rows))

    monkeypatch.setattr(_TaskInstanceApi, "get_task_instances", paged)
    payload = _payload(
        airflow_tools.list_task_instances(
            instance=v2_instance,
            dag_id="etl",
            dag_run_id="run_1",
            task_ids=["task_000", "needle"],
        )
    )
    assert payload["count"] == 2
    assert {ti["task_id"] for ti in payload["task_instances"]} == {"task_000", "needle"}


def test_list_task_instances_scan_ceiling_raises_result_incomplete(
    v2_instance: str, monkeypatch: pytest.MonkeyPatch
):
    from airflow_mcp.tools import tasks as tasks_mod

    # No `state`/`task_id` params: forces the generic in-memory scan for a
    # state filter that never matches, so only the row ceiling can stop it.
    def endless(self, dag_id, dag_run_id, limit=100, offset=0):
        rows = [_Obj(task_id=f"t{offset + i}", state="success", try_number=1) for i in range(limit)]
        return _Obj(task_instances=rows, total_entries=100_000)

    monkeypatch.setattr(_TaskInstanceApi, "get_task_instances", endless)
    monkeypatch.setattr(tasks_mod, "_MAX_FILTER_SCAN_ROWS", 300)
    with pytest.raises(AirflowToolError) as exc:
        airflow_tools.list_task_instances(
            instance=v2_instance, dag_id="etl", dag_run_id="run_1", state=["failed"]
        )
    assert exc.value.code == "RESULT_INCOMPLETE"
    assert exc.value.context["scan_ceiling"] == 300


def test_list_task_instances_scan_exact_ceiling_boundary_is_complete(
    v2_instance: str, monkeypatch: pytest.MonkeyPatch
):
    from airflow_mcp.tools import tasks as tasks_mod

    # Run size == scan ceiling, on an exact page boundary: the reported total
    # marks the scan as exhausted, so this must succeed, not RESULT_INCOMPLETE.
    total = 300
    rows = [_Obj(task_id=f"t{i}", state="success", try_number=1) for i in range(total - 1)]
    rows.append(_Obj(task_id="last", state="failed", try_number=1))

    def paged(self, dag_id, dag_run_id, limit=100, offset=0):
        return _Obj(task_instances=rows[offset : offset + limit], total_entries=total)

    monkeypatch.setattr(_TaskInstanceApi, "get_task_instances", paged)
    monkeypatch.setattr(tasks_mod, "_MAX_FILTER_SCAN_ROWS", 300)
    payload = _payload(
        airflow_tools.list_task_instances(
            instance=v2_instance, dag_id="etl", dag_run_id="run_1", state=["failed"]
        )
    )
    assert payload["count"] == 1
    assert payload["task_instances"][0]["task_id"] == "last"


def test_list_task_instances_scan_survives_server_capped_pages(
    v2_instance: str, monkeypatch: pytest.MonkeyPatch
):
    # Server caps every page below the requested limit (maximum_page_limit):
    # a short page must not be read as exhaustion while total_entries says more.
    rows = [_Obj(task_id=f"t{i}", state="success", try_number=1) for i in range(15)]
    rows.append(_Obj(task_id="needle", state="failed", try_number=1))

    def capped(self, dag_id, dag_run_id, limit=100, offset=0):
        return _Obj(task_instances=rows[offset : offset + min(limit, 10)], total_entries=len(rows))

    monkeypatch.setattr(_TaskInstanceApi, "get_task_instances", capped)
    payload = _payload(
        airflow_tools.list_task_instances(
            instance=v2_instance, dag_id="etl", dag_run_id="run_1", state=["failed"]
        )
    )
    assert payload["count"] == 1
    assert payload["task_instances"][0]["task_id"] == "needle"


def test_list_task_instances_per_task_pagination_survives_server_capped_pages(
    v2_instance: str, monkeypatch: pytest.MonkeyPatch
):
    # Mapped task with 12 instances; server caps pages at 5. Per-task paging
    # must follow total_entries and return every mapped instance.
    rows = [_Obj(task_id="mapped", state="success", try_number=1) for _ in range(12)]
    rows.append(_Obj(task_id="other", state="success", try_number=1))

    def capped(self, dag_id, dag_run_id, limit=100, offset=0, state=None, task_id=None):
        matching = [r for r in rows if task_id is None or r.task_id == task_id]
        return _Obj(
            task_instances=matching[offset : offset + min(limit, 5)],
            total_entries=len(matching),
        )

    monkeypatch.setattr(_TaskInstanceApi, "get_task_instances", capped)
    payload = _payload(
        airflow_tools.list_task_instances(
            instance=v2_instance, dag_id="etl", dag_run_id="run_1", task_ids=["mapped", "other"]
        )
    )
    assert payload["count"] == 13
    assert payload["total_entries"] == 13


def test_dataset_events_asset_lookup_includes_inactive_assets(v2_instance: str):
    airflow_tools.dataset_events(instance=v2_instance, dataset_uri="s3://bucket/data")
    client = cf.get_client_factory().get_api_client(v2_instance)
    # Inactive assets keep their historical events; the lookup must not use the
    # server's only-active default.
    assert client.calls["get_assets"]["only_active"] is False


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
    # The whole log must be requested up front: without full_content the API
    # returns only the first chunk plus a continuation token we don't follow.
    client = cf.get_client_factory().get_api_client(v2_instance)
    assert client.calls["get_log_without_preload_content"]["full_content"] is True


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
