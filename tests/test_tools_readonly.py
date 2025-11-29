import sys
from datetime import datetime
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from airflow_mcp import tools as airflow_tools
from airflow_mcp.client_factory import get_client_factory
from airflow_mcp.errors import AirflowToolError
from airflow_mcp.tools import ApiException as AirflowApiException
from airflow_mcp.tools._common import _coerce_datetime, _coerce_int


class _Obj:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def to_dict(self):
        return dict(self.__dict__)


def _payload(result):
    assert isinstance(result, dict)
    assert "request_id" in result
    return result


def _install_fake_airflow_client(monkeypatch: pytest.MonkeyPatch):
    class Configuration:  # noqa: D401
        def __init__(self, host: str, **_: object) -> None:  # noqa: D401
            self.host = host
            self.username = None
            self.password = None
            self.verify_ssl = True

    class ApiClient:  # noqa: D401
        def __init__(self, cfg: Configuration) -> None:  # noqa: D401
            self.configuration = cfg
            self.last_post_dag_run = None
            self.last_clear_task_instances = None
            self.last_patch_dag = None
            self.last_clear_dag_run = None

    class DAGsApi:  # noqa: D401
        def __init__(self, api_client: ApiClient) -> None:  # noqa: D401
            self._c = api_client

        def get_dags(self, limit: int = 100, offset: int = 0):  # noqa: D401
            return _Obj(
                dags=[_Obj(dag_id="dag_a", is_paused=False), _Obj(dag_id="dag_b", is_paused=True)],
                total_entries=2,
            )

        def get_dag(self, dag_id: str):  # noqa: D401
            return _Obj(dag_id=dag_id, is_paused=False)

        def get_task(self, dag_id: str, task_id: str):  # noqa: D401
            return _Obj(
                dag_id=dag_id,
                task_id=task_id,
                retries=3,
                retry_delay="0:05:00",
                owner="data-eng",
            )

        def clear_task_instances(self, dag_id: str, clear_task_instance):  # noqa: D401
            self._c.last_clear_task_instances = clear_task_instance
            payload = (
                clear_task_instance.to_dict()
                if hasattr(clear_task_instance, "to_dict")
                else clear_task_instance
            )
            return _Obj(dag_id=dag_id, cleared=payload)

        def patch_dag(self, dag_id: str, dag, update_mask: str | None = None):  # noqa: D401
            self._c.last_patch_dag = {"dag": dag, "update_mask": update_mask}
            is_paused = getattr(dag, "is_paused", None)
            if isinstance(dag, dict):
                is_paused = dag.get("is_paused")
            return _Obj(dag_id=dag_id, is_paused=is_paused)

    class DAGRunsApi:  # noqa: D401
        def __init__(self, api_client: ApiClient) -> None:  # noqa: D401
            self._c = api_client

        def get_dag_runs(self, dag_id: str, **kwargs):  # noqa: D401
            return _Obj(
                dag_runs=[
                    _Obj(dag_run_id="dr1", state="success", start_date="s1", end_date="e1"),
                    _Obj(dag_run_id="dr2", state="failed", start_date="s2", end_date="e2"),
                ],
                total_entries=2,
            )

        def get_dag_run(self, dag_id: str, dag_run_id: str):  # noqa: D401
            return _Obj(dag_id=dag_id, dag_run_id=dag_run_id, state="success")

        def post_dag_run(self, dag_id: str, dag_run):  # noqa: D401
            self._c.last_post_dag_run = dag_run
            run_id = getattr(dag_run, "dag_run_id", None)
            if isinstance(dag_run, dict):
                run_id = dag_run.get("dag_run_id")
            run_id = run_id or "generated__20241027"
            conf = (
                getattr(dag_run, "conf", None)
                if not isinstance(dag_run, dict)
                else dag_run.get("conf")
            )
            logical_date = (
                getattr(dag_run, "logical_date", None)
                if not isinstance(dag_run, dict)
                else dag_run.get("logical_date")
            )
            note = (
                getattr(dag_run, "note", None)
                if not isinstance(dag_run, dict)
                else dag_run.get("note")
            )
            return _Obj(
                dag_id=dag_id, dag_run_id=run_id, conf=conf, logical_date=logical_date, note=note
            )

        def clear_dag_run(self, dag_id: str, dag_run_id: str, clear_task_instance):  # noqa: D401
            self._c.last_clear_dag_run = clear_task_instance
            payload = (
                clear_task_instance.to_dict()
                if hasattr(clear_task_instance, "to_dict")
                else clear_task_instance
            )
            return _Obj(dag_id=dag_id, dag_run_id=dag_run_id, cleared={"dag_run": payload})

    class TaskInstanceApi:  # noqa: D401
        def __init__(self, api_client: ApiClient) -> None:  # noqa: D401
            self._c = api_client

        def get_log(self, dag_id: str, dag_run_id: str, task_id: str, try_number: int):  # noqa: D401
            return f"log for {dag_id}/{dag_run_id}/{task_id}#{try_number}"

        def get_task_instance(self, dag_id: str, dag_run_id: str, task_id: str):  # noqa: D401
            return _Obj(
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                task_id=task_id,
                state="success",
                try_number=1,
                start_date="2025-01-01T00:00:00Z",
                end_date="2025-01-01T00:10:00Z",
                hostname="worker-1",
                operator="PythonOperator",
                queue="default",
                pool="default_pool",
                priority_weight=5,
                rendered_fields={"ds": "2025-01-01"},
            )

    class DatasetEventsApi:  # noqa: D401
        def __init__(self, api_client: ApiClient) -> None:  # noqa: D401
            self._c = api_client

        def get_dataset_events(self, limit: int = 50, uri: str | None = None):  # noqa: D401
            return _Obj(
                dataset_events=[_Obj(uri=uri, event="E1"), _Obj(uri=uri, event="E2")],
                total_entries=2,
            )

    class DAGModel:  # noqa: D401
        def __init__(self, **kwargs):  # noqa: D401
            self.__dict__.update(kwargs)

        def to_dict(self):  # noqa: D401
            return dict(self.__dict__)

    class DAGRunModel:  # noqa: D401
        def __init__(self, **kwargs):  # noqa: D401
            self.__dict__.update(kwargs)

        def to_dict(self):  # noqa: D401
            return dict(self.__dict__)

    class ClearTaskInstanceModel:  # noqa: D401
        def __init__(self, **kwargs):  # noqa: D401
            self.__dict__.update(kwargs)

        def to_dict(self):  # noqa: D401
            return dict(self.__dict__)

    # Mock the apis module to match real airflow_client.client.apis structure
    api_pkg = SimpleNamespace(
        DAGApi=DAGsApi,
        DAGRunApi=DAGRunsApi,
        TaskInstanceApi=TaskInstanceApi,
        DatasetApi=DatasetEventsApi,  # Real client uses DatasetApi for events
    )

    monkeypatch.setattr(
        "airflow_mcp.client_factory._import_airflow_client",
        lambda: (Configuration, ApiClient, api_pkg),
    )

    pkg = ModuleType("airflow_client")
    client_mod = ModuleType("airflow_client.client")
    model_pkg = ModuleType("airflow_client.client.model")
    dag_mod = ModuleType("airflow_client.client.model.dag")
    dag_mod.DAG = DAGModel
    dag_run_mod = ModuleType("airflow_client.client.model.dag_run")
    dag_run_mod.DAGRun = DAGRunModel
    clear_task_instance_mod = ModuleType("airflow_client.client.model.clear_task_instance")
    clear_task_instance_mod.ClearTaskInstance = ClearTaskInstanceModel

    model_pkg.dag = dag_mod
    model_pkg.dag_run = dag_run_mod
    model_pkg.clear_task_instance = clear_task_instance_mod
    client_mod.model = model_pkg
    pkg.client = client_mod

    sys.modules["airflow_client"] = pkg
    sys.modules["airflow_client.client"] = client_mod
    sys.modules["airflow_client.client.model"] = model_pkg
    sys.modules["airflow_client.client.model.dag"] = dag_mod
    sys.modules["airflow_client.client.model.dag_run"] = dag_run_mod
    sys.modules["airflow_client.client.model.clear_task_instance"] = clear_task_instance_mod


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


def test_list_dags_with_instance():
    out = airflow_tools.list_dags(instance="data-stg", limit=2)
    payload = _payload(out)
    assert payload["count"] == 2
    assert payload["dags"][0]["ui_url"].endswith("/dags/dag_a/grid")


def test_get_dag_with_instance():
    out = airflow_tools.get_dag(instance="data-stg", dag_id="dag_a")
    payload = _payload(out)
    assert payload["dag"]["dag_id"] == "dag_a"
    assert payload["ui_url"].endswith("/dags/dag_a/grid")


def test_list_dag_runs_with_ui_url():
    host = "https://airflow.data-stg.example.com"
    ui_url = f"{host}/dags/dag_a/grid?dag_run_id=dr1"
    out = airflow_tools.list_dag_runs(ui_url=ui_url)
    payload = _payload(out)
    assert payload["count"] == 2
    assert payload["dag_runs"][0]["dag_run_id"] == "dr1"
    assert payload["dag_runs"][0]["ui_url"].endswith("/dags/dag_a/dagRuns/dr1")
    assert payload["dag_runs"][1]["dag_run_id"] == "dr2"
    assert payload["dag_runs"][1]["ui_url"].endswith("/dags/dag_a/dagRuns/dr2")


def test_list_dag_runs_defaults_to_latest_first(monkeypatch: pytest.MonkeyPatch):
    from airflow_mcp import client_factory as cf

    *_, apis = cf._import_airflow_client()  # type: ignore[attr-defined]
    captured: dict[str, object] = {}

    def _fake_get_dag_runs(self, dag_id: str, **kwargs):  # noqa: D401
        captured.update(kwargs)
        return _Obj(
            dag_runs=[
                _Obj(dag_run_id="dr1", state="running"),
                _Obj(dag_run_id="dr0", state="success"),
            ],
            total_entries=2,
        )

    monkeypatch.setattr(apis.DAGRunApi, "get_dag_runs", _fake_get_dag_runs)

    out = airflow_tools.list_dag_runs(instance="data-stg", dag_id="dag_a")
    payload = _payload(out)
    assert captured["order_by"] == "-execution_date"
    assert payload["count"] == 2


def test_list_dag_runs_maps_api_not_found(monkeypatch: pytest.MonkeyPatch):
    from airflow_mcp import client_factory as cf

    *_, apis = cf._import_airflow_client()  # type: ignore[attr-defined]

    def _raise_not_found(self, dag_id: str, **kwargs):  # noqa: D401
        raise AirflowApiException(status=404, reason="DAG not found")

    monkeypatch.setattr(apis.DAGRunApi, "get_dag_runs", _raise_not_found)

    with pytest.raises(AirflowToolError) as exc:
        airflow_tools.list_dag_runs(instance="data-stg", dag_id="missing_dag")

    assert exc.value.code == "NOT_FOUND"
    assert "HTTP 404" in str(exc.value)


def test_list_dag_runs_order_by_start_date_desc(monkeypatch: pytest.MonkeyPatch):
    from airflow_mcp import client_factory as cf

    *_, apis = cf._import_airflow_client()  # type: ignore[attr-defined]
    captured: dict[str, object] = {}

    def _fake_get_dag_runs(self, dag_id: str, **kwargs):  # noqa: D401
        captured.update(kwargs)
        return _Obj(
            dag_runs=[
                _Obj(dag_run_id="recent", state="success", start_date="2025-01-02T00:00:00Z"),
            ],
            total_entries=1,
        )

    monkeypatch.setattr(apis.DAGRunApi, "get_dag_runs", _fake_get_dag_runs)

    out = airflow_tools.list_dag_runs(
        instance="data-stg", dag_id="dag_a", order_by="start_date"
    )
    payload = _payload(out)
    assert captured["order_by"] == "-start_date"
    assert payload["dag_runs"][0]["dag_run_id"] == "recent"


def test_list_dag_runs_order_by_execution_date_ascending(monkeypatch: pytest.MonkeyPatch):
    from airflow_mcp import client_factory as cf

    *_, apis = cf._import_airflow_client()  # type: ignore[attr-defined]
    captured: dict[str, object] = {}

    def _fake_get_dag_runs(self, dag_id: str, **kwargs):  # noqa: D401
        captured.update(kwargs)
        return _Obj(
            dag_runs=[
                _Obj(
                    dag_run_id="oldest",
                    state="failed",
                    start_date="2025-01-01T00:00:00Z",
                    end_date="2025-01-01T01:00:00Z",
                )
            ],
            total_entries=1,
        )

    monkeypatch.setattr(apis.DAGRunApi, "get_dag_runs", _fake_get_dag_runs)

    out = airflow_tools.list_dag_runs(
        instance="data-stg", dag_id="dag_a", order_by="execution_date", descending=False
    )
    payload = _payload(out)
    assert captured["order_by"] == "execution_date"
    assert payload["dag_runs"][0]["dag_run_id"] == "oldest"


def test_list_dag_runs_order_by_end_date_desc(monkeypatch: pytest.MonkeyPatch):
    from airflow_mcp import client_factory as cf

    *_, apis = cf._import_airflow_client()  # type: ignore[attr-defined]
    captured: dict[str, object] = {}

    def _fake_get_dag_runs(self, dag_id: str, **kwargs):  # noqa: D401
        captured.update(kwargs)
        return _Obj(
            dag_runs=[
                _Obj(
                    dag_run_id="completed_last",
                    state="success",
                    start_date="2025-01-02T00:00:00Z",
                    end_date="2025-01-02T02:00:00Z",
                ),
            ],
            total_entries=1,
        )

    monkeypatch.setattr(apis.DAGRunApi, "get_dag_runs", _fake_get_dag_runs)

    out = airflow_tools.list_dag_runs(
        instance="data-stg", dag_id="dag_a", order_by="end_date"
    )
    payload = _payload(out)
    assert captured["order_by"] == "-end_date"
    assert payload["dag_runs"][0]["dag_run_id"] == "completed_last"


def test_list_dag_runs_order_by_with_state_filter(monkeypatch: pytest.MonkeyPatch):
    from airflow_mcp import client_factory as cf

    *_, apis = cf._import_airflow_client()  # type: ignore[attr-defined]
    captured: dict[str, object] = {}

    def _fake_get_dag_runs(self, dag_id: str, **kwargs):  # noqa: D401
        captured.update(kwargs)
        return _Obj(
            dag_runs=[
                _Obj(dag_run_id="filtered", state="failed", start_date="2025-01-03T00:00:00Z"),
            ],
            total_entries=1,
        )

    monkeypatch.setattr(apis.DAGRunApi, "get_dag_runs", _fake_get_dag_runs)

    out = airflow_tools.list_dag_runs(
        instance="data-stg",
        dag_id="dag_a",
        state=["failed"],
        order_by="end_date",
    )
    payload = _payload(out)
    assert captured["order_by"] == "-end_date"
    assert captured["state"] == ["failed"]
    assert payload["dag_runs"][0]["dag_run_id"] == "filtered"


def test_get_task_instance_logs_maps_api_invalid_input(monkeypatch: pytest.MonkeyPatch):
    from airflow_mcp import client_factory as cf

    *_, apis = cf._import_airflow_client()  # type: ignore[attr-defined]

    def _raise_invalid(self, dag_id: str, dag_run_id: str, task_id: str, try_number: int):  # noqa: D401
        raise AirflowApiException(status=400, reason="bad try_number")

    monkeypatch.setattr(apis.TaskInstanceApi, "get_log", _raise_invalid)

    with pytest.raises(AirflowToolError) as exc:
        airflow_tools.get_task_instance_logs(
            instance="data-stg",
            dag_id="dag_a",
            dag_run_id="dr1",
            task_id="task_a",
            try_number=1,
        )

    assert exc.value.code == "INVALID_INPUT"


def test_list_dag_runs_invalid_order_by(monkeypatch: pytest.MonkeyPatch):
    with pytest.raises(AirflowToolError) as exc:
        airflow_tools.list_dag_runs(instance="data-stg", dag_id="dag_a", order_by="duration")
    assert "order_by" in str(exc.value)
    assert exc.value.code == "INVALID_INPUT"


def test_list_dag_runs_invalid_descending_value():
    with pytest.raises(AirflowToolError) as exc:
        airflow_tools.list_dag_runs(
            instance="data-stg", dag_id="dag_a", order_by="start_date", descending="nope"
        )
    assert "descending" in str(exc.value)
    assert exc.value.code == "INVALID_INPUT"


def test_get_dag_run():
    out = airflow_tools.get_dag_run(
        instance="data-stg", dag_id="dag_a", dag_run_id="dr1"
    )
    payload = _payload(out)
    assert payload["dag_run"]["dag_run_id"] == "dr1"


def test_get_task_instance_basic():
    out = airflow_tools.get_task_instance(
        instance="data-stg", dag_id="dag_a", dag_run_id="dr1", task_id="t1"
    )
    payload = _payload(out)
    ti = payload["task_instance"]
    assert ti["task_id"] == "t1"
    assert ti["duration_ms"] == 600000
    attempts = payload["attempts"]
    assert attempts["try_number"] == 1
    assert attempts["retries_configured"] == 3
    assert attempts["retries_remaining"] == 3
    config = payload["task_config"]
    assert config["owner"] == "data-eng"
    assert payload["ui_url"]["grid"].endswith("/dags/dag_a/grid?dag_run_id=dr1")
    assert payload["ui_url"]["log"].endswith("/dags/dag_a/dagRuns/dr1/taskInstances/t1/logs/1")
    assert "rendered_fields" not in payload


def test_get_task_instance_rendered_fields_truncated(monkeypatch: pytest.MonkeyPatch):
    from airflow_mcp import client_factory as cf

    *_, apis = cf._import_airflow_client()  # type: ignore[attr-defined]

    def _fake_get_task_instance(self, dag_id: str, dag_run_id: str, task_id: str):  # noqa: D401
        return _Obj(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            state="failed",
            try_number=4,
            start_date="2025-01-01T00:00:00Z",
            end_date="2025-01-01T00:05:00Z",
            rendered_fields={"template": "x" * 5000},
        )

    monkeypatch.setattr(apis.TaskInstanceApi, "get_task_instance", _fake_get_task_instance)

    out = airflow_tools.get_task_instance(
        instance="data-stg",
        dag_id="dag_a",
        dag_run_id="dr1",
        task_id="t1",
        include_rendered=True,
        max_rendered_bytes=128,
    )
    payload = _payload(out)
    rendered = payload["rendered_fields"]
    assert rendered["truncated"] is True
    assert rendered["fields"] == {"_truncated": "Increase max_rendered_bytes"}
    assert rendered["bytes_returned"] <= 128
    assert payload["attempts"]["try_number"] == 4


def test_get_task_instance_task_config_unavailable(monkeypatch: pytest.MonkeyPatch):
    from airflow_mcp import client_factory as cf

    *_, apis = cf._import_airflow_client()  # type: ignore[attr-defined]

    def _raising_get_task(self, dag_id: str, task_id: str):  # noqa: D401
        raise RuntimeError("boom")

    monkeypatch.setattr(apis.DAGApi, "get_task", _raising_get_task)

    out = airflow_tools.get_task_instance(
        instance="data-stg",
        dag_id="dag_a",
        dag_run_id="dr1",
        task_id="t1",
    )
    payload = _payload(out)
    assert payload["task_config"] == {
        "retries": None,
        "retry_delay": None,
        "owner": None,
    }


def test_get_task_instance_requires_identifiers():
    with pytest.raises(AirflowToolError) as exc:
        airflow_tools.get_task_instance(
            instance="data-stg", dag_id="dag_a", dag_run_id="dr1"
        )
    assert exc.value.code == "INVALID_INPUT"


def test_get_task_instance_not_found(monkeypatch: pytest.MonkeyPatch):
    from airflow_mcp import client_factory as cf

    *_, apis = cf._import_airflow_client()  # type: ignore[attr-defined]

    def _not_found(self, dag_id: str, dag_run_id: str, task_id: str):  # noqa: D401
        raise AirflowToolError("Task instance not found", code="NOT_FOUND")

    monkeypatch.setattr(apis.TaskInstanceApi, "get_task_instance", _not_found)

    with pytest.raises(AirflowToolError) as exc:
        airflow_tools.get_task_instance(
            instance="data-stg",
            dag_id="dag_a",
            dag_run_id="dr1",
            task_id="missing",
        )
    assert exc.value.code == "NOT_FOUND"


def test_get_task_instance_logs():
    out = airflow_tools.get_task_instance_logs(
        instance="data-stg", dag_id="dag_a", dag_run_id="dr1", task_id="t1", try_number=1
    )
    payload = _payload(out)
    assert "log for dag_a/dr1/t1#1" in payload["log"]


def test_task_logs_filter_error_with_context(monkeypatch: pytest.MonkeyPatch):
    # Patch TaskInstanceApi.get_log to return multi-line content
    from airflow_mcp import client_factory as cf

    *_, apis = cf._import_airflow_client()  # type: ignore[attr-defined]

    def _fake_get_log(self, dag_id: str, dag_run_id: str, task_id: str, try_number: int):  # noqa: D401
        return "\n".join(
            [
                "2025-01-01 INFO start",
                "2025-01-01 WARN something odd",
                "2025-01-01 INFO working",
                "2025-01-01 ERROR boom",
                "2025-01-01 INFO after",
                "2025-01-01 CRITICAL meltdown",
                "2025-01-01 INFO done",
            ]
        )

    monkeypatch.setattr(apis.TaskInstanceApi, "get_log", _fake_get_log)

    out = airflow_tools.get_task_instance_logs(
        instance="data-stg",
        dag_id="dag_a",
        dag_run_id="dr1",
        task_id="t1",
        try_number=1,
        tail_lines=100,
        filter_level="error",
        context_lines=1,
        max_bytes=10_000,
    )
    payload = _payload(out)
    # Expect ERROR and CRITICAL lines with 1 line of context each
    assert payload["match_count"] == 2
    # Returned lines should include context around both matches (deduped if overlapping)
    assert payload["returned_lines"] >= 4
    assert "ERROR boom" in payload["log"]
    assert "CRITICAL meltdown" in payload["log"]
    # Filters echoed in meta
    assert payload["meta"]["filters"]["filter_level"] == "error"
    assert payload["meta"]["filters"]["context_lines"] == 1


def test_task_logs_truncation_with_max_bytes(monkeypatch: pytest.MonkeyPatch):
    # Force a long log and verify truncation behavior
    from airflow_mcp import client_factory as cf

    *_, apis = cf._import_airflow_client()  # type: ignore[attr-defined]

    long_log = ("ERROR something happened\n" * 1000) + ("INFO done\n" * 1000)

    def _fake_get_log(self, dag_id: str, dag_run_id: str, task_id: str, try_number: int):  # noqa: D401
        return long_log

    monkeypatch.setattr(apis.TaskInstanceApi, "get_log", _fake_get_log)

    out = airflow_tools.get_task_instance_logs(
        instance="data-stg",
        dag_id="dag_a",
        dag_run_id="dr1",
        task_id="t1",
        try_number=1,
        filter_level="error",
        context_lines=0,
        max_bytes=1024,
    )
    payload = _payload(out)
    assert payload["truncated"] is True
    assert payload["bytes_returned"] <= 1024
    assert "ERROR" in payload["log"]


def test_task_logs_tail_zero(monkeypatch: pytest.MonkeyPatch):
    # tail_lines=0 should return empty log quickly
    from airflow_mcp import client_factory as cf

    *_, apis = cf._import_airflow_client()  # type: ignore[attr-defined]

    def _fake_get_log(self, dag_id: str, dag_run_id: str, task_id: str, try_number: int):  # noqa: D401
        return "INFO a\nINFO b\nERROR c\nINFO d"

    monkeypatch.setattr(apis.TaskInstanceApi, "get_log", _fake_get_log)

    out = airflow_tools.get_task_instance_logs(
        instance="data-stg",
        dag_id="dag_a",
        dag_run_id="dr1",
        task_id="t1",
        try_number=1,
        tail_lines=0,
    )
    payload = _payload(out)
    assert payload["returned_lines"] == 0
    assert payload["log"] == ""


def test_task_logs_filter_warning_includes_warn_and_error(monkeypatch: pytest.MonkeyPatch):
    from airflow_mcp import client_factory as cf

    *_, apis = cf._import_airflow_client()  # type: ignore[attr-defined]

    def _fake_get_log(self, dag_id: str, dag_run_id: str, task_id: str, try_number: int):  # noqa: D401
        return "\n".join(
            [
                "INFO booting",
                "WARNING subtle issue",
                "ERROR failure",
                "INFO end",
            ]
        )

    monkeypatch.setattr(apis.TaskInstanceApi, "get_log", _fake_get_log)

    out = airflow_tools.get_task_instance_logs(
        instance="data-stg",
        dag_id="dag_a",
        dag_run_id="dr1",
        task_id="t1",
        try_number=1,
        filter_level="warning",
        context_lines=0,
    )
    payload = _payload(out)
    assert "WARNING subtle issue" in payload["log"]
    assert "ERROR failure" in payload["log"]
    # INFO-only line should be excluded at warning level
    assert "INFO booting" not in payload["log"]


def test_task_logs_filter_info_includes_all_levels(monkeypatch: pytest.MonkeyPatch):
    from airflow_mcp import client_factory as cf

    *_, apis = cf._import_airflow_client()  # type: ignore[attr-defined]

    def _fake_get_log(self, dag_id: str, dag_run_id: str, task_id: str, try_number: int):  # noqa: D401
        return "\n".join(
            [
                "INFO booting",
                "WARN about something",
                "CRITICAL crash",
            ]
        )

    monkeypatch.setattr(apis.TaskInstanceApi, "get_log", _fake_get_log)

    out = airflow_tools.get_task_instance_logs(
        instance="data-stg",
        dag_id="dag_a",
        dag_run_id="dr1",
        task_id="t1",
        try_number=1,
        filter_level="info",
        context_lines=0,
    )
    payload = _payload(out)
    assert "INFO booting" in payload["log"]
    assert "WARN about something" in payload["log"]
    assert "CRITICAL crash" in payload["log"]


def test_task_logs_host_segment_flatten(monkeypatch: pytest.MonkeyPatch):
    from airflow_mcp import client_factory as cf

    *_, apis = cf._import_airflow_client()  # type: ignore[attr-defined]

    class _SegmentedResponse:
        def __init__(self):
            self.content = [
                ("worker-1.example", "INFO start\nERROR boom\n"),
                ("", "INFO tail\n"),
            ]

    def _fake_get_log(self, dag_id: str, dag_run_id: str, task_id: str, try_number: int):  # noqa: D401
        return _SegmentedResponse()

    monkeypatch.setattr(apis.TaskInstanceApi, "get_log", _fake_get_log)

    out = airflow_tools.get_task_instance_logs(
        instance="data-stg",
        dag_id="dag_a",
        dag_run_id="dr1",
        task_id="t1",
        try_number=1,
    )
    payload = _payload(out)
    assert payload["log"].startswith("--- [worker-1.example] ---")
    assert "--- [unknown-host] ---" in payload["log"]
    assert "ERROR boom" in payload["log"]


def test_task_logs_bytes_response(monkeypatch: pytest.MonkeyPatch):
    from airflow_mcp import client_factory as cf

    *_, apis = cf._import_airflow_client()  # type: ignore[attr-defined]

    class _BytesResponse:
        def __init__(self, data: bytes) -> None:
            self.content = data

    def _fake_get_log(self, dag_id: str, dag_run_id: str, task_id: str, try_number: int):  # noqa: D401
        return _BytesResponse(b"INFO hello\nERROR fail\n")

    monkeypatch.setattr(apis.TaskInstanceApi, "get_log", _fake_get_log)

    out = airflow_tools.get_task_instance_logs(
        instance="data-stg",
        dag_id="dag_a",
        dag_run_id="dr1",
        task_id="t1",
        try_number=1,
        filter_level="error",
        context_lines=0,
    )
    payload = _payload(out)
    assert "ERROR fail" in payload["log"]


def test_task_logs_auto_tail_for_large_logs(monkeypatch: pytest.MonkeyPatch):
    # CRITICAL: Test that logs >100MB trigger auto-tail to last 10,000 lines
    from airflow_mcp import client_factory as cf

    *_, apis = cf._import_airflow_client()  # type: ignore[attr-defined]  # noqa: N806

    # Generate 100M+ character log (simulates 100MB+)
    large_log = "INFO line {}\n" * 12_000  # Each ~15 chars = ~180KB
    large_log = large_log.format(*range(12_000))
    # Repeat to exceed 100M chars
    large_log = large_log * 1000  # ~180MB in characters

    def _fake_get_log(self, dag_id: str, dag_run_id: str, task_id: str, try_number: int):  # noqa: D401
        return large_log

    monkeypatch.setattr(apis.TaskInstanceApi, "get_log", _fake_get_log)

    out = airflow_tools.get_task_instance_logs(
        instance="data-stg",
        dag_id="dag_a",
        dag_run_id="dr1",
        task_id="t1",
        try_number=1,
    )
    payload = _payload(out)
    assert payload["auto_tailed"] is True
    # After auto-tail, should have ~10,000 lines max
    assert payload["returned_lines"] <= 10_000


def test_task_logs_context_expansion_at_start_edge(monkeypatch: pytest.MonkeyPatch):
    # Edge case: ERROR at line 0, context_lines=5 should not fail
    from airflow_mcp import client_factory as cf

    *_, apis = cf._import_airflow_client()  # type: ignore[attr-defined]  # noqa: N806

    def _fake_get_log(self, dag_id: str, dag_run_id: str, task_id: str, try_number: int):  # noqa: D401
        return "\n".join(
            [
                "ERROR first line failure",
                "INFO line 1",
                "INFO line 2",
                "INFO line 3",
                "INFO line 4",
                "INFO line 5",
                "INFO line 6",
            ]
        )

    monkeypatch.setattr(apis.TaskInstanceApi, "get_log", _fake_get_log)

    out = airflow_tools.get_task_instance_logs(
        instance="data-stg",
        dag_id="dag_a",
        dag_run_id="dr1",
        task_id="t1",
        try_number=1,
        filter_level="error",
        context_lines=5,
    )
    payload = _payload(out)
    assert payload["match_count"] == 1
    # Should include ERROR + 5 lines after (no negative indexing)
    assert "ERROR first line failure" in payload["log"]
    assert "INFO line 5" in payload["log"]
    assert payload["returned_lines"] == 6  # 0 before + match + 5 after


def test_task_logs_context_expansion_at_end_edge(monkeypatch: pytest.MonkeyPatch):
    # Edge case: ERROR at last line, context_lines=5 should not fail
    from airflow_mcp import client_factory as cf

    *_, apis = cf._import_airflow_client()  # type: ignore[attr-defined]  # noqa: N806

    def _fake_get_log(self, dag_id: str, dag_run_id: str, task_id: str, try_number: int):  # noqa: D401
        return "\n".join(
            [
                "INFO line 0",
                "INFO line 1",
                "INFO line 2",
                "INFO line 3",
                "INFO line 4",
                "INFO line 5",
                "ERROR last line failure",
            ]
        )

    monkeypatch.setattr(apis.TaskInstanceApi, "get_log", _fake_get_log)

    out = airflow_tools.get_task_instance_logs(
        instance="data-stg",
        dag_id="dag_a",
        dag_run_id="dr1",
        task_id="t1",
        try_number=1,
        filter_level="error",
        context_lines=5,
    )
    payload = _payload(out)
    assert payload["match_count"] == 1
    # Should include 5 lines before + ERROR (no overflow past end)
    assert "ERROR last line failure" in payload["log"]
    assert "INFO line 2" in payload["log"]
    assert payload["returned_lines"] == 6  # 5 before + match + 0 after


def test_task_logs_parameter_clamps_upper_bounds(monkeypatch: pytest.MonkeyPatch):
    # Verify excessive parameters are clamped to safe limits
    from airflow_mcp import client_factory as cf

    *_, apis = cf._import_airflow_client()  # type: ignore[attr-defined]

    def _fake_get_log(self, dag_id: str, dag_run_id: str, task_id: str, try_number: int):  # noqa: D401
        return "ERROR test\n" * 10

    monkeypatch.setattr(apis.TaskInstanceApi, "get_log", _fake_get_log)

    out = airflow_tools.get_task_instance_logs(
        instance="data-stg",
        dag_id="dag_a",
        dag_run_id="dr1",
        task_id="t1",
        try_number=1,
        tail_lines=200_000,  # Exceeds 100K limit
        context_lines=5_000,  # Exceeds 1K limit
    )
    payload = _payload(out)
    # Parameters should be clamped in meta.filters
    assert payload["meta"]["filters"]["tail_lines"] == 100_000
    assert payload["meta"]["filters"]["context_lines"] == 1_000


def test_task_logs_combined_filters(monkeypatch: pytest.MonkeyPatch):
    # Test tail + filter + context working together
    from airflow_mcp import client_factory as cf

    *_, apis = cf._import_airflow_client()  # type: ignore[attr-defined]

    def _fake_get_log(self, dag_id: str, dag_run_id: str, task_id: str, try_number: int):  # noqa: D401
        lines = []
        for i in range(100):
            if i == 95:
                lines.append(f"ERROR failure at {i}")
            else:
                lines.append(f"INFO line {i}")
        return "\n".join(lines)

    monkeypatch.setattr(apis.TaskInstanceApi, "get_log", _fake_get_log)

    out = airflow_tools.get_task_instance_logs(
        instance="data-stg",
        dag_id="dag_a",
        dag_run_id="dr1",
        task_id="t1",
        try_number=1,
        tail_lines=20,  # Only last 20 lines (lines 80-99)
        filter_level="error",  # Find ERROR in those 20
        context_lines=2,  # +/- 2 lines around match
    )
    payload = _payload(out)
    assert payload["match_count"] == 1
    assert "ERROR failure at 95" in payload["log"]
    # Should have context: lines 93, 94, 95, 96, 97 (5 total)
    assert "INFO line 93" in payload["log"]
    assert "INFO line 97" in payload["log"]
    # Earlier lines should NOT be included (were filtered by tail_lines)
    assert "INFO line 50" not in payload["log"]


def test_coerce_int_edge_cases():
    assert _coerce_int(True) == 1
    assert _coerce_int(3.0) == 3
    assert _coerce_int(3.5) is None
    assert _coerce_int("7") == 7
    assert _coerce_int("abc") is None


def test_coerce_datetime_edge_cases():
    dt_value = _coerce_datetime("2025-01-01T00:00:00Z")
    assert isinstance(dt_value, datetime)
    assert dt_value.isoformat().endswith("+00:00")
    assert _coerce_datetime("not-a-date") is None
    assert _coerce_datetime(None) is None


def test_dataset_events():
    out = airflow_tools.dataset_events(
        instance="data-stg", dataset_uri="dataset://abc", limit=2
    )
    payload = _payload(out)
    assert payload["count"] == 2
    assert payload["events"][0]["uri"] == "dataset://abc"


def test_list_dag_runs_invalid_identifier():
    # dag_id with newline should fail
    with pytest.raises(AirflowToolError) as exc:
        airflow_tools.list_dag_runs(instance="data-stg", dag_id="bad\ndag")
    assert exc.value.code == "INVALID_INPUT"


def test_task_logs_invalid_task_id():
    # task_id with space should fail (strict pattern)
    with pytest.raises(AirflowToolError) as exc:
        airflow_tools.get_task_instance_logs(
            instance="data-stg",
            dag_id="dag_a",
            dag_run_id="dr1",
            task_id="bad task",
            try_number=1,
        )
    assert exc.value.code == "INVALID_INPUT"


def test_dataset_events_invalid_uri():
    with pytest.raises(AirflowToolError) as exc:
        airflow_tools.dataset_events(
            instance="data-stg", dataset_uri="dataset://bad uri"
        )
    assert exc.value.code == "INVALID_INPUT"


def test_list_dags_invalid_instance_key():
    with pytest.raises(AirflowToolError) as exc:
        airflow_tools.list_dags(instance="bad instance!")
    assert exc.value.code == "INVALID_INPUT"


def test_get_dag_requires_instance_or_url():
    with pytest.raises(AirflowToolError) as exc:
        airflow_tools.get_dag(dag_id="dag_a")
    assert "MISSING_TARGET" in str(exc.value)
    assert exc.value.code == "INVALID_INPUT"


def test_get_dag_unknown_instance_raises():
    with pytest.raises(AirflowToolError) as exc:
        airflow_tools.get_dag(instance="missing", dag_id="dag_a")
    assert "Unknown instance" in str(exc.value)
    assert exc.value.code == "NOT_FOUND"


def test_list_dag_runs_instance_mismatch():
    host = "https://airflow.data-stg.example.com"
    ui_url = f"{host}/dags/dag_a/grid?dag_run_id=dr1"
    with pytest.raises(AirflowToolError) as exc:
        airflow_tools.list_dag_runs(instance="ml-stg", ui_url=ui_url)
    assert "INSTANCE_MISMATCH" in str(exc.value)
    assert exc.value.code == "INVALID_INPUT"


def test_resolve_url_invalid_scheme():
    with pytest.raises(AirflowToolError) as exc:
        airflow_tools.resolve_url("ftp://example.com/dags/")
    assert "must start with http or https" in str(exc.value)
    assert exc.value.code == "INVALID_INPUT"


def test_list_task_instances_filters_state(monkeypatch: pytest.MonkeyPatch):
    from airflow_mcp import client_factory as cf

    *_, apis = cf._import_airflow_client()  # type: ignore[attr-defined]

    captured_kwargs: dict[str, object] = {}

    def _fake_get_task_instances(self, dag_id: str, dag_run_id: str, **kwargs):  # noqa: D401
        captured_kwargs.clear()
        captured_kwargs.update(kwargs)
        return _Obj(
            task_instances=[
                _Obj(task_id="a", state="failed", try_number=1, start_date="2025-01-01T08:00:00Z"),
                _Obj(task_id="b", state="success", try_number=2, start_date="2025-01-01T09:00:00Z"),
            ],
            total_entries=2,
        )

    monkeypatch.setattr(
        apis.TaskInstanceApi, "get_task_instances", _fake_get_task_instances, raising=False
    )

    out = airflow_tools.list_task_instances(
        instance="data-stg", dag_id="dag_a", dag_run_id="dr1", state=["failed"]
    )
    payload = _payload(out)
    assert captured_kwargs["state"] == ["failed"]
    assert payload["count"] == 1
    assert payload["total_entries"] == 2
    assert payload["filters"]["state"] == ["failed"]
    assert payload["filters"]["task_ids"] is None
    assert payload["task_instances"][0]["task_id"] == "a"
    assert payload["task_instances"][0]["ui_url"].endswith(
        "/dags/dag_a/dagRuns/dr1/taskInstances/a/logs/1"
    )


def test_list_task_instances_filters_task_ids(monkeypatch: pytest.MonkeyPatch):
    from airflow_mcp import client_factory as cf

    *_, apis = cf._import_airflow_client()  # type: ignore[attr-defined]

    def _fake_get_task_instances(  # noqa: D401
        self, dag_id: str, dag_run_id: str, limit: int = 100, offset: int = 0
    ):
        assert limit == 100
        assert offset == 0
        return _Obj(
            task_instances=[
                _Obj(task_id="keep", state="failed", try_number=4),
                _Obj(task_id="drop", state="failed", try_number=1),
            ],
            total_entries=2,
        )

    monkeypatch.setattr(
        apis.TaskInstanceApi, "get_task_instances", _fake_get_task_instances, raising=False
    )

    out = airflow_tools.list_task_instances(
        instance="data-stg", dag_id="dag_a", dag_run_id="dr1", task_ids=["keep"]
    )
    payload = _payload(out)
    assert payload["count"] == 1
    assert payload["total_entries"] == 2
    assert payload["task_instances"][0]["task_id"] == "keep"
    assert payload["filters"]["state"] is None
    assert payload["filters"]["task_ids"] == ["keep"]


def test_list_task_instances_combined_filters(monkeypatch: pytest.MonkeyPatch):
    from airflow_mcp import client_factory as cf

    *_, apis = cf._import_airflow_client()  # type: ignore[attr-defined]

    def _fake_get_task_instances(self, dag_id: str, dag_run_id: str, **kwargs):  # noqa: D401
        return _Obj(
            task_instances=[
                _Obj(task_id="t1", state="FAILED", try_number=5),
                _Obj(task_id="t2", state="queued", try_number=1),
            ],
            total_entries=2,
        )

    monkeypatch.setattr(
        apis.TaskInstanceApi, "get_task_instances", _fake_get_task_instances, raising=False
    )

    out = airflow_tools.list_task_instances(
        instance="data-stg",
        dag_id="dag_a",
        dag_run_id="dr1",
        state="Failed",  # Accept string input; case-insensitive match
        task_ids="t1",  # Accept string input
    )
    payload = _payload(out)
    assert payload["count"] == 1
    assert payload["task_instances"][0]["task_id"] == "t1"
    assert payload["task_instances"][0]["state"] == "FAILED"
    assert payload["filters"]["state"] == ["Failed"]
    assert payload["filters"]["task_ids"] == ["t1"]


def test_list_task_instances_invalid_task_ids():
    with pytest.raises(AirflowToolError) as exc:
        airflow_tools.list_task_instances(
            instance="data-stg",
            dag_id="dag_a",
            dag_run_id="dr1",
            task_ids=["bad task"],  # space invalid
        )
    assert exc.value.code == "INVALID_INPUT"
