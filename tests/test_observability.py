from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from airflow_mcp.observability import (
    get_current_operation_logger,
    operation_logger,
)
from airflow_mcp.registry import reset_registry_cache


@pytest.fixture
def observability_caplog(caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
    caplog.clear()
    caplog.set_level(logging.DEBUG, logger="airflow_mcp.observability")
    caplog.set_level(logging.DEBUG, logger="airflow_mcp.client_factory")
    return caplog


def _extract_events(caplog: pytest.LogCaptureFixture, tool_name: str) -> list[logging.LogRecord]:
    return [record for record in caplog.records if getattr(record, "tool_name", None) == tool_name]


def test_operation_logger_success_flow(observability_caplog: pytest.LogCaptureFixture) -> None:
    caplog = observability_caplog

    with operation_logger("test_success", instance="data-stg") as op:
        assert get_current_operation_logger() is op
        response = op.success({"result": "ok"})

    assert get_current_operation_logger() is None
    payload = response
    assert payload["result"] == "ok"
    assert "request_id" in payload and len(payload["request_id"]) == 32

    records = _extract_events(caplog, "test_success")
    events = {getattr(record, "event", None) for record in records}
    assert {"tool_start", "tool_success"}.issubset(events)
    success_records = [r for r in records if getattr(r, "event", None) == "tool_success"]
    assert success_records
    assert success_records[-1].response_bytes > 0
    assert success_records[-1].instance == "data-stg"


def test_operation_logger_error_flow(observability_caplog: pytest.LogCaptureFixture) -> None:
    caplog = observability_caplog

    with operation_logger("test_error", dag_id="example") as op:
        response = op.error("boom", error_type="BoomError", level=logging.ERROR)

    payload = json.loads(response)
    assert payload["error"] == "boom"
    assert "request_id" in payload

    records = _extract_events(caplog, "test_error")
    error_records = [r for r in records if getattr(r, "event", None) == "tool_error"]
    assert error_records
    record = error_records[-1]
    assert record.levelno == logging.ERROR
    assert record.error_type == "BoomError"
    assert record.dag_id == "example"


def test_operation_logger_exception_logs_warning(
    observability_caplog: pytest.LogCaptureFixture,
) -> None:
    caplog = observability_caplog

    with pytest.raises(RuntimeError):
        with operation_logger("test_exception"):
            raise RuntimeError("failure")

    assert get_current_operation_logger() is None

    records = _extract_events(caplog, "test_exception")
    warning_records = [r for r in records if getattr(r, "event", None) == "tool_error"]
    assert warning_records
    assert warning_records[-1].levelno == logging.WARNING


def test_operation_logger_update_context(observability_caplog: pytest.LogCaptureFixture) -> None:
    caplog = observability_caplog

    with operation_logger("test_update") as op:
        op.update_context(dag_id="dag_a", task_id="task_b")
        response = op.success({})

    payload = response
    assert payload["request_id"]

    records = _extract_events(caplog, "test_update")
    success_record = next(r for r in records if getattr(r, "event", None) == "tool_success")
    assert success_record.dag_id == "dag_a"
    assert success_record.task_id == "task_b"


@pytest.fixture
def registry_env(monkeypatch: pytest.MonkeyPatch) -> None:
    reset_registry_cache()
    examples = Path(__file__).resolve().parent.parent / "examples" / "instances.yaml"
    monkeypatch.setenv("AIRFLOW_MCP_INSTANCES_FILE", str(examples))
    monkeypatch.setenv("AIRFLOW_INSTANCE_DATA_STG_USERNAME", "user")
    monkeypatch.setenv("AIRFLOW_INSTANCE_DATA_STG_PASSWORD", "pass")
    monkeypatch.setenv("AIRFLOW_INSTANCE_ML_STG_USERNAME", "user")
    monkeypatch.setenv("AIRFLOW_INSTANCE_ML_STG_PASSWORD", "pass")
    yield
    reset_registry_cache()
