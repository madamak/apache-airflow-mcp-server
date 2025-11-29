import json
import logging

import pytest
from fastmcp.exceptions import ToolError

from airflow_mcp.errors import AirflowToolError, handle_errors
from airflow_mcp.observability import operation_logger


def test_handle_errors_wraps_airflow_tool_error(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger="airflow_mcp.errors")

    @handle_errors
    def _tool() -> str:
        with operation_logger("test_handle_errors"):
            raise AirflowToolError("Missing dag_id", code="INVALID_INPUT")

    with pytest.raises(ToolError) as exc:
        _tool()

    payload = json.loads(str(exc.value))
    assert payload["code"] == "INVALID_INPUT"
    assert payload["message"] == "Missing dag_id"
    assert payload["request_id"]


def test_handle_errors_masks_internal_errors(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger="airflow_mcp.errors")

    @handle_errors
    def _boom() -> str:
        with operation_logger("test_handle_errors_internal"):
            raise RuntimeError("secret failure")

    with pytest.raises(ToolError) as exc:
        _boom()

    payload = json.loads(str(exc.value))
    assert payload["code"] == "INTERNAL_ERROR"
    assert payload["message"] == "Unexpected error; see logs with request_id"
    assert payload["request_id"]
