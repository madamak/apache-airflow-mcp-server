from __future__ import annotations

import json
import logging
import uuid
from contextlib import AbstractContextManager
from contextvars import ContextVar, Token
from time import perf_counter
from typing import Any

from airflow_mcp.utils import json_safe_default

logger = logging.getLogger(__name__)


_current_logger: ContextVar[OperationLogger | None] = ContextVar(
    "airflow_operation_logger", default=None
)
_last_logger: ContextVar[OperationLogger | None] = ContextVar(
    "airflow_operation_logger_last", default=None
)


class OperationLogger(AbstractContextManager["OperationLogger"]):
    """Context manager that emits structured logs for tool executions."""

    def __init__(self, tool_name: str, **context: Any) -> None:
        self.tool_name = tool_name
        self.context: dict[str, Any] = {
            key: value for key, value in context.items() if value is not None
        }
        provided_request_id = self.context.pop("request_id", None)
        self.request_id = provided_request_id or uuid.uuid4().hex
        self._start: float | None = None
        self._completed = False
        self._token: Token[OperationLogger | None] | None = None

    def __enter__(self) -> OperationLogger:
        self._start = perf_counter()
        self._token = _current_logger.set(self)
        _last_logger.set(self)
        logger.info("tool_start", extra=self._log_fields(event="tool_start"))
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        elapsed_ms = self._elapsed_ms()
        if exc is not None and not self._completed:
            logger.warning(
                "tool_error",
                extra=self._log_fields(event="tool_error", duration_ms=elapsed_ms, error=str(exc)),
            )
        elif not self._completed:
            logger.info(
                "tool_success",
                extra=self._log_fields(
                    event="tool_success",
                    duration_ms=elapsed_ms,
                    response_bytes=0,
                    note="completed_without_payload",
                ),
            )
        if self._token is not None:
            _current_logger.reset(self._token)
        _last_logger.set(self)
        return False

    def success(self, payload: dict[str, Any]) -> dict[str, Any]:
        result_payload = dict(payload)
        result_payload["request_id"] = self.request_id
        # Be robust to non-JSON-serializable types that may slip through tool payloads
        # (e.g., datetime objects coming from client models).
        response_json = json.dumps(result_payload, ensure_ascii=False, default=json_safe_default)
        elapsed_ms = self._elapsed_ms()
        logger.info(
            "tool_success",
            extra=self._log_fields(
                event="tool_success",
                duration_ms=elapsed_ms,
                response_bytes=len(response_json.encode("utf-8")),
            ),
        )
        self._completed = True
        _last_logger.set(self)
        return result_payload

    def error(
        self,
        message: str,
        *,
        error_type: str | None = None,
        level: int = logging.WARNING,
        exc_info: Any | None = None,
    ) -> str:
        payload = {"error": message, "request_id": self.request_id}
        elapsed_ms = self._elapsed_ms()
        logger.log(
            level,
            "tool_error",
            extra=self._log_fields(
                event="tool_error",
                duration_ms=elapsed_ms,
                error=message,
                error_type=error_type,
            ),
            exc_info=exc_info,
        )
        self._completed = True
        _last_logger.set(self)
        return json.dumps(payload, ensure_ascii=False)

    def update_context(self, **context: Any) -> None:
        for key, value in context.items():
            if value is not None:
                self.context[key] = value

    def _elapsed_ms(self) -> float:
        if self._start is None:
            self._start = perf_counter()
        return round((perf_counter() - self._start) * 1000, 2)

    def _log_fields(self, **extra: Any) -> dict[str, Any]:
        fields = {"tool_name": self.tool_name, "request_id": self.request_id}
        fields.update(self.context)
        fields.update(extra)
        return fields


def operation_logger(tool_name: str, **context: Any) -> OperationLogger:
    """Helper for creating an :class:`OperationLogger` instance."""

    return OperationLogger(tool_name, **context)


def get_current_operation_logger() -> OperationLogger | None:
    return _current_logger.get()


def get_last_operation_logger() -> OperationLogger | None:
    return _last_logger.get()


__all__ = [
    "OperationLogger",
    "operation_logger",
    "get_current_operation_logger",
    "get_last_operation_logger",
]
