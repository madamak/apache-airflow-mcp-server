"""Error handling utilities for Airflow MCP.

- ``AirflowToolError``: unified exception for expected/user-facing failures with explicit codes.
- ``handle_errors``: wraps server-exposed tool functions to log via ``OperationLogger`` and
  raise MCP ``ToolError`` with a compact JSON payload (`code`, `message`, `request_id`, optional context).

Usage:
- Apply ``@handle_errors`` to server tool wrappers (not business logic) so failures raise
  an MCP ``ToolError`` (`isError=true`) with sanitized payloads.
- Business logic should raise ``AirflowToolError`` (or subclasses) for validation failures
  or other anticipated problems.
- Unexpected exceptions are masked as ``INTERNAL_ERROR`` while retaining the original stack trace in logs.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from functools import wraps
from typing import Any, Final

from fastmcp.exceptions import ToolError

from .observability import get_current_operation_logger, get_last_operation_logger

logger = logging.getLogger(__name__)

_UNEXPECTED_ERROR_MESSAGE: Final[str] = "Unexpected error; see logs with request_id"


class AirflowToolError(ValueError):
    """Raised for expected/user-facing errors within Airflow MCP tools."""

    def __init__(self, message: str, *, code: str, context: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.context = context or {}


def handle_errors(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except AirflowToolError as exc:
            current_op = get_current_operation_logger()
            op = current_op or get_last_operation_logger()
            request_id = getattr(op, "request_id", None)
            payload: dict[str, Any] = {
                "code": exc.code,
                "message": str(exc),
                "request_id": request_id,
            }
            if exc.context:
                payload["context"] = exc.context

            if current_op is not None:
                current_op.error(str(exc), error_type=exc.code)
                payload["request_id"] = getattr(op, "request_id", request_id)
            else:
                logger.warning(
                    "tool_error",
                    extra={
                        "event": "tool_error",
                        "code": exc.code,
                        "tool_name": func.__name__,
                    },
                )

            raise ToolError(json.dumps(payload, ensure_ascii=False)) from None
        except ToolError:
            raise
        except Exception:  # pragma: no cover - compact envelope
            current_op = get_current_operation_logger()
            op = current_op or get_last_operation_logger()
            request_id = getattr(op, "request_id", None)

            logger.error(
                "unexpected_tool_error",
                extra={"tool_name": func.__name__, "request_id": request_id},
                exc_info=True,
            )

            if current_op is not None:
                current_op.error(
                    _UNEXPECTED_ERROR_MESSAGE,
                    error_type="INTERNAL_ERROR",
                    level=logging.ERROR,
                    exc_info=True,
                )
                request_id = getattr(op, "request_id", request_id)

            payload = {
                "code": "INTERNAL_ERROR",
                "message": _UNEXPECTED_ERROR_MESSAGE,
                "request_id": request_id,
            }
            raise ToolError(json.dumps(payload, ensure_ascii=False)) from None

    return wrapper


__all__ = ["AirflowToolError", "handle_errors"]
