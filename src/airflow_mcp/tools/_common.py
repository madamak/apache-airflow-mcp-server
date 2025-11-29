from __future__ import annotations

import json
from datetime import datetime
from importlib import import_module
from typing import Any, NoReturn

from ..errors import AirflowToolError

try:  # pragma: no cover - exercised indirectly via runtime import
    ApiException = import_module("airflow_client.client.exceptions").ApiException
except Exception:  # pragma: no cover - fallback when airflow client isn't installed

    class ApiException(Exception):  # type: ignore[override]
        def __init__(
            self, status: int | None = None, reason: str | None = None, body: str | None = None
        ) -> None:
            super().__init__(reason or "Airflow API error")
            self.status = status
            self.reason = reason
            self.body = body


def _parse_iso_datetime(name: str, value: str | None) -> str | None:
    """Validate optional ISO-8601 timestamp strings.

    Parameters
    - name: User-facing name to reuse in error messages
    - value: Raw value supplied by the user/tool caller

    Returns the trimmed string when valid, ``None`` when the input is null-ish,
    and raises ``AirflowToolError`` when the value is empty or not ISO-8601 compliant.
    """
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise AirflowToolError(
            f"Invalid {name}",
            code="INVALID_INPUT",
            context={"field": name, "value": value},
        )
    text = value.strip()
    try:
        datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:  # pragma: no cover - defensive (format mismatch)
        raise AirflowToolError(
            f"Invalid {name}; expected ISO-8601 datetime",
            code="INVALID_INPUT",
            context={"field": name, "value": text},
        ) from exc
    return text


def _normalize_conf(conf: Any) -> dict[str, Any] | None:
    """Normalise ``conf`` payloads to a JSON-compatible mapping.

    Accepts ``None`` (returns ``None``), dictionaries, or JSON strings that decode
    to dictionaries. Any other shape raises ``AirflowToolError`` so the caller can
    surface a clear validation message to the user.
    """
    if conf is None:
        return None
    data = conf
    if isinstance(conf, str):
        try:
            data = json.loads(conf)
        except json.JSONDecodeError as exc:
            raise AirflowToolError(
                "conf must be valid JSON",
                code="INVALID_INPUT",
                context={"field": "conf"},
            ) from exc
    if not isinstance(data, dict):
        raise AirflowToolError(
            "conf must be a JSON object",
            code="INVALID_INPUT",
            context={"field": "conf", "type": type(data).__name__},
        )
    return data


def _coerce_int(value: Any) -> int | None:
    """Best-effort conversion of a value to ``int`` without raising."""

    if isinstance(value, bool):  # bool is subclass of int; treat separately
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _coerce_datetime(value: Any) -> datetime | None:
    """Parse ISO-8601-ish values into ``datetime`` when possible."""

    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _trim_detail(value: Any, limit: int = 200) -> str | None:
    """Return a compact string representation of API error details."""

    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except Exception:
            value = str(value)
    text = value if isinstance(value, str) else str(value)
    text = text.strip()
    if not text:
        return None
    if len(text) > limit:
        return f"{text[:limit]}..."
    return text


def _raise_api_error(
    exc: ApiException,
    message: str,
    *,
    context: dict[str, Any] | None = None,
) -> NoReturn:
    """Normalize Airflow API exceptions to ``AirflowToolError`` codes."""

    status = getattr(exc, "status", None)
    detail = _trim_detail(getattr(exc, "reason", None) or getattr(exc, "body", None))

    friendly = message
    if status:
        friendly = f"{friendly} (HTTP {status})"
    if detail:
        friendly = f"{friendly}: {detail}"

    payload_context = dict(context or {})
    if status is not None:
        payload_context["status"] = status
    if detail:
        payload_context.setdefault("detail", detail)

    code = "INTERNAL_ERROR"
    if status == 404:
        code = "NOT_FOUND"
    elif isinstance(status, int) and 400 <= status < 500:
        code = "INVALID_INPUT"

    raise AirflowToolError(
        friendly,
        code=code,
        context=payload_context or None,
    ) from exc


def _build_dag_run_body(**kwargs: Any) -> Any:
    """Create an Airflow client ``DAGRun`` model (or plain dict fallback)."""
    payload = {k: v for k, v in kwargs.items() if v is not None}
    try:
        from airflow_client.client.model.dag_run import DAGRun  # type: ignore

        return DAGRun(**payload)
    except Exception:
        return payload


def _build_clear_task_instance_body(**kwargs: Any) -> Any:
    """Create ``ClearTaskInstance`` model payload while tolerating client absence."""
    payload = {k: v for k, v in kwargs.items() if v is not None}
    try:
        module = import_module("airflow_client.client.model.clear_task_instance")
        clear_task_instance_cls = module.ClearTaskInstance
        return clear_task_instance_cls(**payload)
    except Exception:
        return payload


def _build_clear_dag_run_body(**kwargs: Any) -> Any:
    """Create ``ClearDagRun`` model payload while tolerating client absence.

    Falls back to a plain dict if the model is not available in the installed
    ``apache-airflow-client`` version.
    """
    payload = {k: v for k, v in kwargs.items() if v is not None}
    try:
        from airflow_client.client.model.clear_dag_run import (  # type: ignore
            ClearDagRun,
        )

        return ClearDagRun(**payload)
    except Exception:
        return payload


def _build_dag_body(**kwargs: Any) -> Any:
    """Create ``DAG`` model payload or a plain dict when the model is unavailable."""
    payload = {k: v for k, v in kwargs.items() if v is not None}
    try:
        from airflow_client.client.model.dag import DAG  # type: ignore

        return DAG(**payload)
    except Exception:
        return payload
