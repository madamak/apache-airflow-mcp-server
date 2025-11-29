from __future__ import annotations

from typing import Any

from ..client_factory import get_client_factory
from ..errors import AirflowToolError
from ..observability import OperationLogger, operation_logger
from ..url_utils import build_airflow_ui_url, resolve_and_validate
from ..utils import json_safe_recursive as _json_safe
from ..validation import validate_dag_id, validate_dag_run_id
from ._common import (
    _build_dag_body,
    _build_dag_run_body,
    _coerce_int,
    _normalize_conf,
    _parse_iso_datetime,
)

_factory = get_client_factory()


def list_dags(
    instance: str | None = None,
    ui_url: str | None = None,
    limit: int | float | str = 100,
    offset: int | float | str = 0,
) -> str:
    """List DAGs with basic metadata and UI URL.

    Parameters
    - instance | ui_url: Target instance selection (ui_url takes precedence)
    - limit, offset: Pagination

    Returns
    - JSON: { "dags": [{"dag_id", "is_paused", "ui_url"}], "count": int } or { "error": "..." }
    """
    with operation_logger(
        "airflow_list_dags", instance=instance, ui_url=ui_url, limit=limit, offset=offset
    ) as op:
        resolved = resolve_and_validate(ui_url=ui_url, instance=instance)
        op.update_context(instance=resolved.instance, dag_id=resolved.dag_id)
        # Coerce pagination params to non-negative integers
        limit_int = _coerce_int(limit)
        offset_int = _coerce_int(offset)
        if limit_int is None:
            limit_int = 100
        if offset_int is None:
            offset_int = 0
        if limit_int < 0:
            limit_int = 0
        if offset_int < 0:
            offset_int = 0

        api = _factory.get_dags_api(resolved.instance)
        resp = api.get_dags(limit=limit_int, offset=offset_int)
        dags = []
        for d in getattr(resp, "dags", []) or []:
            dag_id = getattr(d, "dag_id", None)
            ui = build_airflow_ui_url(resolved.instance, "grid", dag_id) if dag_id else None
            dags.append(
                {"dag_id": dag_id, "is_paused": getattr(d, "is_paused", None), "ui_url": ui}
            )
        payload = {"dags": dags, "count": getattr(resp, "total_entries", len(dags))}
        return op.success(payload)


def get_dag(
    instance: str | None = None, ui_url: str | None = None, dag_id: str | None = None
) -> str:
    """Get DAG details and UI URL.

    Parameters
    - instance | ui_url: Target instance selection
    - dag_id: Required if ui_url not provided

    Returns
    - JSON: { "dag": object, "ui_url": str } or { "error": "..." }
    """
    with operation_logger("airflow_get_dag", instance=instance, dag_id=dag_id) as op:
        resolved = resolve_and_validate(ui_url=ui_url, instance=instance)
        dag_id_value = dag_id or resolved.dag_id
        dag_id_value = validate_dag_id(dag_id_value)
        if not dag_id_value:
            raise AirflowToolError(
                "Missing dag_id",
                code="INVALID_INPUT",
                context={"field": "dag_id"},
            )
        op.update_context(instance=resolved.instance, dag_id=dag_id_value)
        api = _factory.get_dags_api(resolved.instance)
        d = api.get_dag(dag_id_value)
        ui = build_airflow_ui_url(resolved.instance, "grid", dag_id_value)
        payload = {
            "dag": _json_safe(d),
            "ui_url": ui,
        }
        return op.success(_json_safe(payload))


def trigger_dag(
    instance: str | None = None,
    ui_url: str | None = None,
    dag_id: str | None = None,
    dag_run_id: str | None = None,
    logical_date: str | None = None,
    conf: dict[str, Any] | str | None = None,
    note: str | None = None,
) -> str:
    """Trigger a DAG run with optional configuration.

    Parameters
    - instance | ui_url: Target instance selection
    - dag_id: DAG identifier (required if ui_url not provided)
    - dag_run_id: Optional custom run identifier
    - logical_date: ISO-8601 logical date for the run
    - conf: Optional JSON configuration (dict or JSON string)
    - note: Optional run note/comment

    Returns
    - JSON: { "dag_run_id": str, "ui_url": str, "dag_run": object }
    """
    with operation_logger(
        "airflow_trigger_dag",
        instance=instance,
        dag_id=dag_id,
        dag_run_id=dag_run_id,
    ) as op:
        resolved = resolve_and_validate(ui_url=ui_url, instance=instance)
        dag_id_value = validate_dag_id(dag_id or resolved.dag_id)
        if not dag_id_value:
            raise AirflowToolError(
                "Missing dag_id",
                code="INVALID_INPUT",
                context={"field": "dag_id"},
            )
        dag_run_id_value = validate_dag_run_id(dag_run_id)
        logical_date_value = _parse_iso_datetime("logical_date", logical_date)
        conf_obj = _normalize_conf(conf)
        if note is not None and not isinstance(note, str):
            raise AirflowToolError(
                "note must be a string",
                code="INVALID_INPUT",
                context={"field": "note", "value": note},
            )

        op.update_context(
            instance=resolved.instance,
            dag_id=dag_id_value,
            dag_run_id=dag_run_id_value,
        )

        body = _build_dag_run_body(
            dag_run_id=dag_run_id_value,
            logical_date=logical_date_value,
            conf=conf_obj,
            note=note,
        )
        api = _factory.get_dag_runs_api(resolved.instance)
        response = api.post_dag_run(dag_id_value, dag_run=body)
        dag_run_payload = response.to_dict() if hasattr(response, "to_dict") else response
        response_run_id = getattr(response, "dag_run_id", None)
        if isinstance(dag_run_payload, dict):
            response_run_id = (
                response_run_id
                or dag_run_payload.get("dag_run_id")
                or dag_run_payload.get("run_id")
            )
        response_run_id = response_run_id or dag_run_id_value
        if not response_run_id:
            raise AirflowToolError(
                "Airflow did not return a dag_run_id",
                code="INTERNAL_ERROR",
                context={"dag_id": dag_id_value},
            )
        ui = build_airflow_ui_url(
            resolved.instance, "dag_run", dag_id_value, dag_run_id=response_run_id
        )
        payload = {
            "dag_run_id": response_run_id,
            "ui_url": ui,
            "dag_run": dag_run_payload,
        }
        return op.success(_json_safe(payload))


def _set_dag_paused(
    *,
    desired_state: bool,
    instance: str | None = None,
    ui_url: str | None = None,
    dag_id: str | None = None,
    op_logger: OperationLogger | None = None,
) -> str:
    """Shared implementation for ``pause_dag`` and ``unpause_dag``."""

    def _execute(op: OperationLogger) -> str:
        resolved = resolve_and_validate(ui_url=ui_url, instance=instance)
        dag_id_value = validate_dag_id(dag_id or resolved.dag_id)
        if not dag_id_value:
            raise AirflowToolError(
                "Missing dag_id",
                code="INVALID_INPUT",
                context={"field": "dag_id"},
            )
        op.update_context(
            instance=resolved.instance, dag_id=dag_id_value, desired_state=desired_state
        )

        body = _build_dag_body(is_paused=desired_state)
        api = _factory.get_dags_api(resolved.instance)
        # Airflow OpenAPI client expects update_mask as list[str] (observed on ≥2.7 clients).
        # Older examples sometimes used a single string; list is backward compatible across
        # supported versions (2.5–2.11), so we standardize on the list form.
        response = api.patch_dag(dag_id_value, dag=body, update_mask=["is_paused"])
        dag_payload_raw = response.to_dict() if hasattr(response, "to_dict") else response
        dag_payload = _json_safe(dag_payload_raw)
        ui = build_airflow_ui_url(resolved.instance, "grid", dag_id_value)
        payload = {
            "dag_id": dag_id_value,
            "is_paused": desired_state,
            "dag": dag_payload,
            "ui_url": ui,
        }
        return op.success(_json_safe(payload))

    if op_logger is not None:
        return _execute(op_logger)

    with operation_logger(
        "airflow_set_dag_paused",
        instance=instance,
        dag_id=dag_id,
        desired_state=desired_state,
    ) as op:
        return _execute(op)


def pause_dag(
    instance: str | None = None, ui_url: str | None = None, dag_id: str | None = None
) -> str:
    """Pause DAG scheduling."""
    return _set_dag_paused(desired_state=True, instance=instance, ui_url=ui_url, dag_id=dag_id)


def unpause_dag(
    instance: str | None = None, ui_url: str | None = None, dag_id: str | None = None
) -> str:
    """Unpause DAG scheduling."""
    return _set_dag_paused(desired_state=False, instance=instance, ui_url=ui_url, dag_id=dag_id)
