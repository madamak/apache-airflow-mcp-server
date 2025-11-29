from __future__ import annotations

from typing import Any, Literal

from ..client_factory import get_client_factory
from ..errors import AirflowToolError
from ..observability import operation_logger
from ..url_utils import build_airflow_ui_url, resolve_and_validate
from ..utils import json_safe_recursive as _json_safe
from ..validation import validate_dag_id, validate_dag_run_id
from ._common import (
    ApiException,
    _build_clear_dag_run_body,
    _coerce_int,
    _raise_api_error,
)

_factory = get_client_factory()


def list_dag_runs(
    instance: str | None = None,
    ui_url: str | None = None,
    dag_id: str | None = None,
    limit: int | float | str = 100,
    offset: int | float | str = 0,
    state: list[str] | None = None,
    order_by: Literal["start_date", "end_date", "execution_date"] | None = None,
    descending: bool = True,
) -> str:
    """List DAG runs for a DAG with per-run UI URLs.

    Parameters
    - instance | ui_url: Target instance selection
    - dag_id: Required if ui_url not provided
    - limit, offset, state: Pagination and filtering
    - order_by, descending: Optional ordering field and direction (defaults to ``execution_date`` descending)

    Returns
    - JSON: { "dag_runs": [{"dag_run_id", "state", ... , "ui_url"}], "count": int } or { "error": "..." }
    """
    with operation_logger(
        "airflow_list_dag_runs", instance=instance, dag_id=dag_id, ui_url=ui_url
    ) as op:
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
        api = _factory.get_dag_runs_api(resolved.instance)
        # Coerce pagination params
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

        kwargs: dict[str, Any] = {"limit": limit_int, "offset": offset_int}
        if state:
            kwargs["state"] = state
        allowed_fields = {"start_date", "end_date", "execution_date"}
        order_field = order_by
        order_desc = descending
        if order_by is None:
            order_field = "execution_date"
            order_desc = True
        else:
            if order_by not in allowed_fields:
                raise AirflowToolError(
                    "order_by must be one of 'start_date', 'end_date', or 'execution_date'",
                    code="INVALID_INPUT",
                    context={"field": "order_by", "value": order_by},
                )
            if not isinstance(descending, bool):
                raise AirflowToolError(
                    "descending must be a boolean value",
                    code="INVALID_INPUT",
                    context={"field": "descending", "value": descending},
                )
        order_token = f"-{order_field}" if order_desc else order_field
        kwargs["order_by"] = order_token
        op.update_context(order_by=order_field, descending=order_desc)
        try:
            resp = api.get_dag_runs(dag_id_value, **kwargs)
        except ApiException as exc:
            _raise_api_error(
                exc,
                "Unable to list DAG runs",
                context={"dag_id": dag_id_value, "instance": resolved.instance},
            )
        runs = []
        for r in getattr(resp, "dag_runs", []) or []:
            dr_id = getattr(r, "dag_run_id", None)
            ui = (
                build_airflow_ui_url(resolved.instance, "dag_run", dag_id_value, dag_run_id=dr_id)
                if dr_id
                else None
            )
            runs.append(
                {
                    "dag_run_id": dr_id,
                    "state": getattr(r, "state", None),
                    "start_date": getattr(r, "start_date", None),
                    "end_date": getattr(r, "end_date", None),
                    "ui_url": ui,
                }
            )
        payload = {
            "dag_runs": runs,
            "count": getattr(resp, "total_entries", len(runs)),
        }
        return op.success(_json_safe(payload))


def get_dag_run(
    instance: str | None = None,
    ui_url: str | None = None,
    dag_id: str | None = None,
    dag_run_id: str | None = None,
) -> str:
    """Get a DAG run and a UI URL.

    Parameters
    - instance | ui_url: Target instance selection
    - dag_id, dag_run_id: Required identifiers (unless ui_url provided)

    Returns
    - JSON: { "dag_run": object, "ui_url": str } or { "error": "..." }
    """
    with operation_logger(
        "airflow_get_dag_run", instance=instance, dag_id=dag_id, dag_run_id=dag_run_id
    ) as op:
        resolved = resolve_and_validate(ui_url=ui_url, instance=instance)
        dag_id_value = dag_id or resolved.dag_id
        dag_run_id_value = dag_run_id or resolved.dag_run_id
        dag_id_value = validate_dag_id(dag_id_value)
        dag_run_id_value = validate_dag_run_id(dag_run_id_value)
        if not dag_id_value or not dag_run_id_value:
            raise AirflowToolError(
                "Missing dag_id or dag_run_id",
                code="INVALID_INPUT",
                context={"fields": ["dag_id", "dag_run_id"]},
            )
        op.update_context(
            instance=resolved.instance, dag_id=dag_id_value, dag_run_id=dag_run_id_value
        )
        api = _factory.get_dag_runs_api(resolved.instance)
        try:
            r = api.get_dag_run(dag_id_value, dag_run_id_value)
        except ApiException as exc:
            _raise_api_error(
                exc,
                "Unable to fetch DAG run",
                context={
                    "dag_id": dag_id_value,
                    "dag_run_id": dag_run_id_value,
                    "instance": resolved.instance,
                },
            )
        ui = build_airflow_ui_url(
            resolved.instance, "dag_run", dag_id_value, dag_run_id=dag_run_id_value
        )
        payload = {
            "dag_run": _json_safe(r),
            "ui_url": ui,
        }
        return op.success(_json_safe(payload))


def clear_dag_run(
    instance: str | None = None,
    ui_url: str | None = None,
    dag_id: str | None = None,
    dag_run_id: str | None = None,
    include_subdags: bool | None = None,
    include_parentdag: bool | None = None,
    include_upstream: bool | None = None,
    include_downstream: bool | None = None,
    dry_run: bool | None = None,
    reset_dag_runs: bool | None = None,
) -> str:
    """Clear all task instances within a DAG run."""
    with operation_logger(
        "airflow_clear_dag_run",
        instance=instance,
        dag_id=dag_id,
        dag_run_id=dag_run_id,
    ) as op:
        resolved = resolve_and_validate(ui_url=ui_url, instance=instance)
        dag_id_value = validate_dag_id(dag_id or resolved.dag_id)
        dag_run_id_value = validate_dag_run_id(dag_run_id or resolved.dag_run_id)
        if not dag_id_value or not dag_run_id_value:
            raise AirflowToolError(
                "Missing dag_id or dag_run_id",
                code="INVALID_INPUT",
                context={"fields": ["dag_id", "dag_run_id"]},
            )

        op.update_context(
            instance=resolved.instance, dag_id=dag_id_value, dag_run_id=dag_run_id_value
        )

        # Build clear_dag_run body with conditional parameter support based on configuration.
        # Airflow 2.5.x may reject include_* and reset_dag_runs fields with 400 Bad Request.
        # Airflow ≥2.6 supports the full parameter set (include_subdags, include_parentdag,
        # include_upstream, include_downstream, reset_dag_runs).
        # Use AIRFLOW_MCP_ENABLE_EXTENDED_CLEAR_PARAMS=true to enable extended params.
        from ..config import config

        body_kwargs = {"dry_run": dry_run if dry_run is not None else False}
        if config.enable_extended_clear_params:
            if include_subdags is not None:
                body_kwargs["include_subdags"] = include_subdags
            if include_parentdag is not None:
                body_kwargs["include_parentdag"] = include_parentdag
            if include_upstream is not None:
                body_kwargs["include_upstream"] = include_upstream
            if include_downstream is not None:
                body_kwargs["include_downstream"] = include_downstream
            if reset_dag_runs is not None:
                body_kwargs["reset_dag_runs"] = reset_dag_runs

        body = _build_clear_dag_run_body(**body_kwargs)
        api = _factory.get_dag_runs_api(resolved.instance)
        # Airflow client expects body under 'clear_dag_run' (observed on 2.5.x client).
        # Some client codegens used 'clear_task_instances' or 'clear_task_instance'.
        # Maintain fallbacks until all instances run a uniform client version.
        try:
            response = api.clear_dag_run(dag_id_value, dag_run_id_value, clear_dag_run=body)
        except TypeError:
            try:
                response = api.clear_dag_run(
                    dag_id_value, dag_run_id_value, clear_task_instances=body
                )
            except TypeError:
                response = api.clear_dag_run(
                    dag_id_value, dag_run_id_value, clear_task_instance=body
                )
        response_payload = response.to_dict() if hasattr(response, "to_dict") else response
        cleared_payload = response_payload
        if isinstance(response_payload, dict) and "cleared" in response_payload:
            cleared_payload = response_payload["cleared"]
        payload = {
            "dag_id": dag_id_value,
            "dag_run_id": dag_run_id_value,
            "cleared": cleared_payload,
        }
        return op.success(_json_safe(payload))
