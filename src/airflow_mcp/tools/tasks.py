from __future__ import annotations

import inspect
import json
from collections.abc import Sequence
from typing import Any

from ..client_factory import get_client_factory
from ..errors import AirflowToolError
from ..observability import operation_logger
from ..url_utils import build_airflow_ui_url, resolve_and_validate
from ..utils import json_safe_recursive as _json_safe
from ..validation import (
    validate_dag_id,
    validate_dag_run_id,
    validate_task_id,
)
from ._common import (
    ApiException,
    _build_clear_task_instance_body,
    _coerce_datetime,
    _coerce_int,
    _parse_iso_datetime,
    _raise_api_error,
)

_factory = get_client_factory()


def _normalize_state_filters(state: Sequence[str] | str | None) -> list[str] | None:
    """Normalize state filters to a deduplicated, non-empty list of strings."""

    if state is None:
        return None
    if isinstance(state, str):
        values: Sequence[str] = [state]
    elif isinstance(state, Sequence):
        values = state
    else:
        raise AirflowToolError(
            "state must be a list of strings",
            code="INVALID_INPUT",
            context={"field": "state", "value": state},
        )

    normalized: list[str] = []
    for item in values:
        if not isinstance(item, str):
            raise AirflowToolError(
                "state filters must be strings",
                code="INVALID_INPUT",
                context={"field": "state", "value": item},
            )
        text = item.strip()
        if not text:
            raise AirflowToolError(
                "state filters cannot be empty",
                code="INVALID_INPUT",
                context={"field": "state"},
            )
        normalized.append(text)

    if not normalized:
        return None
    # Deduplicate while preserving order
    seen = dict.fromkeys(normalized)
    return list(seen.keys())


def _normalize_task_ids(task_ids: Sequence[str] | str | None) -> list[str] | None:
    """Normalize task_id filters, validating each entry."""

    if task_ids is None:
        return None
    if isinstance(task_ids, str):
        values: Sequence[str] = [task_ids]
    elif isinstance(task_ids, Sequence):
        values = task_ids
    else:
        raise AirflowToolError(
            "task_ids must be a list of strings",
            code="INVALID_INPUT",
            context={"field": "task_ids", "value": task_ids},
        )

    normalized: list[str] = []
    for item in values:
        if not isinstance(item, str):
            raise AirflowToolError(
                "task_ids entries must be strings",
                code="INVALID_INPUT",
                context={"field": "task_ids", "value": item},
            )
        validated = validate_task_id(item)
        if not validated:
            raise AirflowToolError(
                "task_ids entries cannot be empty",
                code="INVALID_INPUT",
                context={"field": "task_ids"},
            )
        normalized.append(validated)

    if not normalized:
        return None
    seen = dict.fromkeys(normalized)
    return list(seen.keys())


def list_task_instances(
    instance: str | None = None,
    ui_url: str | None = None,
    dag_id: str | None = None,
    dag_run_id: str | None = None,
    limit: int | float | str = 100,
    offset: int | float | str = 0,
    state: Sequence[str] | str | None = None,
    task_ids: Sequence[str] | str | None = None,
) -> str:
    """List task instances for a given DAG run.

    Parameters
    - instance | ui_url: Target instance selection
    - dag_id, dag_run_id: Required identifiers (unless ui_url provided)
    - limit, offset: Pagination controls
    - state: Optional list of task states to include (case-insensitive, deduplicated)
    - task_ids: Optional list of specific task IDs to include

    Returns
    - JSON: {
        "task_instances": [{"task_id", "state", "try_number", ... , "ui_url"?}],
        "count": int,
        "total_entries"?: int
      }
    """
    with operation_logger(
        "airflow_list_task_instances",
        instance=instance,
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        limit=limit,
        offset=offset,
        state=state,
        task_ids=task_ids,
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

        state_filters = _normalize_state_filters(state)
        task_id_filters = _normalize_task_ids(task_ids)

        op.update_context(
            instance=resolved.instance,
            dag_id=dag_id_value,
            dag_run_id=dag_run_id_value,
            state=state_filters,
            task_ids=task_id_filters,
        )

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

        api = _factory.get_task_instances_api(resolved.instance)
        method = api.get_task_instances
        kwargs: dict[str, Any] = {"limit": limit_int, "offset": offset_int}

        # Airflow client versions diverge here: older releases omit kwonly
        # filters such as ``state`` / ``task_ids`` entirely, while newer ones
        # accept them (or a generic **kwargs). Introspect the bound method so we
        # only forward params the installed client understands, preventing
        # ``TypeError: got an unexpected keyword`` at runtime.
        supports_state = False
        task_id_param_name: str | None = None
        try:
            signature = inspect.signature(method)
        except (TypeError, ValueError):  # pragma: no cover - defensive
            signature = None

        if signature:
            params = list(signature.parameters.values())
            accepts_kwargs = any(p.kind == p.VAR_KEYWORD for p in params)
            names = {p.name for p in params if p.name != "self"}
            supports_state = "state" in names or accepts_kwargs
            if "task_ids" in names:
                task_id_param_name = "task_ids"
            elif "task_id" in names:
                task_id_param_name = "task_id"
            elif accepts_kwargs:
                task_id_param_name = "task_ids"
        else:  # pragma: no cover - best-effort default
            supports_state = True

        if state_filters and supports_state:
            kwargs["state"] = state_filters
        if task_id_filters and task_id_param_name:
            kwargs[task_id_param_name] = task_id_filters

        try:
            resp = method(dag_id_value, dag_run_id_value, **kwargs)
        except TypeError:
            # Fallback: Client might not support the passed kwargs (state/task_ids)
            # despite our best-effort introspection. Retry without filters and
            # rely on the in-memory filtering below.
            # We still need limit/offset if supported, but standardizing on minimal
            # args for safety is better than crashing.
            # Re-build minimal kwargs
            safe_kwargs = {"limit": limit_int, "offset": offset_int}
            resp = method(dag_id_value, dag_run_id_value, **safe_kwargs)

        except ApiException as exc:
            _raise_api_error(
                exc,
                "Unable to list task instances",
                context={
                    "dag_id": dag_id_value,
                    "dag_run_id": dag_run_id_value,
                    "instance": resolved.instance,
                },
            )

        state_filter_set = {s.lower() for s in state_filters} if state_filters else None
        task_id_filter_set = set(task_id_filters) if task_id_filters else None

        task_instances: list[dict[str, Any]] = []
        for ti in getattr(resp, "task_instances", []) or []:
            task_id_value = getattr(ti, "task_id", None)
            state_value = getattr(ti, "state", None)
            if state_filter_set and (state_value or "").lower() not in state_filter_set:
                continue
            if task_id_filter_set and task_id_value not in task_id_filter_set:
                continue
            try_num = getattr(ti, "try_number", None)
            ui = (
                build_airflow_ui_url(
                    resolved.instance,
                    "log",
                    dag_id_value,
                    dag_run_id=dag_run_id_value,
                    task_id=task_id_value,
                    try_number=try_num,
                )
                if (task_id_value and try_num is not None)
                else None
            )
            task_instances.append(
                {
                    "task_id": task_id_value,
                    "state": state_value,
                    "try_number": try_num,
                    "start_date": getattr(ti, "start_date", None),
                    "end_date": getattr(ti, "end_date", None),
                    "ui_url": ui,
                }
            )

        payload: dict[str, Any] = {
            "task_instances": task_instances,
            "count": len(task_instances),
        }
        total_entries = getattr(resp, "total_entries", None)
        if total_entries is not None:
            payload["total_entries"] = total_entries
        if state_filters or task_id_filters:
            payload["filters"] = {
                "state": state_filters,
                "task_ids": task_id_filters,
            }
        return op.success(_json_safe(payload))


def get_task_instance(
    instance: str | None = None,
    ui_url: str | None = None,
    dag_id: str | None = None,
    dag_run_id: str | None = None,
    task_id: str | None = None,
    include_rendered: bool = False,
    max_rendered_bytes: int | float | str = 100_000,
) -> str:
    """Return task instance metadata, task config, attempt summary, optional rendered fields."""

    with operation_logger(
        "airflow_get_task_instance",
        instance=instance,
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        include_rendered=include_rendered,
    ) as op:
        resolved = resolve_and_validate(ui_url=ui_url, instance=instance)
        dag_id_value = validate_dag_id(dag_id or resolved.dag_id)
        dag_run_id_value = validate_dag_run_id(dag_run_id or resolved.dag_run_id)
        task_id_value = validate_task_id(task_id or resolved.task_id)

        if not dag_id_value or not dag_run_id_value or not task_id_value:
            raise AirflowToolError(
                "Missing dag_id, dag_run_id, or task_id",
                code="INVALID_INPUT",
                context={"fields": ["dag_id", "dag_run_id", "task_id"]},
            )

        if include_rendered:
            max_bytes_int = _coerce_int(max_rendered_bytes)
            if max_bytes_int is None or max_bytes_int <= 0:
                raise AirflowToolError(
                    "max_rendered_bytes must be a positive integer",
                    code="INVALID_INPUT",
                    context={"field": "max_rendered_bytes", "value": max_rendered_bytes},
                )

        op.update_context(
            instance=resolved.instance,
            dag_id=dag_id_value,
            dag_run_id=dag_run_id_value,
            task_id=task_id_value,
        )

        ti_api = _factory.get_task_instances_api(resolved.instance)
        try:
            task_instance = ti_api.get_task_instance(dag_id_value, dag_run_id_value, task_id_value)
        except ApiException as exc:
            _raise_api_error(
                exc,
                "Unable to fetch task instance",
                context={
                    "dag_id": dag_id_value,
                    "dag_run_id": dag_run_id_value,
                    "task_id": task_id_value,
                    "instance": resolved.instance,
                },
            )
        except AttributeError as exc:  # pragma: no cover - defensive
            raise AirflowToolError(
                "Airflow client does not support get_task_instance",
                code="INTERNAL_ERROR",
                context={"method": "get_task_instance", "api": type(ti_api).__name__},
            ) from exc

        try_number_value = _coerce_int(getattr(task_instance, "try_number", None))
        start_raw = getattr(task_instance, "start_date", None)
        end_raw = getattr(task_instance, "end_date", None)
        start_dt = _coerce_datetime(start_raw)
        end_dt = _coerce_datetime(end_raw)
        duration_ms: int | None = None
        if start_dt and end_dt:
            duration_ms = int(max((end_dt - start_dt).total_seconds(), 0) * 1000)

        task_instance_payload = {
            "task_id": task_id_value,
            "state": getattr(task_instance, "state", None),
            "try_number": try_number_value,
            "start_date": _json_safe(start_raw),
            "end_date": _json_safe(end_raw),
            "duration_ms": duration_ms,
            "hostname": getattr(task_instance, "hostname", None),
            "operator": getattr(task_instance, "operator", None),
            "queue": getattr(task_instance, "queue", None),
            "pool": getattr(task_instance, "pool", None),
            "priority_weight": _coerce_int(getattr(task_instance, "priority_weight", None)),
        }

        dags_api = _factory.get_dags_api(resolved.instance)
        task_details: Any | None = None
        try:
            task_details = dags_api.get_task(dag_id_value, task_id_value)
        except AttributeError:  # pragma: no cover - defensive
            task_details = None
        except Exception:
            task_details = None

        def _extract(source: Any | None, name: str) -> Any:
            if source is None:
                return None
            if isinstance(source, dict):
                return source.get(name)
            return getattr(source, name, None)

        retries_configured = _coerce_int(_extract(task_details, "retries"))
        retry_delay_value = _json_safe(_extract(task_details, "retry_delay"))
        owner_value = _extract(task_details, "owner")
        if owner_value is None:
            owners_list = _extract(task_details, "owners")
            if isinstance(owners_list, (list, tuple, set)):
                owner_value = ", ".join(str(o) for o in owners_list)
            elif owners_list is not None:
                owner_value = str(owners_list)

        task_config_payload = {
            "retries": retries_configured,
            "retry_delay": retry_delay_value,
            "owner": owner_value,
        }

        retries_consumed: int | None = None
        if try_number_value is not None:
            retries_consumed = max(try_number_value - 1, 0)
        retries_remaining: int | None = None
        if retries_configured is not None and retries_consumed is not None:
            retries_remaining = max(retries_configured - retries_consumed, 0)

        attempts_payload = {
            "try_number": try_number_value,
            "retries_configured": retries_configured,
            "retries_consumed": retries_consumed,
            "retries_remaining": retries_remaining,
        }

        rendered_payload: dict[str, Any] | None = None
        if include_rendered:
            fields_raw = getattr(task_instance, "rendered_fields", None)
            if hasattr(fields_raw, "to_dict"):
                fields_raw = fields_raw.to_dict()
            fields_value: Any
            if fields_raw is None:
                fields_value = {}
            else:
                fields_value = fields_raw
            if not isinstance(fields_value, dict):
                fields_value = {"value": _json_safe(fields_value)}
            sanitized_fields_value = _json_safe(fields_value)
            rendered_json = json.dumps(sanitized_fields_value, ensure_ascii=False)
            rendered_bytes = rendered_json.encode("utf-8")
            truncated = len(rendered_bytes) > max_rendered_bytes
            if truncated:
                fields_value = {"_truncated": "Increase max_rendered_bytes"}
                rendered_bytes = json.dumps(fields_value, ensure_ascii=False).encode("utf-8")
                fields_output = _json_safe(fields_value)
            else:
                fields_output = sanitized_fields_value
            rendered_payload = {
                "fields": fields_output,
                "bytes_returned": len(rendered_bytes),
                "truncated": truncated,
            }

        grid_url = build_airflow_ui_url(
            resolved.instance, "grid", dag_id_value, dag_run_id=dag_run_id_value
        )
        log_url: str | None = None
        if try_number_value is not None:
            try:
                log_url = build_airflow_ui_url(
                    resolved.instance,
                    "log",
                    dag_id_value,
                    dag_run_id=dag_run_id_value,
                    task_id=task_id_value,
                    try_number=try_number_value,
                )
            except AirflowToolError:
                log_url = None

        payload: dict[str, Any] = {
            "task_instance": _json_safe(task_instance_payload),
            "task_config": _json_safe(task_config_payload),
            "attempts": _json_safe(attempts_payload),
            "ui_url": _json_safe({"grid": grid_url, "log": log_url}),
        }
        if rendered_payload is not None:
            payload["rendered_fields"] = _json_safe(rendered_payload)

        return op.success(payload)


def clear_task_instances(
    instance: str | None = None,
    ui_url: str | None = None,
    dag_id: str | None = None,
    task_ids: Sequence[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    include_subdags: bool | None = None,
    include_parentdag: bool | None = None,
    include_upstream: bool | None = None,
    include_downstream: bool | None = None,
    include_future: bool | None = None,
    include_past: bool | None = None,
    dry_run: bool | None = None,
    reset_dag_runs: bool | None = None,
) -> str:
    """Clear task instances with optional filters."""
    with operation_logger(
        "airflow_clear_task_instances",
        instance=instance,
        dag_id=dag_id,
        task_ids=list(task_ids) if isinstance(task_ids, Sequence) else task_ids,
    ) as op:
        resolved = resolve_and_validate(ui_url=ui_url, instance=instance)
        dag_id_value = validate_dag_id(dag_id or resolved.dag_id)
        if not dag_id_value:
            raise AirflowToolError(
                "Missing dag_id",
                code="INVALID_INPUT",
                context={"field": "dag_id"},
            )

        normalized_task_ids: list[str] | None = None
        if task_ids is not None:
            if isinstance(task_ids, str):
                raise AirflowToolError(
                    "task_ids must be a list of task identifiers",
                    code="INVALID_INPUT",
                    context={"field": "task_ids", "value": task_ids},
                )
            normalized_task_ids = []
            for tid in task_ids:
                if not isinstance(tid, str):
                    raise AirflowToolError(
                        "task_ids must contain strings",
                        code="INVALID_INPUT",
                        context={"field": "task_ids", "value": tid},
                    )
                validated = validate_task_id(tid)
                if not validated:
                    raise AirflowToolError(
                        "Invalid task_id provided",
                        code="INVALID_INPUT",
                        context={"field": "task_id", "value": tid},
                    )
                normalized_task_ids.append(validated)

        op.update_context(instance=resolved.instance, dag_id=dag_id_value)
        if normalized_task_ids is not None:
            op.update_context(task_ids=normalized_task_ids)

        start_date_value = _parse_iso_datetime("start_date", start_date)
        end_date_value = _parse_iso_datetime("end_date", end_date)

        body = _build_clear_task_instance_body(
            task_ids=normalized_task_ids,
            start_date=start_date_value,
            end_date=end_date_value,
            include_subdags=include_subdags,
            include_parentdag=include_parentdag,
            include_upstream=include_upstream,
            include_downstream=include_downstream,
            include_future=include_future,
            include_past=include_past,
            dry_run=dry_run if dry_run is not None else False,
            reset_dag_runs=reset_dag_runs,
        )
        api = _factory.get_dags_api(resolved.instance)
        # Airflow client uses POST endpoint for clearing task instances
        # Version notes:
        # - Airflow 2.5.x Python client (openapi): method is post_clear_task_instances and
        #   expects the body kwarg named 'clear_task_instances' (plural).
        # - Some older client codegens used 'clear_task_instance' (singular).
        # - Even older clients use method name 'clear_task_instances' (no 'post_' prefix).
        #   Keep fallbacks until all environments are on uniform client version.
        try:
            response = api.post_clear_task_instances(dag_id_value, clear_task_instances=body)
        except (TypeError, AttributeError):
            try:
                # Fallback for older client keyword name
                response = api.post_clear_task_instances(dag_id_value, clear_task_instance=body)
            except (TypeError, AttributeError):
                # Fallback for oldest client method name (no 'post_' prefix)
                response = api.clear_task_instances(dag_id_value, clear_task_instance=body)
        response_payload = response.to_dict() if hasattr(response, "to_dict") else response
        cleared_payload = response_payload
        if isinstance(response_payload, dict) and "cleared" in response_payload:
            cleared_payload = response_payload["cleared"]
        payload = {"dag_id": dag_id_value, "cleared": cleared_payload}
        return op.success(_json_safe(payload))
