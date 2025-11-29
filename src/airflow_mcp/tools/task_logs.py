from __future__ import annotations

import ast
import re
from typing import Any

from ..client_factory import get_client_factory
from ..errors import AirflowToolError
from ..observability import operation_logger
from ..url_utils import build_airflow_ui_url, resolve_and_validate
from ..validation import (
    validate_dag_id,
    validate_dag_run_id,
    validate_task_id,
)
from ._common import ApiException, _coerce_int, _raise_api_error

_factory = get_client_factory()


def _filter_logs(
    raw_log: str,
    filter_level: str | None,
    context_lines: int | None,
    tail_lines: int | None,
    max_bytes: int,
) -> tuple[str, bool, dict[str, int]]:
    """Filter and truncate logs. Returns (filtered_log, truncated, stats).

    Order of operations:
    1) Tail extraction (if requested)
    2) Content filtering (by level)
    3) Context expansion (symmetric)
    4) Size cap (UTF-8 byte limit)
    """
    lines = raw_log.splitlines()
    original_count = len(lines)

    # Step 1: Tail extraction
    if tail_lines is not None and tail_lines >= 0:
        if tail_lines == 0:
            return (
                "",
                False,
                {
                    "bytes_returned": 0,
                    "original_lines": original_count,
                    "returned_lines": 0,
                    "match_count": 0,
                },
            )
        lines = lines[-tail_lines:]

    # Step 2: Content filtering
    match_count = 0
    if filter_level:
        patterns: dict[str, re.Pattern[str]] = {
            "error": re.compile(r"\b(ERROR|CRITICAL|FATAL|Exception|Traceback)\b", re.I),
            "warning": re.compile(r"\b(WARN(ING)?|ERROR|CRITICAL|FATAL)\b", re.I),
            "info": re.compile(r"\b(INFO|WARN|ERROR|CRITICAL)\b", re.I),
        }
        pattern = patterns.get(filter_level)
        if pattern is not None:
            matched_indices = [i for i, line in enumerate(lines) if pattern.search(line)]
            match_count = len(matched_indices)

            # Step 3: Context expansion (symmetric)
            if context_lines and matched_indices:
                expanded_indices: set[int] = set()
                for idx in matched_indices:
                    start = max(0, idx - context_lines)
                    end = min(len(lines), idx + context_lines + 1)
                    expanded_indices.update(range(start, end))
                lines = [lines[i] for i in sorted(expanded_indices)]
            else:
                lines = [lines[i] for i in matched_indices]

    # Step 4: Size cap (UTF-8)
    result = "\n".join(lines)
    truncated = False
    if len(result.encode()) > max_bytes:
        result = result.encode()[:max_bytes].decode(errors="ignore")
        truncated = True

    stats = {
        "bytes_returned": len(result.encode()),
        "original_lines": original_count,
        "returned_lines": len(lines),
        "match_count": match_count,
    }
    return result, truncated, stats


def _normalize_log_host(host: Any) -> str:
    """Normalize hostnames for log segment headers."""

    if host is None:
        return "unknown-host"
    host_text = str(host).strip()
    return host_text or "unknown-host"


def _flatten_log_segments(segments: Any) -> str:
    """Flatten host-segmented logs into a single text blob."""

    if not isinstance(segments, (list, tuple)):
        return str(segments or "")

    parts: list[str] = []
    for idx, raw_segment in enumerate(segments):
        host: Any = None
        text: Any = ""
        if isinstance(raw_segment, (list, tuple)):
            if len(raw_segment) >= 2:
                host, text = raw_segment[0], raw_segment[1]
            elif len(raw_segment) == 1:
                host, text = raw_segment[0], ""
        else:
            text = raw_segment

        if idx:
            parts.append("")

        normalized_host = _normalize_log_host(host)
        parts.append(f"--- [{normalized_host}] ---")
        text_value = "" if text is None else str(text)
        if text_value:
            parts.append(text_value.rstrip("\n"))

    return "\n".join(parts).strip("\n")


def _coerce_log_text(response: Any) -> str:
    """Coerce Airflow log responses into a single string."""

    if isinstance(response, str):
        # Detect python-repr style list of tuples (common in some Airflow versions)
        # e.g. "[('host', 'log'), ...]"
        if response.strip().startswith("[('"):
            try:
                # Safe evaluation of python literals
                parsed = ast.literal_eval(response)
                if isinstance(parsed, (list, tuple)):
                    return _flatten_log_segments(parsed)
            except (ValueError, SyntaxError):
                pass  # Not a valid literal, treat as raw string
        return response

    content = getattr(response, "content", response)
    if isinstance(content, bytes):
        return content.decode("utf-8", errors="replace")
    if isinstance(content, (list, tuple)):
        return _flatten_log_segments(content)
    if content is None:
        return ""
    return str(content)


def get_task_instance_logs(
    instance: str | None = None,
    ui_url: str | None = None,
    dag_id: str | None = None,
    dag_run_id: str | None = None,
    task_id: str | None = None,
    try_number: int | None = None,
    # New parameters
    filter_level: str | None = None,
    context_lines: int | None = None,
    tail_lines: int | None = None,
    max_bytes: int = 100_000,
) -> str:
    """Fetch task instance logs for a given attempt with optional filtering.

    Parameters
    - instance | ui_url: Target instance selection
    - dag_id, dag_run_id, task_id, try_number: Required identifiers
    - filter_level: one of "error" | "warning" | "info" (optional)
    - context_lines: include N lines before/after matches when filtering (optional)
    - tail_lines: extract last N lines prior to filtering (optional)
    - max_bytes: hard cap on returned payload size in bytes (default 100KB)

    Returns
    - JSON: {
        "log": str,
        "truncated": bool,
        "auto_tailed": bool,
        "bytes_returned": int,
        "original_lines": int,
        "returned_lines": int,
        "match_count": int,
        "meta": {"try_number": int, "filters": {...}},
        "ui_url": str
      }
    or { "error": "..." }
    """
    with operation_logger(
        "airflow_get_task_instance_logs",
        instance=instance,
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        try_number=try_number,
    ) as op:
        resolved = resolve_and_validate(ui_url=ui_url, instance=instance)
        dag_id_value = dag_id or resolved.dag_id
        dag_run_id_value = dag_run_id or resolved.dag_run_id
        task_id_value = task_id or resolved.task_id
        try_number_value = try_number if try_number is not None else resolved.try_number
        # Coerce try_number to int for tool schema compatibility
        if try_number_value is not None and not isinstance(try_number_value, int):
            try:
                try_number_value = int(try_number_value)
            except (TypeError, ValueError) as exc:
                raise AirflowToolError(
                    "try_number must be an integer",
                    code="INVALID_INPUT",
                    context={"field": "try_number", "value": try_number_value},
                ) from exc
        dag_id_value = validate_dag_id(dag_id_value)
        dag_run_id_value = validate_dag_run_id(dag_run_id_value)
        task_id_value = validate_task_id(task_id_value)
        if (
            not dag_id_value
            or not dag_run_id_value
            or not task_id_value
            or try_number_value is None
        ):
            raise AirflowToolError(
                "Missing dag_id, dag_run_id, task_id, or try_number",
                code="INVALID_INPUT",
                context={
                    "fields": [
                        "dag_id",
                        "dag_run_id",
                        "task_id",
                        "try_number",
                    ]
                },
            )
        op.update_context(
            instance=resolved.instance,
            dag_id=dag_id_value,
            dag_run_id=dag_run_id_value,
            task_id=task_id_value,
            try_number=try_number_value,
        )
        api = _factory.get_task_instances_api(resolved.instance)

        def _call_log_method(name: str) -> Any:
            method = getattr(api, name, None)
            if method is None:
                raise AttributeError(name)
            try:
                return method(dag_id_value, dag_run_id_value, task_id_value, try_number_value)
            except ApiException as exc:
                _raise_api_error(
                    exc,
                    "Unable to fetch task instance logs",
                    context={
                        "dag_id": dag_id_value,
                        "dag_run_id": dag_run_id_value,
                        "task_id": task_id_value,
                        "try_number": try_number_value,
                        "instance": resolved.instance,
                    },
                )

        resp: Any | None = None
        for method_name in ("get_log", "get_log_for_attempt_number"):
            try:
                resp = _call_log_method(method_name)
                break
            except AttributeError:
                continue
        if resp is None:
            raise AirflowToolError(
                "Airflow client does not support task log retrieval",
                code="INTERNAL_ERROR",
                context={"api": type(api).__name__},
            )
        log_text = _coerce_log_text(resp)
        ui = build_airflow_ui_url(
            resolved.instance,
            "log",
            dag_id_value,
            dag_run_id=dag_run_id_value,
            task_id=task_id_value,
            try_number=try_number_value,
        )
        # Approximate large-log check by character count (cheap and sufficient)
        auto_tailed = False
        if (
            log_text and len(log_text) > 100_000_000
        ):  # 100M chars (approximates 100MB for typical logs)
            lines = log_text.splitlines()
            log_text = "\n".join(lines[-10000:])
            auto_tailed = True

        # Coerce and clamp numeric parameters (tolerate number/string inputs from clients)
        tail_lines_int = _coerce_int(tail_lines)
        context_lines_int = _coerce_int(context_lines)

        tail_lines_clamped = (
            None if tail_lines_int is None else max(0, min(tail_lines_int, 100_000))
        )
        context_lines_clamped = (
            None if context_lines_int is None else max(0, min(context_lines_int, 1_000))
        )

        filtered_log, truncated, stats = _filter_logs(
            log_text,
            filter_level,
            context_lines_clamped,
            tail_lines_clamped,
            max_bytes,
        )

        payload = {
            "log": filtered_log,
            "truncated": truncated,
            "auto_tailed": auto_tailed,
            **stats,
            "meta": {
                "try_number": try_number_value,
                "filters": {
                    "filter_level": filter_level,
                    "context_lines": context_lines_clamped,
                    "tail_lines": tail_lines_clamped,
                    "max_bytes": max_bytes,
                },
            },
            "ui_url": ui,
        }
        return op.success(payload)
