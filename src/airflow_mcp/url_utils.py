from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import parse_qs, quote, unquote, urlparse

from .errors import AirflowToolError
from .registry import get_registry
from .validation import (
    validate_dag_id,
    validate_dag_run_id,
    validate_instance_key,
    validate_task_id,
)


@dataclass(frozen=True)
class ResolvedUrl:
    instance: str
    route: str  # grid | graph | dag_run | task | log | unknown
    dag_id: str | None = None
    dag_run_id: str | None = None
    task_id: str | None = None
    try_number: int | None = None


def _encode_segment(value: str) -> str:
    """Percent-encode identifier segments with no safe characters."""
    return quote(value, safe="", encoding="utf-8", errors="strict")


def _encode_query(params: dict[str, str | None]) -> str:
    """Encode query parameters without translating '+' into spaces."""
    encoded: list[str] = []
    for key, value in params.items():
        if value is None:
            continue
        encoded.append(f"{quote(key, safe='')}={_encode_segment(value)}")
    return "&".join(encoded)


def _resolve_instance_key_from_host(host: str) -> str | None:
    reg = get_registry()
    for key, inst in reg.instances.items():
        try:
            reg_host = urlparse(inst.host).hostname or ""
            if reg_host == host:
                return key
        except Exception:
            pass
    return None


def parse_airflow_ui_url(url: str) -> ResolvedUrl:
    try:
        parsed = urlparse(url)
    except Exception:
        raise AirflowToolError(
            "Invalid URL",
            code="INVALID_INPUT",
            context={"value": url},
        ) from None

    scheme = (parsed.scheme or "").lower()
    if not scheme:
        raise AirflowToolError(
            f"ui_url must be a full http(s) URL, got '{url}'",
            code="INVALID_INPUT",
            context={"value": url},
        )

    if scheme not in {"http", "https"}:
        raise AirflowToolError(
            "ui_url must start with http or https",
            code="INVALID_INPUT",
            context={"scheme": parsed.scheme},
        )

    host = parsed.hostname or ""
    if not host:
        raise AirflowToolError(
            "Missing hostname in URL",
            code="INVALID_INPUT",
            context={"value": url},
        )

    instance_key = _resolve_instance_key_from_host(host)
    if not instance_key:
        raise AirflowToolError(
            f"Unknown instance host '{host}'; ensure it matches one of airflow_list_instances().",
            code="NOT_FOUND",
            context={"host": host},
        )
    instance_key = validate_instance_key(instance_key)

    path = parsed.path or ""
    query = parse_qs(parsed.query or "")
    segments = [s for s in path.split("/") if s]

    dag_id: str | None = None
    dag_run_id: str | None = None
    task_id: str | None = None
    try_number: int | None = None
    route = "unknown"

    def q(name: str) -> str | None:
        vals = query.get(name)
        return vals[0] if vals else None

    if len(segments) >= 2 and segments[0] == "dags":
        dag_id = validate_dag_id(unquote(segments[1]))
        if len(segments) >= 3 and segments[2] in {"grid", "graph"}:
            route = segments[2]
            r = q("dag_run_id")
            dag_run_id = validate_dag_run_id(r)
        elif len(segments) >= 3 and segments[2] == "dagRuns":
            if len(segments) >= 4:
                dag_run_id = validate_dag_run_id(unquote(segments[3]))
                route = "dag_run"
                if len(segments) >= 8 and segments[4] == "taskInstances" and segments[6] == "logs":
                    task_id = validate_task_id(unquote(segments[5]))
                    try:
                        try_number = int(segments[7])
                    except ValueError:
                        try_number = None
                    route = "log"
        elif len(segments) >= 3 and segments[2] == "task":
            route = "task"
            r_task = q("task_id")
            r_run = q("dag_run_id")
            task_id = validate_task_id(r_task)
            dag_run_id = validate_dag_run_id(r_run)

    return ResolvedUrl(
        instance=instance_key,
        route=route,
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        try_number=try_number,
    )


def build_airflow_ui_url(
    instance: str,
    route: str,
    dag_id: str,
    dag_run_id: str | None = None,
    task_id: str | None = None,
    try_number: int | None = None,
) -> str:
    instance = validate_instance_key(instance)
    dag_id = validate_dag_id(dag_id)
    if not dag_id:
        raise AirflowToolError(
            "Missing dag_id",
            code="INVALID_INPUT",
            context={"field": "dag_id"},
        )

    reg = get_registry()
    if instance not in reg.instances:
        raise AirflowToolError(
            f"Unknown instance '{instance}'",
            code="NOT_FOUND",
            context={"instance": instance},
        )

    base = reg.instances[instance].host.rstrip("/")
    encoded_dag_id = _encode_segment(dag_id)
    if route in {"grid", "graph"}:
        dag_run_id = validate_dag_run_id(dag_run_id)
        path = f"/dags/{encoded_dag_id}/{route}"
        qs = _encode_query({"dag_run_id": dag_run_id})
        return f"{base}{path}{('?' + qs) if qs else ''}"

    if route == "dag_run" and dag_run_id:
        dag_run_id = validate_dag_run_id(dag_run_id)
        return f"{base}/dags/{encoded_dag_id}/dagRuns/{_encode_segment(dag_run_id)}"

    if route == "task" and task_id:
        dag_run_id = validate_dag_run_id(dag_run_id)
        task_id = validate_task_id(task_id)
        qs = _encode_query({"task_id": task_id, "dag_run_id": dag_run_id})
        return f"{base}/dags/{encoded_dag_id}/task{('?' + qs) if qs else ''}"

    if route == "log" and dag_run_id and task_id and try_number is not None:
        dag_run_id = validate_dag_run_id(dag_run_id)
        task_id = validate_task_id(task_id)
        return (
            f"{base}/dags/{encoded_dag_id}/dagRuns/{_encode_segment(dag_run_id)}"
            f"/taskInstances/{_encode_segment(task_id)}/logs/{try_number}"
        )

    raise AirflowToolError(
        f"Unsupported route '{route}' or missing identifiers",
        code="INVALID_INPUT",
        context={
            "route": route,
            "dag_id": dag_id,
            "dag_run_id": dag_run_id,
            "task_id": task_id,
            "try_number": try_number,
        },
    )


def resolve_and_validate(ui_url: str | None, instance: str | None) -> ResolvedUrl:
    """Resolve instance and identifiers from inputs with precedence and mismatch checks.

    Rules:
    - If both provided: hosts/instances must match → return parsed result; else raise ``AirflowToolError``
    - If only ui_url: parse and return
    - If only instance: validate instance exists; return minimal ``ResolvedUrl``
    - If neither: raise ``AirflowToolError``
    """
    if instance:
        instance = validate_instance_key(instance)

    if ui_url and instance:
        parsed = parse_airflow_ui_url(ui_url)
        if parsed.instance != instance:
            raise AirflowToolError(
                "INSTANCE_MISMATCH",
                code="INVALID_INPUT",
                context={"ui_url_instance": parsed.instance, "param_instance": instance},
            )
        return parsed

    if ui_url and not instance:
        return parse_airflow_ui_url(ui_url)

    if instance and not ui_url:
        reg = get_registry()
        if instance not in reg.instances:
            raise AirflowToolError(
                f"Unknown instance '{instance}'",
                code="NOT_FOUND",
                context={"instance": instance},
            )
        return ResolvedUrl(instance=instance, route="unknown")

    raise AirflowToolError("MISSING_TARGET", code="INVALID_INPUT")
