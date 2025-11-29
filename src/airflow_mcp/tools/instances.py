from __future__ import annotations

from typing import Any

from ..errors import AirflowToolError
from ..observability import operation_logger
from ..registry import get_registry
from ..url_utils import parse_airflow_ui_url
from ..validation import validate_instance_key


def list_instances() -> str:
    """List configured Airflow instances and default.

    Returns
    - JSON: { "instances": [str], "default_instance": str | null }
    """
    with operation_logger("airflow_list_instances") as op:
        reg = get_registry()
        payload = {
            "instances": sorted(reg.instances.keys()),
            "default_instance": reg.default_instance,
        }
        return op.success(payload)


def describe_instance(instance: str) -> str:
    """Describe a configured instance (no secrets).

    Parameters
    - instance: Instance key

    Returns
    - JSON: { "instance", "host", "api_version", "verify_ssl", "auth_type" } or { "error": "..." }
    """
    instance = validate_instance_key(instance)
    with operation_logger("airflow_describe_instance", instance=instance) as op:
        reg = get_registry()
        if instance not in reg.instances:
            raise AirflowToolError(
                f"Unknown instance '{instance}'",
                code="NOT_FOUND",
                context={"instance": instance},
            )
        desc = reg.describe_instance(instance)
        payload = {
            "instance": desc.key,
            "host": desc.host,
            "api_version": desc.api_version,
            "verify_ssl": desc.verify_ssl,
            "auth_type": desc.auth_type,
        }
        return op.success(payload)


def resolve_url(url: str) -> str:
    """Resolve an Airflow UI URL to identifiers and instance.

    Parameters
    - url: Airflow UI URL

    Returns
    - JSON: { "instance", "dag_id"?, "dag_run_id"?, "task_id"?, "try_number"?, "route" } or { "error": "..." }
    """
    with operation_logger("airflow_resolve_url") as op:
        result = parse_airflow_ui_url(url)
        op.update_context(instance=result.instance, route=result.route)
        payload: dict[str, Any] = {
            "instance": result.instance,
            "dag_id": result.dag_id,
            "dag_run_id": result.dag_run_id,
            "task_id": result.task_id,
            "try_number": result.try_number,
            "route": result.route,
        }
        return op.success(payload)
