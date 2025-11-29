"""Identifier validation utilities for Airflow MCP tools."""

from __future__ import annotations

import re
from re import Pattern

from .errors import AirflowToolError

INSTANCE_KEY_PATTERN: Pattern[str] = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
# DAG IDs follow Airflow's validate_key helper: letters, numbers, underscore, dot, dash
DAG_ID_PATTERN: Pattern[str] = re.compile(r"^[A-Za-z0-9_.-]+$")
DAG_RUN_ID_PATTERN: Pattern[str] = re.compile(r"^[A-Za-z0-9_.:+-]+$")
TASK_ID_PATTERN: Pattern[str] = re.compile(r"^[A-Za-z0-9_.+-]+$")
DATASET_URI_PATTERN: Pattern[str] = re.compile(r"^[A-Za-z0-9_.:/+-]+$")


def _validate_optional(name: str, value: str | None, pattern: Pattern[str]) -> str | None:
    if value is None:
        return None
    if not pattern.fullmatch(value):
        raise AirflowToolError(
            f"Invalid {name}",
            code="INVALID_INPUT",
            context={"field": name, "value": value},
        )
    return value


def validate_instance_key(value: str) -> str:
    if not value:
        raise AirflowToolError(
            "Missing instance",
            code="INVALID_INPUT",
            context={"field": "instance"},
        )
    if not INSTANCE_KEY_PATTERN.fullmatch(value):
        raise AirflowToolError(
            "Invalid instance",
            code="INVALID_INPUT",
            context={"field": "instance", "value": value},
        )
    return value


def validate_dag_id(value: str | None) -> str | None:
    return _validate_optional("dag_id", value, DAG_ID_PATTERN)


def validate_dag_run_id(value: str | None) -> str | None:
    return _validate_optional("dag_run_id", value, DAG_RUN_ID_PATTERN)


def validate_task_id(value: str | None) -> str | None:
    return _validate_optional("task_id", value, TASK_ID_PATTERN)


def validate_dataset_uri(value: str) -> str:
    if not value:
        raise AirflowToolError(
            "Missing dataset_uri",
            code="INVALID_INPUT",
            context={"field": "dataset_uri"},
        )
    if not DATASET_URI_PATTERN.fullmatch(value):
        raise AirflowToolError(
            "Invalid dataset_uri",
            code="INVALID_INPUT",
            context={"field": "dataset_uri", "value": value},
        )
    return value
