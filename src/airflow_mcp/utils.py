"""Shared utility functions for the Airflow MCP server."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any


def json_safe_default(value: Any) -> Any:
    """Convert non-JSON-serializable types to JSON-safe values.

    This function is designed to be used as the `default` parameter to `json.dumps()`.
    It handles common types that may appear in Airflow API responses:
    - datetime -> ISO-8601 string
    - Enum -> its value (or string representation)
    - Objects with to_dict() -> call to_dict() and recursively process
    - All others -> string representation

    Usage:
        json.dumps(data, default=json_safe_default)

    Args:
        value: A non-serializable value that json.dumps encountered

    Returns:
        A JSON-serializable representation of the value
    """
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return getattr(value, "value", str(value))
    if hasattr(value, "to_dict"):
        try:
            return value.to_dict()
        except Exception:
            return str(value)
    return str(value)


def json_safe_recursive(value: Any) -> Any:
    """Recursively convert a data structure to JSON-safe values.

    Unlike json_safe_default which is called by json.dumps for non-serializable objects,
    this function recursively traverses the entire data structure and converts all
    values upfront. This is more comprehensive but also more expensive.

    Handles:
    - None, primitives (str, int, float, bool) -> passthrough
    - datetime -> ISO-8601 string
    - Enum -> its value (or string)
    - Objects with to_dict() -> recurse into the dict
    - dict -> recurse into keys and values
    - list, tuple, set -> recurse into elements (converted to list)
    - All others -> string representation

    Args:
        value: Any Python value

    Returns:
        A JSON-serializable representation of the value
    """
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return getattr(value, "value", str(value))
    if hasattr(value, "to_dict"):
        try:
            return json_safe_recursive(value.to_dict())
        except Exception:
            return str(value)
    if isinstance(value, dict):
        return {k: json_safe_recursive(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe_recursive(v) for v in value]
    return str(value)
