from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, Literal

import yaml
from pydantic import BaseModel, Field, ValidationError

from .config import AirflowServerConfig

_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")


def _expand_env_value(value: str) -> str:
    def _repl(match: re.Match[str]) -> str:
        var = match.group(1)
        if var not in os.environ:
            raise ValueError(
                f"Missing required environment variable '{var}' referenced in registry YAML"
            )
        return os.environ[var]

    if "${" in value:
        return _ENV_PATTERN.sub(_repl, value)
    return value


def _expand_env(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _expand_env(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_expand_env(v) for v in obj]
    if isinstance(obj, str):
        return _expand_env_value(obj)
    return obj


class BasicAuthConfig(BaseModel):
    type: Literal["basic"] = "basic"
    username: str
    password: str


class BearerAuthConfig(BaseModel):
    type: Literal["bearer"] = "bearer"
    token: str


AuthConfig = Annotated[BasicAuthConfig | BearerAuthConfig, Field(discriminator="type")]


class InstanceConfig(BaseModel):
    host: str
    api_version: str = "v1"
    verify_ssl: bool = True
    auth: AuthConfig


@dataclass(frozen=True)
class InstanceDescriptor:
    key: str
    host: str
    api_version: str
    verify_ssl: bool
    auth_type: str  # redacted detail


class InstanceRegistry(BaseModel):
    instances: dict[str, InstanceConfig]
    default_instance: str | None = None

    def describe_instance(self, key: str) -> InstanceDescriptor:
        cfg = self.instances[key]
        return InstanceDescriptor(
            key=key,
            host=cfg.host,
            api_version=cfg.api_version,
            verify_ssl=cfg.verify_ssl,
            auth_type=cfg.auth.type,
        )


def load_registry_from_yaml(
    file_path: str, default_instance: str | None = None
) -> InstanceRegistry:
    path = Path(file_path)
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"Instances file not found: {file_path}")

    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    if not isinstance(raw, dict):
        raise ValueError("Invalid registry YAML: expected a mapping of instances")

    # Apply environment variable substitution (${VAR})
    raw_expanded = _expand_env(raw)

    # Normalize and validate
    instances: dict[str, InstanceConfig] = {}
    for key, value in raw_expanded.items():
        if not isinstance(value, dict):
            raise ValueError(f"Invalid instance config for '{key}': expected mapping")
        try:
            instances[key] = InstanceConfig(**value)
        except ValidationError as e:
            # surface a compact error
            raise ValueError(f"Invalid configuration for instance '{key}': {e}") from e

    if not instances:
        raise ValueError("No instances configured in registry YAML")

    if default_instance is not None and default_instance not in instances:
        raise ValueError(
            f"Default instance '{default_instance}' not found in registry. Known: {', '.join(instances.keys())}"
        )

    return InstanceRegistry(instances=instances, default_instance=default_instance)


# Cached registry for runtime use (env-aware on first access)
_registry: InstanceRegistry | None = None


def reset_registry_cache() -> None:
    """For tests: clear the cached registry."""
    global _registry
    _registry = None


def get_registry() -> InstanceRegistry:
    """Load and cache the instance registry, using env-aware settings.

    Reads AIRFLOW_MCP_INSTANCES_FILE and AIRFLOW_MCP_DEFAULT_INSTANCE from env or settings.
    """
    global _registry
    if _registry is not None:
        return _registry

    settings = AirflowServerConfig()
    instances_file = settings.instances_file
    default_instance = settings.default_instance

    if not instances_file:
        raise RuntimeError(
            "AIRFLOW_MCP_INSTANCES_FILE is required in Phase 1 for discovery tools and client factory"
        )

    _registry = load_registry_from_yaml(instances_file, default_instance=default_instance)
    return _registry
