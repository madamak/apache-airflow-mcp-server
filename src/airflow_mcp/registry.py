from __future__ import annotations

import importlib.util
import os
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any, Literal

import yaml
from pydantic import BaseModel, Field, ValidationError, field_validator

from .config import AirflowServerConfig
from .errors import AirflowToolError
from .validation import validate_instance_key

_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")


def installed_client_major() -> int | None:
    """Best-effort detection of the installed apache-airflow-client major version.

    Returns 2 for the Airflow 2.x codegen (has ``airflow_client.client.apis``),
    3 for the Airflow 3.x codegen, or None when the client is not installed.
    """
    try:
        if importlib.util.find_spec("airflow_client.client") is None:
            return None
        return 2 if importlib.util.find_spec("airflow_client.client.apis") else 3
    except (ImportError, ModuleNotFoundError, ValueError):  # pragma: no cover - defensive
        return None


@lru_cache(maxsize=1)
def default_api_version() -> str:
    """API version to assume when an instance doesn't specify one.

    Inferred from the installed apache-airflow-client major so a plain
    ``pip install`` works against the matching Airflow out of the box.
    """
    return "v2" if installed_client_major() == 3 else "v1"


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
    api_version: str | None = None  # None → inferred from the installed client major
    verify_ssl: bool = True
    auth: AuthConfig

    @field_validator("api_version")
    @classmethod
    def _normalize_api_version(cls, value: str | None) -> str | None:
        if value is None:
            return None
        text = str(value).strip().lower()
        if text not in {"v1", "v2"}:
            raise ValueError(
                f"api_version must be 'v1' (Airflow 2) or 'v2' (Airflow 3), got '{value}'"
            )
        return text

    @property
    def resolved_api_version(self) -> str:
        """The effective API version ('v1' or 'v2'), inferring a default when unset."""
        return self.api_version or default_api_version()

    @property
    def api_family(self) -> str:
        """API family for version branching: 'v1' (Airflow 2) or 'v2' (Airflow 3)."""
        return self.resolved_api_version


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
            api_version=cfg.resolved_api_version,
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
            # Compact error without input values: auth blocks carry credentials,
            # and this message ends up in logs.
            details = "; ".join(
                f"{'.'.join(str(loc) for loc in err['loc'])}: {err['msg']}"
                for err in e.errors(include_input=False, include_url=False)
            )
            raise ValueError(f"Invalid configuration for instance '{key}': {details}") from e

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
    default_api_version.cache_clear()


def build_single_instance_registry(settings: AirflowServerConfig) -> InstanceRegistry:
    """Build a one-instance registry from AIRFLOW_MCP_HOST/USERNAME/PASSWORD/TOKEN settings."""
    auth: BasicAuthConfig | BearerAuthConfig
    if settings.token:
        auth = BearerAuthConfig(token=settings.token)
    elif settings.username and settings.password:
        auth = BasicAuthConfig(username=settings.username, password=settings.password)
    else:
        raise AirflowToolError(
            "AIRFLOW_MCP_HOST is set but credentials are missing: provide "
            "AIRFLOW_MCP_USERNAME and AIRFLOW_MCP_PASSWORD (basic auth) "
            "or AIRFLOW_MCP_TOKEN (bearer)",
            code="CONFIG_ERROR",
        )

    key = settings.default_instance or "default"
    try:
        validate_instance_key(key)
    except AirflowToolError as exc:
        raise AirflowToolError(
            f"Invalid AIRFLOW_MCP_DEFAULT_INSTANCE value used as instance key: {key!r}",
            code="CONFIG_ERROR",
        ) from exc
    try:
        instance = InstanceConfig(
            host=settings.host,
            api_version=settings.api_version,
            verify_ssl=settings.verify_ssl,
            auth=auth,
        )
    except ValidationError as exc:
        raise AirflowToolError(
            f"Invalid single-instance configuration: {exc}",
            code="CONFIG_ERROR",
        ) from exc
    return InstanceRegistry(instances={key: instance}, default_instance=key)


def get_registry() -> InstanceRegistry:
    """Load and cache the instance registry, using env-aware settings.

    Configuration sources, in precedence order:
    1. AIRFLOW_MCP_INSTANCES_FILE: YAML registry (multi-instance).
    2. AIRFLOW_MCP_HOST (+ USERNAME/PASSWORD or TOKEN): single-instance mode, no file needed.
    """
    global _registry
    if _registry is not None:
        return _registry

    settings = AirflowServerConfig()
    instances_file = settings.instances_file
    default_instance = settings.default_instance

    if instances_file:
        _registry = load_registry_from_yaml(instances_file, default_instance=default_instance)
    elif settings.host:
        _registry = build_single_instance_registry(settings)
    else:
        raise AirflowToolError(
            "No Airflow instances configured. Either set AIRFLOW_MCP_INSTANCES_FILE to a "
            "registry YAML (multi-instance), or set AIRFLOW_MCP_HOST with "
            "AIRFLOW_MCP_USERNAME/AIRFLOW_MCP_PASSWORD or AIRFLOW_MCP_TOKEN (single instance)",
            code="CONFIG_ERROR",
        )
    return _registry
