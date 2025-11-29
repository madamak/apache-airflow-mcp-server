from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from .registry import get_registry

logger = logging.getLogger(__name__)


@dataclass
class AirflowApiBundle:
    api_client: Any
    config: Any


def _import_airflow_client() -> tuple[Any, Any, Any]:
    """Import airflow client Configuration, ApiClient, and apis package lazily.

    Returns (Configuration, ApiClient, apis)
    """
    from airflow_client.client import ApiClient, Configuration, apis

    return Configuration, ApiClient, apis


class AirflowClientFactory:
    """Factory creating and caching ApiClient per instance key."""

    def __init__(self) -> None:
        self._cache: dict[str, AirflowApiBundle] = {}

    def _build_base_url(self, host: str, api_version: str) -> str:
        # Airflow 2.x (API v1): the generated client expects base_url to include "/api/{version}".
        host_stripped = host.rstrip("/")
        return f"{host_stripped}/api/{api_version}"

    def _set_bearer_auth(self, client_config: Any, token: str) -> None:
        """Configure bearer token on the Airflow client configuration (experimental).

        We favor explicit Authorization headers (widely supported across client versions) and
        fall back to other fields when available. All assignments are best-effort to avoid
        coupling to a specific SDK version.
        """

        auth_header = f"Bearer {token}"

        default_headers = getattr(client_config, "default_headers", None)
        if isinstance(default_headers, dict):
            default_headers["Authorization"] = auth_header

        # Common pattern in the generated client for OAuth2/bearer tokens
        if hasattr(client_config, "access_token"):
            client_config.access_token = token

        api_key = getattr(client_config, "api_key", None)
        api_key_prefix = getattr(client_config, "api_key_prefix", None)
        if isinstance(api_key, dict):
            api_key["authorization"] = token
            if isinstance(api_key_prefix, dict):
                api_key_prefix["authorization"] = "Bearer"

    def get_api_client(self, instance: str) -> Any:
        if instance in self._cache:
            return self._cache[instance].api_client

        reg = get_registry()
        if instance not in reg.instances:
            raise ValueError(f"Unknown instance '{instance}'")
        inst = reg.instances[instance]

        Configuration, ApiClient, _ = _import_airflow_client()  # noqa: N806
        base_url = self._build_base_url(inst.host, inst.api_version)

        client_config = Configuration(host=base_url)
        if inst.auth.type == "basic":
            client_config.username = inst.auth.username
            client_config.password = inst.auth.password
        elif inst.auth.type == "bearer":
            logger.warning("Bearer token auth is experimental; prefer basic auth when possible")
            self._set_bearer_auth(client_config, inst.auth.token)
        else:  # pragma: no cover - guarded by Pydantic discriminated union
            raise ValueError(f"Unsupported auth type: {inst.auth.type}")
        # SSL
        try:
            client_config.verify_ssl = inst.verify_ssl  # type: ignore[attr-defined]
        except AttributeError:
            logger.debug(
                "SSL verification configuration not supported by this Airflow client version"
            )

        api_client = ApiClient(client_config)
        self._cache[instance] = AirflowApiBundle(api_client=api_client, config=client_config)
        return api_client

    def get_dags_api(self, instance: str) -> Any:
        _, _, apis = _import_airflow_client()
        return apis.DAGApi(self.get_api_client(instance))

    def get_dag_runs_api(self, instance: str) -> Any:
        _, _, apis = _import_airflow_client()
        return apis.DAGRunApi(self.get_api_client(instance))

    def get_task_instances_api(self, instance: str) -> Any:
        _, _, apis = _import_airflow_client()
        return apis.TaskInstanceApi(self.get_api_client(instance))

    def get_dataset_events_api(self, instance: str) -> Any:
        _, _, apis = _import_airflow_client()
        return apis.DatasetApi(self.get_api_client(instance))


_global_factory: AirflowClientFactory | None = None


def get_client_factory() -> AirflowClientFactory:
    """Get the singleton AirflowClientFactory instance."""
    global _global_factory
    if _global_factory is None:
        _global_factory = AirflowClientFactory()
    return _global_factory
