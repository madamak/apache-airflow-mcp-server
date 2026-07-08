from __future__ import annotations

import importlib.util
import json
import logging
import ssl
import time
import urllib.request
from dataclasses import dataclass, field
from typing import Any

from .config import config as _server_config
from .registry import get_registry

logger = logging.getLogger(__name__)


@dataclass
class AirflowApiBundle:
    api_client: Any
    config: Any
    created_at: float = field(default_factory=time.monotonic)
    needs_token_refresh: bool = False


def _import_airflow_client() -> tuple[Any, Any, Any]:
    """Import the Airflow 2.x client Configuration, ApiClient, and apis package lazily.

    Returns (Configuration, ApiClient, apis)
    """
    from airflow_client.client import ApiClient, Configuration, apis

    return Configuration, ApiClient, apis


def _import_airflow_client_v3() -> Any:
    """Import the Airflow 3.x client module lazily.

    The 3.x codegen exposes API classes (DAGApi, DagRunApi, ...) at the top level
    of ``airflow_client.client`` and drops the 2.x ``apis`` subpackage.
    """
    import airflow_client.client as client_mod

    return client_mod


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


def _fetch_jwt_token(
    host: str, username: str, password: str, *, verify_ssl: bool, timeout: int
) -> str:
    """Obtain a JWT access token from an Airflow 3 API server (POST /auth/token)."""
    url = f"{host.rstrip('/')}/auth/token"
    body = json.dumps({"username": username, "password": password}).encode("utf-8")
    request = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/json"}, method="POST"
    )
    ssl_context = None
    if url.startswith("https"):
        ssl_context = ssl.create_default_context()
        if not verify_ssl:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
    try:
        with urllib.request.urlopen(request, timeout=timeout, context=ssl_context) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        raise RuntimeError(
            f"Failed to obtain JWT token from {url}: {exc}. "
            "Airflow 3 (api_version v2) authenticates with JWT; ensure the credentials are "
            "valid and the auth manager exposes POST /auth/token."
        ) from exc
    token = payload.get("access_token") if isinstance(payload, dict) else None
    if not token:
        raise RuntimeError(f"JWT token response from {url} did not include 'access_token'")
    return token


class AirflowClientFactory:
    """Factory creating and caching ApiClient per instance key."""

    def __init__(self) -> None:
        self._cache: dict[str, AirflowApiBundle] = {}

    def get_api_family(self, instance: str) -> str:
        """Return the API family for an instance: "v1" (Airflow 2) or "v2" (Airflow 3)."""
        reg = get_registry()
        if instance not in reg.instances:
            raise ValueError(f"Unknown instance '{instance}'")
        version = (reg.instances[instance].api_version or "v1").strip().lower()
        return "v2" if version.startswith("v2") else "v1"

    def _build_base_url(self, host: str, api_version: str) -> str:
        host_stripped = host.rstrip("/")
        if (api_version or "v1").strip().lower().startswith("v2"):
            # Airflow 3.x client: generated endpoint paths already include "/api/v2".
            return host_stripped
        # Airflow 2.x (API v1): the generated client expects base_url to include "/api/{version}".
        return f"{host_stripped}/api/{api_version}"

    def _set_bearer_auth(self, client_config: Any, token: str) -> None:
        """Configure bearer token on the Airflow client configuration.

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

    def _build_bundle_v1(self, inst: Any) -> AirflowApiBundle:
        Configuration, ApiClient, _ = self._import_v1_or_raise()  # noqa: N806
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
        try:
            client_config.verify_ssl = inst.verify_ssl  # type: ignore[attr-defined]
        except AttributeError:
            logger.debug(
                "SSL verification configuration not supported by this Airflow client version"
            )
        return AirflowApiBundle(api_client=ApiClient(client_config), config=client_config)

    def _build_bundle_v2(self, inst: Any) -> AirflowApiBundle:
        client_mod = self._import_v3_or_raise()
        base_url = self._build_base_url(inst.host, inst.api_version)

        client_config = client_mod.Configuration(host=base_url)
        needs_token_refresh = False
        if inst.auth.type == "bearer":
            client_config.access_token = inst.auth.token
        elif inst.auth.type == "basic":
            # Airflow 3 API authenticates with JWT; exchange basic credentials for a token.
            client_config.access_token = _fetch_jwt_token(
                inst.host,
                inst.auth.username,
                inst.auth.password,
                verify_ssl=inst.verify_ssl,
                timeout=_server_config.timeout_seconds,
            )
            needs_token_refresh = True
        else:  # pragma: no cover - guarded by Pydantic discriminated union
            raise ValueError(f"Unsupported auth type: {inst.auth.type}")
        try:
            client_config.verify_ssl = inst.verify_ssl  # type: ignore[attr-defined]
        except AttributeError:  # pragma: no cover - defensive
            pass
        return AirflowApiBundle(
            api_client=client_mod.ApiClient(client_config),
            config=client_config,
            needs_token_refresh=needs_token_refresh,
        )

    def _import_v1_or_raise(self) -> tuple[Any, Any, Any]:
        try:
            return _import_airflow_client()
        except ImportError as exc:
            raise RuntimeError(
                "This instance targets the Airflow 2 API (api_version v1) but the installed "
                "apache-airflow-client is not the 2.x series. "
                "Install it with: pip install 'apache-airflow-client<3'"
            ) from exc

    def _import_v3_or_raise(self) -> Any:
        client_mod = _import_airflow_client_v3()
        # The 3.x codegen exports API classes at the top level; the 2.x codegen does not.
        if not hasattr(client_mod, "DAGApi"):
            raise RuntimeError(
                "This instance targets the Airflow 3 API (api_version v2) but the installed "
                "apache-airflow-client is the 2.x series. "
                "Install it with: pip install 'apache-airflow-client>=3,<4'"
            )
        return client_mod

    def _bundle(self, instance: str) -> AirflowApiBundle:
        reg = get_registry()
        if instance not in reg.instances:
            raise ValueError(f"Unknown instance '{instance}'")
        inst = reg.instances[instance]

        cached = self._cache.get(instance)
        if cached is not None:
            expired = (
                cached.needs_token_refresh
                and time.monotonic() - cached.created_at > _server_config.token_refresh_seconds
            )
            if not expired:
                return cached
            logger.info("Refreshing JWT token for instance '%s'", instance)

        family = self.get_api_family(instance)
        bundle = self._build_bundle_v2(inst) if family == "v2" else self._build_bundle_v1(inst)
        self._cache[instance] = bundle
        return bundle

    def get_api_client(self, instance: str) -> Any:
        return self._bundle(instance).api_client

    def _get_api(self, instance: str, v1_name: str, v2_name: str) -> Any:
        if self.get_api_family(instance) == "v2":
            client_mod = self._import_v3_or_raise()
            api_cls = getattr(client_mod, v2_name)
        else:
            _, _, apis = self._import_v1_or_raise()
            api_cls = getattr(apis, v1_name)
        return api_cls(self.get_api_client(instance))

    def get_dags_api(self, instance: str) -> Any:
        return self._get_api(instance, "DAGApi", "DAGApi")

    def get_dag_runs_api(self, instance: str) -> Any:
        return self._get_api(instance, "DAGRunApi", "DagRunApi")

    def get_task_instances_api(self, instance: str) -> Any:
        return self._get_api(instance, "TaskInstanceApi", "TaskInstanceApi")

    def get_tasks_api(self, instance: str) -> Any:
        # Task definitions live on DAGApi in the 2.x client and TaskApi in the 3.x client.
        return self._get_api(instance, "DAGApi", "TaskApi")

    def get_dataset_events_api(self, instance: str) -> Any:
        # Datasets were renamed to assets in Airflow 3.
        return self._get_api(instance, "DatasetApi", "AssetApi")


_global_factory: AirflowClientFactory | None = None


def get_client_factory() -> AirflowClientFactory:
    """Get the singleton AirflowClientFactory instance."""
    global _global_factory
    if _global_factory is None:
        _global_factory = AirflowClientFactory()
    return _global_factory
