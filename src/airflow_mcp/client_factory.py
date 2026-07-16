from __future__ import annotations

import base64
import json
import logging
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

from .config import config as _server_config
from .errors import AirflowToolError
from .registry import get_registry

logger = logging.getLogger(__name__)


@dataclass
class AirflowApiBundle:
    api_client: Any
    config: Any
    # monotonic deadline after which the JWT must be re-fetched; None = never expires
    token_expires_at: float | None = None


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


def _fetch_jwt_token(
    host: str, username: str, password: str, *, verify_ssl: bool, timeout: int
) -> str:
    """Obtain a JWT access token from an Airflow 3 API server (POST /auth/token)."""
    url = f"{host.rstrip('/')}/auth/token"
    try:
        parsed = urllib.parse.urlsplit(url)
        hostname = parsed.hostname
    except ValueError as exc:
        raise AirflowToolError(
            "Airflow host must be a valid HTTP(S) URL",
            code="CONFIG_ERROR",
        ) from exc
    if parsed.scheme.lower() not in {"http", "https"} or not hostname:
        raise AirflowToolError(
            "Airflow host must be a full HTTP(S) URL",
            code="CONFIG_ERROR",
        )
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
        # Bandit cannot infer that the configured URL was restricted to HTTP(S).
        with urllib.request.urlopen(  # nosec B310
            request, timeout=timeout, context=ssl_context
        ) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        context: dict[str, Any] = {}
        if isinstance(exc, urllib.error.HTTPError):
            context["status"] = exc.code
        raise AirflowToolError(
            f"Failed to obtain JWT token from {url}: {exc}. "
            "Airflow 3 (api_version v2) authenticates with JWT; ensure the credentials are "
            "valid and the auth manager exposes POST /auth/token.",
            code="AUTH_FAILED",
            context=context or None,
        ) from exc
    token = payload.get("access_token") if isinstance(payload, dict) else None
    if not token:
        raise AirflowToolError(
            f"JWT token response from {url} did not include 'access_token'",
            code="AUTH_FAILED",
        )
    return token


_TOKEN_REFRESH_MARGIN_SECONDS = 60
_TOKEN_REFRESH_MIN_SECONDS = 30


def _jwt_expiry_epoch(token: str) -> float | None:
    """Best-effort read of a JWT's ``exp`` claim (no signature verification)."""
    try:
        payload_b64 = token.split(".")[1]
        payload_b64 += "=" * (-len(payload_b64) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload_b64))
        exp = claims.get("exp")
        return float(exp) if exp is not None else None
    except Exception:
        return None


def _token_refresh_deadline(token: str) -> float:
    """Monotonic deadline for refreshing ``token``.

    The configured refresh interval is an upper bound; when the JWT carries an
    ``exp`` claim, refresh a margin before it so a shorter server-side lifetime
    ([api_auth] jwt_expiration_time) cannot outlive our schedule.
    """
    deadline = time.monotonic() + _server_config.token_refresh_seconds
    exp = _jwt_expiry_epoch(token)
    if exp is not None:
        lifetime = exp - time.time()
        refresh_in = lifetime - _TOKEN_REFRESH_MARGIN_SECONDS
        if refresh_in < _TOKEN_REFRESH_MIN_SECONDS:
            # Short-lived token: the fixed margin would schedule the refresh at or
            # past expiry. Refresh at half the remaining lifetime instead, with a
            # 1s floor so a bogus/expired exp cannot cause a hot refresh loop.
            refresh_in = max(lifetime / 2, 1.0)
        deadline = min(deadline, time.monotonic() + refresh_in)
    return deadline


class AirflowClientFactory:
    """Factory creating and caching ApiClient per instance key."""

    def __init__(self) -> None:
        self._cache: dict[str, AirflowApiBundle] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    def _instance_lock(self, instance: str) -> threading.Lock:
        with self._locks_guard:
            lock = self._locks.get(instance)
            if lock is None:
                lock = self._locks[instance] = threading.Lock()
            return lock

    def _get_instance_config(self, instance: str) -> Any:
        reg = get_registry()
        if instance not in reg.instances:
            raise AirflowToolError(
                f"Unknown instance '{instance}'",
                code="NOT_FOUND",
                context={"instance": instance},
            )
        return reg.instances[instance]

    def get_api_family(self, instance: str) -> str:
        """Return the API family for an instance: "v1" (Airflow 2) or "v2" (Airflow 3)."""
        return self._get_instance_config(instance).api_family

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
        # Airflow 2.x (API v1): the generated client expects base_url to include "/api/v1".
        base_url = f"{inst.host.rstrip('/')}/api/{inst.resolved_api_version}"

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
        # Airflow 3.x client: generated endpoint paths already include "/api/v2".
        base_url = inst.host.rstrip("/")

        client_config = client_mod.Configuration(host=base_url)
        token_expires_at: float | None = None
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
            token_expires_at = _token_refresh_deadline(client_config.access_token)
        else:  # pragma: no cover - guarded by Pydantic discriminated union
            raise ValueError(f"Unsupported auth type: {inst.auth.type}")
        try:
            client_config.verify_ssl = inst.verify_ssl  # type: ignore[attr-defined]
        except AttributeError:  # pragma: no cover - defensive
            pass
        return AirflowApiBundle(
            api_client=client_mod.ApiClient(client_config),
            config=client_config,
            token_expires_at=token_expires_at,
        )

    def _import_v1_or_raise(self) -> tuple[Any, Any, Any]:
        try:
            return _import_airflow_client()
        except ImportError as exc:
            raise AirflowToolError(
                "This instance targets the Airflow 2 API (api_version v1) but the installed "
                "apache-airflow-client is not the 2.x series. "
                "Install it with: pip install 'apache-airflow-client<3'",
                code="CONFIG_ERROR",
            ) from exc

    def _import_v3_or_raise(self) -> Any:
        try:
            client_mod = _import_airflow_client_v3()
        except ImportError as exc:
            raise AirflowToolError(
                "This instance targets the Airflow 3 API (api_version v2) but "
                "apache-airflow-client is not importable. "
                "Install it with: pip install 'apache-airflow-client>=3,<4'",
                code="CONFIG_ERROR",
            ) from exc
        # The 3.x codegen exports API classes at the top level; the 2.x codegen does not.
        if not hasattr(client_mod, "DAGApi"):
            raise AirflowToolError(
                "This instance targets the Airflow 3 API (api_version v2) but the installed "
                "apache-airflow-client is the 2.x series. "
                "Install it with: pip install 'apache-airflow-client>=3,<4'",
                code="CONFIG_ERROR",
            )
        return client_mod

    def _refresh_jwt_in_place(self, inst: Any, bundle: AirflowApiBundle) -> None:
        """Replace the expired JWT on a cached bundle, keeping its connection pool.

        A transient refresh failure keeps the old token (it may still be accepted)
        and retries on the next call after a short backoff. A credential rejection
        (401/403 from the token endpoint) fails the tool call instead: silently
        re-posting bad credentials risks account lockout and hides the root cause.
        """
        try:
            bundle.config.access_token = _fetch_jwt_token(
                inst.host,
                inst.auth.username,
                inst.auth.password,
                verify_ssl=inst.verify_ssl,
                timeout=_server_config.timeout_seconds,
            )
            bundle.token_expires_at = _token_refresh_deadline(bundle.config.access_token)
        except AirflowToolError as exc:
            bundle.token_expires_at = time.monotonic() + 60
            if exc.context.get("status") in (401, 403):
                raise
            logger.warning("JWT refresh failed; keeping previous token: %s", exc)

    def _bundle(self, instance: str) -> AirflowApiBundle:
        inst = self._get_instance_config(instance)

        with self._instance_lock(instance):
            cached = self._cache.get(instance)
            if cached is not None:
                if (
                    cached.token_expires_at is not None
                    and time.monotonic() > cached.token_expires_at
                ):
                    logger.info("Refreshing JWT token for instance '%s'", instance)
                    self._refresh_jwt_in_place(inst, cached)
                return cached

            bundle = (
                self._build_bundle_v2(inst)
                if inst.api_family == "v2"
                else self._build_bundle_v1(inst)
            )
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
