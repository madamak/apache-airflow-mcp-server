from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from airflow_mcp import client_factory as cf
from airflow_mcp.errors import AirflowToolError
from airflow_mcp.registry import reset_registry_cache


class _BearerConfiguration:
    """Minimal config stub that mirrors the attributes we touch for bearer auth."""

    def __init__(self, host: str, **_: object) -> None:
        self.host = host
        self.default_headers: dict[str, str] = {}
        self.api_key: dict[str, str] = {}
        self.api_key_prefix: dict[str, str] = {}
        self.username = None
        self.password = None
        self.verify_ssl = True


class _BearerApiClient:
    def __init__(self, config: _BearerConfiguration) -> None:
        self.configuration = config


def test_fetch_jwt_rejects_non_http_url_before_network(monkeypatch: pytest.MonkeyPatch):
    urlopen = MagicMock()
    monkeypatch.setattr(cf.urllib.request, "urlopen", urlopen)

    with pytest.raises(AirflowToolError) as exc:
        cf._fetch_jwt_token(
            "file:///tmp/airflow",
            "user",
            "password",
            verify_ssl=True,
            timeout=10,
        )

    assert exc.value.code == "CONFIG_ERROR"
    urlopen.assert_not_called()


def test_fetch_jwt_posts_credentials_to_valid_https_url(monkeypatch: pytest.MonkeyPatch):
    response = MagicMock()
    response.read.return_value = b'{"access_token":"jwt-token"}'
    context_manager = MagicMock()
    context_manager.__enter__.return_value = response
    urlopen = MagicMock(return_value=context_manager)
    monkeypatch.setattr(cf.urllib.request, "urlopen", urlopen)

    token = cf._fetch_jwt_token(
        "https://airflow.example.com/",
        "user",
        "password",
        verify_ssl=True,
        timeout=10,
    )

    assert token == "jwt-token"
    request = urlopen.call_args.args[0]
    assert request.full_url == "https://airflow.example.com/auth/token"
    assert request.method == "POST"
    assert request.data == b'{"username": "user", "password": "password"}'


def test_client_factory_sets_bearer_header_experimental(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    """Bearer auth is experimental; ensure Authorization header is applied when selected."""

    reset_registry_cache()
    monkeypatch.setattr(cf, "_global_factory", None)

    yaml_text = (
        "ml-prod:\n"
        "  host: https://airflow.ml-prod.example.com/\n"
        "  api_version: v1\n"
        "  verify_ssl: true\n"
        "  auth:\n"
        "    type: bearer\n"
        "    token: secret-token\n"
    )
    registry_file = tmp_path / "instances.yaml"
    registry_file.write_text(yaml_text, encoding="utf-8")

    monkeypatch.setenv("AIRFLOW_MCP_INSTANCES_FILE", str(registry_file))
    monkeypatch.delenv("AIRFLOW_MCP_DEFAULT_INSTANCE", raising=False)

    monkeypatch.setattr(
        cf,
        "_import_airflow_client",
        lambda: (_BearerConfiguration, _BearerApiClient, SimpleNamespace()),
    )

    factory = cf.get_client_factory()
    factory._cache.clear()

    api_client = factory.get_api_client("ml-prod")
    cfg = api_client.configuration

    assert cfg.host.endswith("/api/v1")
    assert cfg.default_headers["Authorization"] == "Bearer secret-token"
    assert cfg.api_key["authorization"] == "secret-token"
    assert cfg.api_key_prefix["authorization"] == "Bearer"
    assert cfg.username is None and cfg.password is None
    assert cfg.verify_ssl is True

    reset_registry_cache()
