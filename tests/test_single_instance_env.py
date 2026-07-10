"""Tests for single-instance configuration via environment variables (no YAML needed)."""

from pathlib import Path

import pytest
from pydantic import ValidationError
from test_tools_readonly import _payload

from airflow_mcp import tools as airflow_tools
from airflow_mcp.config import AirflowServerConfig
from airflow_mcp.errors import AirflowToolError
from airflow_mcp.registry import (
    build_single_instance_registry,
    default_api_version,
    reset_registry_cache,
)


@pytest.fixture(autouse=True)
def _fresh_registry(monkeypatch: pytest.MonkeyPatch):
    # Ensure no ambient registry/env config leaks into these tests
    for var in (
        "AIRFLOW_MCP_INSTANCES_FILE",
        "AIRFLOW_MCP_DEFAULT_INSTANCE",
        "AIRFLOW_MCP_HOST",
        "AIRFLOW_MCP_USERNAME",
        "AIRFLOW_MCP_PASSWORD",
        "AIRFLOW_MCP_TOKEN",
        "AIRFLOW_MCP_API_VERSION",
        "AIRFLOW_MCP_VERIFY_SSL",
        "AIRFLOW_MCP_READ_ONLY",
        "AIRFLOW_MCP_TOKEN_REFRESH_SECONDS",
    ):
        monkeypatch.delenv(var, raising=False)
    reset_registry_cache()
    yield
    reset_registry_cache()


def test_build_single_instance_basic_auth():
    settings = AirflowServerConfig(
        host="https://airflow.example.com",
        username="admin",
        password="secret",
    )
    reg = build_single_instance_registry(settings)
    assert set(reg.instances.keys()) == {"default"}
    assert reg.default_instance == "default"
    inst = reg.instances["default"]
    assert inst.host == "https://airflow.example.com"
    assert inst.auth.type == "basic"
    # api_version defaults to whichever matches the installed apache-airflow-client
    assert inst.resolved_api_version == default_api_version()
    assert inst.verify_ssl is True


def test_build_single_instance_rejects_invalid_instance_key():
    settings = AirflowServerConfig(
        host="https://airflow.example.com",
        username="admin",
        password="secret",
        default_instance="bad key!",
    )
    with pytest.raises(AirflowToolError) as exc:
        build_single_instance_registry(settings)
    assert exc.value.code == "CONFIG_ERROR"
    assert "bad key!" in str(exc.value)


@pytest.mark.parametrize("value", [0, -1])
def test_non_positive_token_refresh_seconds_rejected(value: int):
    with pytest.raises(ValidationError):
        AirflowServerConfig(token_refresh_seconds=value)


def test_build_single_instance_bearer_token_takes_precedence():
    settings = AirflowServerConfig(
        host="https://airflow.example.com",
        username="admin",
        password="secret",
        token="tok-123",
    )
    reg = build_single_instance_registry(settings)
    assert reg.instances["default"].auth.type == "bearer"


def test_build_single_instance_custom_key_via_default_instance():
    settings = AirflowServerConfig(
        host="https://airflow.example.com",
        username="admin",
        password="secret",
        default_instance="prod",
    )
    reg = build_single_instance_registry(settings)
    assert set(reg.instances.keys()) == {"prod"}
    assert reg.default_instance == "prod"


def test_build_single_instance_missing_credentials():
    settings = AirflowServerConfig(host="https://airflow.example.com")
    with pytest.raises(AirflowToolError) as exc:
        build_single_instance_registry(settings)
    assert exc.value.code == "CONFIG_ERROR"
    assert "AIRFLOW_MCP_USERNAME" in str(exc.value)
    assert "AIRFLOW_MCP_TOKEN" in str(exc.value)


def test_invalid_api_version_rejected():
    settings = AirflowServerConfig(
        host="https://airflow.example.com",
        username="admin",
        password="secret",
        api_version="3",
    )
    with pytest.raises(AirflowToolError) as exc:
        build_single_instance_registry(settings)
    assert exc.value.code == "CONFIG_ERROR"
    assert "api_version" in str(exc.value)


def test_discovery_tools_with_env_only_config(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AIRFLOW_MCP_HOST", "https://airflow.example.com")
    monkeypatch.setenv("AIRFLOW_MCP_USERNAME", "admin")
    monkeypatch.setenv("AIRFLOW_MCP_PASSWORD", "secret")

    out = airflow_tools.list_instances()
    payload = _payload(out)
    assert payload["instances"] == ["default"]
    assert payload["default_instance"] == "default"

    desc = _payload(airflow_tools.describe_instance("default"))
    assert desc["host"] == "https://airflow.example.com"
    assert desc["auth_type"] == "basic"


def test_instances_file_takes_precedence_over_env_host(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    yaml_text = (
        "from-file:\n"
        "  host: https://airflow.file.example.com/\n"
        "  auth:\n"
        "    type: basic\n"
        "    username: u\n"
        "    password: p\n"
    )
    p = tmp_path / "instances.yaml"
    p.write_text(yaml_text, encoding="utf-8")

    monkeypatch.setenv("AIRFLOW_MCP_INSTANCES_FILE", str(p))
    monkeypatch.setenv("AIRFLOW_MCP_HOST", "https://airflow.env.example.com")
    monkeypatch.setenv("AIRFLOW_MCP_USERNAME", "admin")
    monkeypatch.setenv("AIRFLOW_MCP_PASSWORD", "secret")

    payload = _payload(airflow_tools.list_instances())
    assert payload["instances"] == ["from-file"]


def test_no_configuration_gives_actionable_error(monkeypatch: pytest.MonkeyPatch):
    with pytest.raises(Exception) as exc:
        airflow_tools.list_instances()
    message = str(exc.value)
    assert "AIRFLOW_MCP_INSTANCES_FILE" in message
    assert "AIRFLOW_MCP_HOST" in message
