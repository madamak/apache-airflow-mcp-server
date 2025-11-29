from pathlib import Path

import pytest
from test_tools_readonly import _payload

from airflow_mcp import tools as airflow_tools
from airflow_mcp.errors import AirflowToolError
from airflow_mcp.registry import load_registry_from_yaml, reset_registry_cache


@pytest.fixture()
def tmp_registry_file(tmp_path: Path) -> Path:
    content = (
        "data-stg:\n"
        "  host: https://airflow.data-stg.example.com/\n"
        "  api_version: v1\n"
        "  verify_ssl: true\n"
        "  auth:\n"
        "    type: basic\n"
        "    username: user\n"
        "    password: pass\n"
        "ml-stg:\n"
        "  host: https://airflow.ml-stg.example.com/\n"
        "  api_version: v1\n"
        "  verify_ssl: true\n"
        "  auth:\n"
        "    type: basic\n"
        "    username: u\n"
        "    password: p\n"
    )
    p = tmp_path / "instances.yaml"
    p.write_text(content, encoding="utf-8")
    return p


@pytest.fixture(autouse=True)
def _ensure_fresh_registry(monkeypatch: pytest.MonkeyPatch):
    reset_registry_cache()
    yield
    reset_registry_cache()


def test_load_registry(tmp_registry_file: Path):
    reg = load_registry_from_yaml(str(tmp_registry_file), default_instance="data-stg")
    assert set(reg.instances.keys()) == {"data-stg", "ml-stg"}
    assert reg.default_instance == "data-stg"


def test_load_registry_with_bearer_auth_experimental(tmp_path: Path):
    yaml_text = (
        "ml-prod:\n"
        "  host: https://airflow.ml-prod.example.com/\n"
        "  api_version: v1\n"
        "  verify_ssl: true\n"
        "  auth:\n"
        "    type: bearer\n"
        "    token: token-123\n"
    )
    p = tmp_path / "instances.yaml"
    p.write_text(yaml_text, encoding="utf-8")

    reg = load_registry_from_yaml(str(p))

    inst = reg.instances["ml-prod"]
    assert inst.auth.type == "bearer"  # experimental
    assert inst.auth.token == "token-123"
    assert reg.describe_instance("ml-prod").auth_type == "bearer"


def test_discovery_tools(monkeypatch: pytest.MonkeyPatch, tmp_registry_file: Path):
    monkeypatch.setenv("AIRFLOW_MCP_INSTANCES_FILE", str(tmp_registry_file))
    monkeypatch.setenv("AIRFLOW_MCP_DEFAULT_INSTANCE", "data-stg")

    out = airflow_tools.list_instances()
    payload = _payload(out)
    assert "instances" in payload and len(payload["instances"]) == 2
    assert payload["default_instance"] == "data-stg"

    out2 = airflow_tools.describe_instance("data-stg")
    desc = _payload(out2)
    assert desc["instance"] == "data-stg"
    assert desc["api_version"] == "v1"

    with pytest.raises(AirflowToolError) as exc:
        airflow_tools.describe_instance("does-not-exist")
    assert "Unknown instance" in str(exc.value)
    assert exc.value.code == "NOT_FOUND"


def test_missing_instances_file(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("AIRFLOW_MCP_INSTANCES_FILE", raising=False)
    monkeypatch.delenv("AIRFLOW_MCP_DEFAULT_INSTANCE", raising=False)

    # Calling list_instances should error (tools layer raises; server wrapper envelopes)
    with pytest.raises(Exception) as exc:
        airflow_tools.list_instances()
    assert "AIRFLOW_MCP_INSTANCES_FILE" in str(exc.value)


def test_env_substitution_success(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    yaml_text = (
        "data-stg:\n"
        "  host: ${HOST_URL}\n"
        "  api_version: v1\n"
        "  verify_ssl: true\n"
        "  auth:\n"
        "    type: basic\n"
        "    username: ${USER_VAR}\n"
        "    password: ${PASS_VAR}\n"
    )
    p = tmp_path / "instances.yaml"
    p.write_text(yaml_text, encoding="utf-8")

    monkeypatch.setenv("AIRFLOW_MCP_INSTANCES_FILE", str(p))
    monkeypatch.setenv("HOST_URL", "https://example")
    monkeypatch.setenv("USER_VAR", "u")
    monkeypatch.setenv("PASS_VAR", "p")

    out = airflow_tools.describe_instance("data-stg")
    desc = _payload(out)
    assert desc["host"] == "https://example"


def test_env_substitution_missing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    yaml_text = (
        "data-stg:\n"
        "  host: ${MISSING_HOST}\n"
        "  api_version: v1\n"
        "  verify_ssl: true\n"
        "  auth:\n"
        "    type: basic\n"
        "    username: ${USER_VAR}\n"
        "    password: ${PASS_VAR}\n"
    )
    p = tmp_path / "instances.yaml"
    p.write_text(yaml_text, encoding="utf-8")

    monkeypatch.setenv("AIRFLOW_MCP_INSTANCES_FILE", str(p))
    monkeypatch.setenv("USER_VAR", "u")
    monkeypatch.setenv("PASS_VAR", "p")

    # tools layer raises; server wrapper will envelope in production
    with pytest.raises(Exception) as exc:
        airflow_tools.list_instances()
    assert "Missing required environment variable" in str(exc.value)


def test_multi_instance_yaml_bearer_token_experimental(tmp_path: Path):
    yaml_text = (
        "data-stg:\n"
        "  host: https://airflow.data-stg.example.com/\n"
        "  api_version: v1\n"
        "  verify_ssl: true\n"
        "  auth:\n"
        "    type: basic\n"
        "    username: user\n"
        "    password: pass\n"
        "ml-prod:\n"
        "  host: https://airflow.ml-prod.example.com/\n"
        "  api_version: v1\n"
        "  verify_ssl: true\n"
        "  auth:\n"
        "    type: bearer\n"
        "    token: token-abc\n"
    )
    p = tmp_path / "instances.yaml"
    p.write_text(yaml_text, encoding="utf-8")

    reg = load_registry_from_yaml(str(p), default_instance="data-stg")

    assert set(reg.instances.keys()) == {"data-stg", "ml-prod"}
    assert reg.instances["ml-prod"].auth.type == "bearer"  # experimental
