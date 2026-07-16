"""E2E suite plumbing.

These tests drive the real MCP server (in-memory FastMCP client -> registered
tools -> live Airflow REST). They only run when scripts/e2e.sh has exported
E2E_AIRFLOW_BASE_URL + AIRFLOW_MCP_* pointing at a seeded instance; otherwise
every test is skipped so the default `pytest` run stays green.
"""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse

import pytest
import pytest_asyncio
import yaml
from _helpers import E2E_BASE_URL

_E2E_DIR = Path(__file__).parent


def pytest_collection_modifyitems(config, items):
    """Skip the E2E tests (only them — this hook sees ALL collected items)
    unless the harness exported a live-instance URL."""
    if E2E_BASE_URL:
        if os.getenv("E2E_ALLOW_WRITES") != "1" or not os.getenv("E2E_HARNESS_NONCE"):
            raise pytest.UsageError(
                "live E2E requires scripts/e2e.sh ownership markers; refusing destructive tests"
            )
        parsed = urlparse(E2E_BASE_URL)
        if parsed.hostname not in {"localhost", "127.0.0.1"}:
            raise pytest.UsageError("live E2E Airflow must be loopback-bound")
        registry_path = os.getenv("AIRFLOW_MCP_INSTANCES_FILE")
        if not registry_path:
            raise pytest.UsageError("live E2E requires the harness-generated instance registry")
        registry = yaml.safe_load(Path(registry_path).read_text(encoding="utf-8"))
        if not isinstance(registry, dict):
            raise pytest.UsageError("live E2E instance registry is not a mapping")
        registered_host = registry.get("e2e-local", {}).get("host")
        if not isinstance(registered_host, str) or (
            registered_host.rstrip("/") != E2E_BASE_URL.rstrip("/")
        ):
            raise pytest.UsageError("e2e-local does not match the harness-owned Airflow URL")
        return
    skip = pytest.mark.skip(reason="E2E env not up — run via scripts/e2e.sh")
    for item in items:
        if _E2E_DIR in Path(item.fspath).parents:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def base_url() -> str:
    return E2E_BASE_URL.rstrip("/")


@pytest_asyncio.fixture
async def client():
    from fastmcp import Client

    import airflow_mcp
    from airflow_mcp.server import mcp

    executable = os.getenv("E2E_INSTALLED_AIRFLOW_MCP")
    assert executable, "harness must expose its installed console script"
    artifact_venv = Path(executable).resolve().parent.parent
    module_path = Path(airflow_mcp.__file__).resolve()
    assert module_path.is_relative_to(artifact_venv), (
        f"E2E imported {module_path}, not the wheel installed under {artifact_venv}"
    )

    async with Client(mcp) as c:
        yield c
