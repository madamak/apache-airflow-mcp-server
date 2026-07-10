"""Tests for AIRFLOW_MCP_READ_ONLY mode.

Server tool registration happens at import time based on config, so these tests
run a fresh interpreter per scenario to get clean, isolated registration.
"""

import json
import os
import subprocess
import sys

LIST_TOOLS_SNIPPET = """
import asyncio
import json
from airflow_mcp.server import mcp

tools = asyncio.run(mcp.get_tools())
print(json.dumps(sorted(tools.keys())))
"""

WRITE_TOOLS = {
    "airflow_trigger_dag",
    "airflow_clear_task_instances",
    "airflow_clear_dag_run",
    "airflow_pause_dag",
    "airflow_unpause_dag",
}


def _registered_tools(extra_env: dict[str, str]) -> set[str]:
    env = {**os.environ, **extra_env}
    result = subprocess.run(
        [sys.executable, "-c", LIST_TOOLS_SNIPPET],
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )
    return set(json.loads(result.stdout.strip().splitlines()[-1]))


def test_write_tools_registered_by_default():
    tools = _registered_tools({"AIRFLOW_MCP_READ_ONLY": "false"})
    assert WRITE_TOOLS.issubset(tools)
    assert "airflow_list_dags" in tools


def test_read_only_mode_hides_write_tools():
    tools = _registered_tools({"AIRFLOW_MCP_READ_ONLY": "true"})
    assert not (WRITE_TOOLS & tools)
    # Read-only tools remain available
    assert "airflow_list_dags" in tools
    assert "airflow_get_task_instance_logs" in tools
    assert "airflow_list_instances" in tools
