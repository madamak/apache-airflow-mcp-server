"""E2E: read-only mode via the real stdio entrypoint.

The in-process server is built once at import time, so read-only mode is
exercised the way a user hits it: a fresh subprocess running the installed
wheel's real `airflow-mcp` console script over stdio with
AIRFLOW_MCP_READ_ONLY=true.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from _helpers import INSTANCE, SEED_DAGS, WRITE_TOOLS, call
from fastmcp import Client
from fastmcp.client.transports import StdioTransport

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]

READ_TOOLS = {"airflow_list_instances", "airflow_list_dags", "airflow_get_task_instance_logs"}


async def test_installed_read_only_stdio_server_hides_writes_and_reads_live_airflow():
    executable = os.environ.get("E2E_INSTALLED_AIRFLOW_MCP")
    assert executable, "scripts/e2e.sh must export the installed airflow-mcp path"
    assert Path(executable).is_file(), f"installed console script not found: {executable}"

    env = {
        **os.environ,
        "AIRFLOW_MCP_READ_ONLY": "true",
    }
    # The console script's shebang selects the isolated artifact environment.
    # Removing development-environment hints ensures imports cannot accidentally
    # resolve through the editable checkout used by the pytest process.
    env.pop("PYTHONPATH", None)
    env.pop("VIRTUAL_ENV", None)
    transport = StdioTransport(
        command=executable,
        args=["--transport", "stdio"],
        env=env,
    )
    async with Client(transport) as client:
        tools = {t.name for t in await client.list_tools()}
        assert READ_TOOLS <= tools
        assert not (WRITE_TOOLS & tools), (
            f"write tools leaked in read-only mode: {WRITE_TOOLS & tools}"
        )

        payload = await call(client, "airflow_list_dags", instance=INSTANCE)
        live_dag_ids = {dag["dag_id"] for dag in payload["dags"]}
        assert set(SEED_DAGS) <= live_dag_ids
