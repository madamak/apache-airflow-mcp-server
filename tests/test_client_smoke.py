"""Smoke tests against the *installed* apache-airflow-client.

The rest of the suite mocks the client, so these assertions are the one place
where CI's client-major matrix exercises the real package — catching codegen
renames and detection drift that mocks cannot.
"""

from __future__ import annotations

import importlib.metadata

from airflow_mcp.registry import default_api_version, installed_client_major


def _package_major() -> int:
    return int(importlib.metadata.version("apache-airflow-client").split(".")[0])


def test_installed_client_major_matches_package_version():
    assert installed_client_major() == _package_major()


def test_default_api_version_matches_installed_major():
    expected = "v2" if _package_major() >= 3 else "v1"
    assert default_api_version() == expected


def test_real_client_exposes_the_surface_we_call():
    import airflow_client.client as client_mod

    if _package_major() >= 3:
        # 3.x codegen: API classes at the top level (note DagRunApi casing)
        for name in ("DAGApi", "DagRunApi", "TaskInstanceApi", "TaskApi", "AssetApi"):
            assert hasattr(client_mod, name), name
        assert hasattr(client_mod.TaskInstanceApi, "get_log_without_preload_content")
        assert hasattr(client_mod.DagRunApi, "trigger_dag_run")
        assert hasattr(client_mod.DagRunApi, "clear_dag_run")
        assert hasattr(client_mod.TaskInstanceApi, "post_clear_task_instances")
        assert hasattr(client_mod.DAGApi, "patch_dag")
        assert hasattr(client_mod.AssetApi, "get_asset_events")
    else:
        # 2.x codegen: API classes under the apis subpackage
        from airflow_client.client import apis

        for name in ("DAGApi", "DAGRunApi", "TaskInstanceApi", "DatasetApi"):
            assert hasattr(apis, name), name
