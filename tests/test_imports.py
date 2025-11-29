import importlib


def test_import_airflow_mcp_main_exists():
    pkg = importlib.import_module("airflow_mcp")
    assert hasattr(pkg, "main")
