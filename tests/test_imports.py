import importlib
import importlib.metadata


def test_import_airflow_mcp_main_exists():
    pkg = importlib.import_module("airflow_mcp")
    assert hasattr(pkg, "main")


def test_console_entry_points_include_cli_and_registry_aliases():
    entry_points = {
        entry_point.name: entry_point.value
        for entry_point in importlib.metadata.distribution(
            "apache-airflow-mcp-server"
        ).entry_points
        if entry_point.group == "console_scripts"
    }

    assert entry_points["airflow-mcp"] == "airflow_mcp:main"
    assert entry_points["apache-airflow-mcp-server"] == "airflow_mcp:main"
