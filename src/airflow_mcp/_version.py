"""Version helpers for the Airflow MCP server."""

from importlib import metadata


def get_version() -> str:
    """Return the installed package version, falling back to a dev placeholder."""

    try:
        return metadata.version("apache-airflow-mcp-server")
    except metadata.PackageNotFoundError:
        return "0.0.0+dev"


__all__ = ["get_version"]

