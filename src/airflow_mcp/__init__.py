"""Apache Airflow MCP Server - Enable agents to interact with Airflow APIs."""

from ._version import get_version

__version__ = get_version()


def main() -> None:
    """Main entry point for the CLI."""

    from .server import run_server

    run_server()


__all__ = ["__version__", "main"]
