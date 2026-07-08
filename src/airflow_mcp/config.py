from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AirflowServerConfig(BaseSettings):
    """Configuration for the Airflow MCP server (Phase 1)."""

    log_level: str = Field(default="INFO", description="Log level (e.g., INFO, DEBUG)")
    log_file: str | None = Field(default=None, description="Optional path to a log file")

    http_host: str = Field(default="127.0.0.1", description="HTTP bind host")
    http_port: int = Field(default=8765, description="HTTP bind port")

    timeout_seconds: int = Field(default=30, description="Default timeout for API calls")

    token_refresh_seconds: int = Field(
        default=3600,
        description="Airflow 3 only: refresh the JWT obtained from basic credentials "
        "after this many seconds",
    )

    http_block_get_on_mcp: bool = Field(
        default=True,
        description="If true, block GET /mcp to avoid SSE read attempts on HTTP deployments",
    )

    instances_file: str | None = Field(
        default=None, description="Path to YAML file containing Airflow instance registry"
    )
    default_instance: str | None = Field(
        default=None, description="Default instance key for discovery and elicitations"
    )

    # Single-instance quick configuration (alternative to instances_file).
    # When instances_file is unset and host is provided, a one-instance registry
    # is built from these values so no YAML file is needed.
    host: str | None = Field(
        default=None,
        description="Airflow base URL for single-instance mode (e.g., https://airflow.example.com)",
    )
    username: str | None = Field(
        default=None, description="Basic auth username for single-instance mode"
    )
    password: str | None = Field(
        default=None, description="Basic auth password for single-instance mode"
    )
    token: str | None = Field(
        default=None, description="Bearer token for single-instance mode (used over basic auth)"
    )
    api_version: str | None = Field(
        default=None,
        description="Airflow REST API version for single-instance mode: 'v1' (Airflow 2) or "
        "'v2' (Airflow 3). Defaults to whichever matches the installed apache-airflow-client",
    )
    verify_ssl: bool = Field(
        default=True, description="Verify SSL certificates in single-instance mode"
    )

    read_only: bool = Field(
        default=False,
        description="If true, write tools (trigger, clear, pause/unpause) are not registered",
    )

    enable_extended_clear_params: bool = Field(
        default=False,
        description="Enable extended clear parameters (include_subdags, include_upstream, etc.) for Airflow ≥2.6. "
        "Set to false for Airflow 2.5.x compatibility which may reject these fields.",
    )

    model_config = SettingsConfigDict(env_prefix="AIRFLOW_MCP_", case_sensitive=False)

    def validate_config(self) -> None:
        """Basic validation. Deep registry validation occurs in registry loader."""
        if not isinstance(self.http_port, int) or self.http_port <= 0:
            raise ValueError("Invalid HTTP port configured")


config = AirflowServerConfig()
