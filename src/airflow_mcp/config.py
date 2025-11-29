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
