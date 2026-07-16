"""E2E: Airflow 3 bearer authentication and live JWT refresh."""

from __future__ import annotations

import asyncio
import os

import pytest
from _helpers import E2E_API_VERSION, INSTANCE, call

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.asyncio,
    pytest.mark.skipif(E2E_API_VERSION != "v2", reason="Airflow 3 authentication only"),
]


async def test_direct_bearer_registry_entry_reads_live_airflow(client):
    bearer_instance = os.environ.get("E2E_BEARER_INSTANCE")
    assert bearer_instance, "Airflow 3 harness must register a direct bearer instance"

    payload = await call(client, "airflow_list_dags", instance=bearer_instance)

    assert payload["count"] >= 1
    assert payload["request_id"]

    description = await call(client, "airflow_describe_instance", instance=bearer_instance)
    assert description["auth_type"] == "bearer"
    assert "token" not in description
    assert "password" not in description


async def test_basic_auth_fetches_and_refreshes_jwt_against_live_endpoint(client, monkeypatch):
    """Wrap the real token fetch so both observed calls still hit /auth/token."""
    from airflow_mcp import client_factory as client_factory_module

    factory = client_factory_module.get_client_factory()
    factory._cache.pop(INSTANCE, None)
    real_fetch = client_factory_module._fetch_jwt_token
    fetched_tokens: list[str] = []

    def observed_fetch(*args, **kwargs) -> str:
        token = real_fetch(*args, **kwargs)
        fetched_tokens.append(token)
        return token

    monkeypatch.setattr(client_factory_module, "_fetch_jwt_token", observed_fetch)

    first = await call(client, "airflow_list_dags", instance=INSTANCE)
    assert first["count"] >= 1
    assert len(fetched_tokens) == 1

    refresh_seconds = client_factory_module._server_config.token_refresh_seconds
    assert refresh_seconds == 1, "Airflow 3 harness must configure a short refresh interval"
    await asyncio.sleep(refresh_seconds + 0.25)

    second = await call(client, "airflow_list_dags", instance=INSTANCE)
    assert second["count"] >= 1
    assert len(fetched_tokens) == 2
    assert all(token.count(".") == 2 for token in fetched_tokens)
