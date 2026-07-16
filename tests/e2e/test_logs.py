"""E2E: log fetching, filtering, tailing, and truncation against real task logs."""

from __future__ import annotations

import pytest
from _helpers import INSTANCE, call, seed_run_id

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


async def _logs(client, dag_id: str, task_id: str, try_number: int, **kwargs):
    return await call(
        client,
        "airflow_get_task_instance_logs",
        instance=INSTANCE,
        dag_id=dag_id,
        dag_run_id=seed_run_id(dag_id),
        task_id=task_id,
        try_number=try_number,
        **kwargs,
    )


async def test_error_filter_finds_failure_marker(client):
    payload = await _logs(
        client, "e2e_failure", "transform", 2, filter_level="error", context_lines=3
    )
    assert "E2E_FAILURE_MARKER" in payload["log"]
    assert payload["match_count"] >= 1
    assert payload["meta"]["try_number"] == 2
    assert payload["meta"]["filters"]["filter_level"] == "error"


async def test_first_attempt_log_contains_failure_marker(client):
    first = await _logs(client, "e2e_failure", "transform", 1)
    assert "E2E_FAILURE_MARKER" in first["log"]
    assert first["original_lines"] > 0


async def test_tail_lines(client):
    payload = await _logs(client, "e2e_noisy_logs", "spew", 1, tail_lines=100)
    assert "E2E_NOISY_LAST_LINE" in payload["log"]
    assert payload["returned_lines"] >= 90
    assert payload["returned_lines"] <= 110  # tail + a little header slack
    assert payload["original_lines"] > 3000


async def test_max_bytes_truncation(client):
    payload = await _logs(client, "e2e_noisy_logs", "spew", 1, max_bytes=5000)
    assert payload["truncated"] is True
    assert payload["bytes_returned"] <= 5000


async def test_error_filter_on_noisy_log(client):
    payload = await _logs(client, "e2e_noisy_logs", "spew", 1, filter_level="error")
    # fixture prints an ERROR marker every 500 of 3000 lines
    assert payload["match_count"] >= 6
    assert "E2E_NOISY_ERROR" in payload["log"]
    assert "e2e noisy filler" not in payload["log"]
