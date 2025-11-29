# AGENTS.md

Guidance for AI Agents writing tests for the Airflow MCP Server. The goals are fast, deterministic tests with comprehensive coverage of contracts (structured JSON outputs, statelessness, observability, and security checks).

## Test Suite Layout

- `test_imports.py`: Sanity import checks; ensures top-level modules import without side effects.
- `test_registry.py`: Instance registry parsing, env substitution, defaults, and error cases.
- `test_url_resolver.py`: URL → identifiers resolution (hostname match, routes, error paths).
- `test_url_builder.py`: UI URL construction from identifiers.
- `test_tools_readonly.py`: Read-only tools (dags, dag runs, logs, dataset events) – contract and routing.
- `test_tools_write.py`: Write tools (trigger, clear, pause/unpause) – annotations and payload shape.
- `test_observability.py`: Structured logging and `request_id` propagation; timeout handling on client config.

## Principles

- No real network calls. Patch the Airflow client import path (`airflow_mcp.client_factory._import_airflow_client`) and/or API classes.
- Isolate state. Reset the registry cache in fixtures; never rely on prior test ordering.
- Contract-first. Assert dict payloads on success (`request_id` present) and `ToolError` payloads (code/message/request_id) on failure.
- Stateliness enforced. Verify `instance` vs `ui_url` precedence and `INSTANCE_MISMATCH` errors.
- Security. Validate identifier pattern checks and SSRF guard via strict hostname matching.

## Core Fixtures

- Registry environment fixture (example):

```python
import pytest
from pathlib import Path
from airflow_mcp.registry import reset_registry_cache

@pytest.fixture
def registry_env(monkeypatch: pytest.MonkeyPatch) -> None:
    reset_registry_cache()
    examples = Path(__file__).resolve().parent.parent / "examples" / "instances.yaml"
    monkeypatch.setenv("AIRFLOW_MCP_INSTANCES_FILE", str(examples))
    # Provide only the variables referenced by the example YAML
    monkeypatch.setenv("AIRFLOW_INSTANCE_DATA_STG_USERNAME", "user")
    monkeypatch.setenv("AIRFLOW_INSTANCE_DATA_STG_PASSWORD", "pass")
    monkeypatch.setenv("AIRFLOW_INSTANCE_ML_STG_USERNAME", "user")
    monkeypatch.setenv("AIRFLOW_INSTANCE_ML_STG_PASSWORD", "pass")
    yield
    reset_registry_cache()
```

- Observability caplog fixture (example):

```python
import logging
import pytest

@pytest.fixture
def observability_caplog(caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
    caplog.clear()
    caplog.set_level(logging.DEBUG, logger="airflow_mcp.observability")
    caplog.set_level(logging.DEBUG, logger="airflow_mcp.client_factory")
    return caplog
```

## Mocking the Airflow Client

- Patch the import helper so tests do not import or talk to the real client:

```python
from types import SimpleNamespace
from airflow_mcp import client_factory as cf

class FakeConfiguration:
    def __init__(self, host: str) -> None:
        self.host = host
        self.username = None
        self.password = None
        self.verify_ssl = None
        self.timeout = None

class FakeApiClient:
    def __init__(self, configuration: FakeConfiguration) -> None:
        self.configuration = configuration

monkeypatch.setattr(
    cf, _import_airflow_client.__name__,  # or cf._import_airflow_client
    lambda: (FakeConfiguration, FakeApiClient, SimpleNamespace()),
)
```

## Write Tools

- Tests should not execute real destructive operations. Either:
  - Use `dry_run` when available, or
  - Patch the corresponding API class method to a stub that records inputs.
- Assert that server wrappers use destructive annotations and return a compact dict response with a UI link.

## What to Assert for Every Tool

- Successful tool calls return dict payloads; never tables or markdown. Errors should raise `ToolError` (parse `json.loads(str(exc.value))` to inspect the failure envelope).
- `request_id` presence in every response.
- For URL-based calls, correct UI URL construction (`url_utils.build_airflow_ui_url`).
- Observability fields exist in logs: `event`, `duration_ms`, optionally `response_bytes`, and contextual identifiers.

## Running Tests

- `uv run pytest -q` from the repository root.

## Adding New Tests

- Cover: happy path, validation failures, instance/URL precedence, and observability.
- Prefer small, focused tests; mock at module boundary instead of deep internals.

