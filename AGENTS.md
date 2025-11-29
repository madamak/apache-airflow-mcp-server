# AGENTS.md

Guidance for AI Agents working on the Airflow MCP Server. This file orients you at the package level (what belongs where, how to extend safely). For module-level guidance, see `src/airflow_mcp/AGENTS.md`. For tests, see `tests/AGENTS.md`.

## Project Layout

- `src/airflow_mcp/`
  - `server.py`: FastMCP entrypoint. Registers tools and HTTP routes. Thin wrappers only.
  - `tools.py`: Business logic for MCP tools (structured dict outputs; FastMCP serializes). No FastMCP or web concerns.
  - `registry.py`: Instance registry loader (YAML with `${VAR}` env substitution) and cached accessor.
  - `client_factory.py`: Builds cached `apache-airflow-client` clients per instance; applies auth, SSL, timeouts.
  - `url_utils.py`: URL resolver/builder (instance detection, dag/task/run extraction, UI URL construction).
  - `validation.py`: Input validation (safe identifier patterns; SSRF guard via host check in resolver).
  - `observability.py`: Structured logging with request_id; common operation logger.
- `errors.py`: Error types and `handle_errors` decorator (raises MCP `ToolError` with compact JSON payload).
  - `config.py`: Pydantic settings (`AIRFLOW_MCP_*`).
- `tests/`: Unit and integration tests (mocked client; no real network).
- `examples/instances.yaml`: Example instance registry used by tests.

## Development Workflow

- Install & sync deps with `uv sync`.
- Run locally:
  - `uv run airflow-mcp --transport stdio`
  - `uv run airflow-mcp --transport http --host 127.0.0.1 --port 8765`
- Test & lint before pushing:
  - `uv run pytest`
  - `uv run ruff check .`
- Optional format pass: `uv run ruff format .`
- Commit hygiene: run tests + lint locally prior to opening a PR.

## Core Design Principles

- Structured JSON tools: return dict payloads (FastMCP serializes). Failures raise `ToolError` with payload `{ "code": "...", "message": "...", "request_id": "...", "context"?: {...} }`.
- Stateless: each call resolves target instance (via `instance` or `ui_url`). No cross-call state.
- Thin server: wrappers forward to `tools.py` and apply `@handle_errors` plus annotations.
- Multi-instance: registry-backed; unknown instances/hosts are rejected early.
- Security first: validate identifiers, never log secrets, guard SSRF by exact hostname match.
- Keep the tool surface compact: `airflow_list_task_instances` exposes the filters needed for failed-task discovery (`state`, `task_ids`), so new helpers should build on it rather than duplicating the workflow.
- Client capability detection: the Airflow SDK evolves over time, so `list_task_instances` inspects the bound `TaskInstanceApi.get_task_instances` signature to decide which filter kwargs (`state`, `task_ids`) are supported. When bumping SDK versions, update the detection logic and the read-only tests together so we never pass unsupported kwargs.

## Tooling Contracts and Annotations

- Read-only tools: annotated with `{"readOnlyHint": true, "destructiveHint": false, "idempotentHint": true}`.
- Write tools: annotated with `{"destructiveHint": true, "idempotentHint": false}` so MCP clients prompt users.
- URL precedence: when both `instance` and `ui_url` are provided, hosts must match or the call fails (`INSTANCE_MISMATCH`).
- Always include a `request_id` in JSON responses (injected by `observability.OperationLogger`).

## Configuration (env)

- Required for runtime: `AIRFLOW_MCP_INSTANCES_FILE` → path to instance registry YAML.
- Optional: `AIRFLOW_MCP_DEFAULT_INSTANCE`, `AIRFLOW_MCP_HTTP_HOST`, `AIRFLOW_MCP_HTTP_PORT`, `AIRFLOW_MCP_TIMEOUT_SECONDS`, `AIRFLOW_MCP_LOG_FILE`, `AIRFLOW_MCP_HTTP_BLOCK_GET_ON_MCP`.
- Per-instance credentials are referenced within the YAML and resolved from environment (see `examples/instances.yaml`).

## Observability and Logging

- Use `observability.OperationLogger` around tool executions to emit:
  - `tool_start`, `tool_success/tool_error` with `duration_ms`, `response_bytes`, and context (instance, dag_id, etc.).
  - `request_id` is included in both logs and JSON payloads for correlation.

## Error Handling

- Business logic raises `AirflowToolError` for user-facing validation errors.
- Wrappers apply `@handle_errors` to log via `OperationLogger` and raise `ToolError` with a compact JSON payload; unexpected exceptions are masked as `INTERNAL_ERROR`.
- Never include credentials in logs or error messages. Registry exposure is redacted to `auth_type`.

## Adding or Changing Tools (high level)

1. Implement logic in `tools.py` (validate → resolve instance/URL → call client → shape JSON → include UI URLs when relevant).
2. Register the tool in `server.py` with appropriate annotations and `@handle_errors`.
3. Add tests in `tests/` (read-only and error paths; write tools behind mocks and/or dry-run).
4. Update `README.md` and docs if the tool contract changes.

## References

- Release plan: `docs/RELEASE_PLAN.md`.
