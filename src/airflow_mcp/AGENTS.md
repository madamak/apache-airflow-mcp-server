# src/AGENTS.md

Module-level guidance for the Airflow MCP server source. Use this when adding or modifying code inside `src/airflow_mcp/`.

## Modules and Responsibilities

- `server.py`
  - Registers FastMCP tools and HTTP routes; keep minimal and declarative.
  - Applies `@handle_errors` and tool annotations; no business logic here.
- `tools.py`
  - Implements tool logic: validate → resolve instance/URL → call Airflow client → shape JSON.
  - Must return structured dict payloads; `observability.OperationLogger.success()` injects `request_id`.
- `registry.py`
  - Loads instance registry YAML with `${VAR}` substitution.
  - `get_registry()` caches the parsed registry; `reset_registry_cache()` exists for tests.
- `client_factory.py`
  - Provides cached `apache-airflow-client` instances per registry key; applies basic auth, SSL, and timeouts.
  - Accessors: `get_dags_api`, `get_dag_runs_api`, `get_task_instances_api`, `get_dataset_events_api`.
- `url_utils.py`
  - `parse_airflow_ui_url` resolves instance and identifiers from UI URLs with strict hostname matching.
  - `build_airflow_ui_url` constructs canonical links. `resolve_and_validate` enforces `ui_url` precedence and mismatch errors.
- `validation.py`
  - Safe identifier validation (patterns for instance, dag_id, dag_run_id, task_id, dataset_uri).
- `observability.py`
  - `OperationLogger` context manager emits structured logs and injects `request_id` into JSON outputs.
- `errors.py`
  - `AirflowToolError` for user-facing validation errors. `handle_errors` logs with context and raises MCP `ToolError` containing a compact JSON payload.
- `config.py`
  - Pydantic settings with `AIRFLOW_MCP_*` env prefix (timeouts, HTTP bind, log file, registry file, default instance).
- `formatting/`
  - Reserved. No table rendering; outputs remain JSON-only.

## Contracts and Conventions

- Tools return structured dicts on success (FastMCP handles serialization) and always include `request_id`. Failures raise `ToolError` with a compact JSON message: `{ "code": "...", "message": "...", "request_id": "...", "context"?: {...} }`.
- Read-only vs write:
  - Read-only tools: annotate with `readOnlyHint=true`, `destructiveHint=false`, `idempotentHint=true`.
  - Write tools: annotate with `destructiveHint=true`, `idempotentHint=false`.
- Target selection precedence:
  - `ui_url` must be a fully qualified http(s) Airflow UI URL; reject shorthand hostnames like `airflow-2`.
  - If both `instance` and `ui_url` are provided, hosts must match (`INSTANCE_MISMATCH` otherwise).
  - If only `ui_url` is provided, resolve instance and identifiers from the URL.
  - If only `instance` is provided, validate against the registry.
- Never expose credentials. When describing instances, expose only `auth_type`.

## Recipe: Adding a Read-only Tool

1) Validate inputs using `validation.py` and/or `url_utils.resolve_and_validate(ui_url, instance)`.
2) Create an `OperationLogger` with relevant context: tool name, instance, dag/task IDs when available.
3) Use `client_factory.AirflowClientFactory` to obtain the appropriate API (e.g., `get_dags_api`).
4) Call the Airflow API; transform the response into a compact JSON structure. Build UI links using `url_utils.build_airflow_ui_url`.
5) Return via `op.success(payload)` (ensures `request_id` and logs response size).
6) Register a thin wrapper in `server.py` with `@handle_errors` and read-only annotations.

## Recipe: Adding a Write Tool

Follow the read-only recipe, with additional safeguards:
- Use write annotations in `server.py` (`destructiveHint=true`).
- Enforce explicit parameters (avoid implicit destructive defaults). Include `dry_run` where supported.
- Return a clear, minimal JSON describing the change and a UI link to the affected resource.

## Observability

- Wrap each logical operation in `OperationLogger`: it emits `tool_start`, `tool_success`, `tool_error` with fields like `duration_ms`, `response_bytes`, `instance`, `dag_id`, `task_id`, and `error_type`.
- Do not log credentials or raw request bodies. Prefer high-level counts and identifiers.

## Error Handling

- Raise `AirflowToolError` with an explicit `code` (e.g., `INVALID_INPUT`, `NOT_FOUND`). Include small `context` dictionaries when it helps troubleshoot.
- `@handle_errors` logs via `OperationLogger` and raises `ToolError` with a JSON message containing `code`, `message`, `request_id`, and optional `context`. Unexpected exceptions are masked as `INTERNAL_ERROR` with a generic message.

## Security Guidelines

- Hostname match in `url_utils.parse_airflow_ui_url` is required to prevent SSRF.
- Validate all identifiers against `validation.py` patterns before use.
- Sanitize/limit user-controlled fields passed to client APIs. Do not echo credentials.

## Testing Hooks

- Call `registry.reset_registry_cache()` in fixtures to isolate registry state.
- Prefer patching `client_factory._import_airflow_client` to avoid importing real client classes during unit tests.
- Assert the presence of `request_id` in all tool responses (dicts) and verify structured log fields with `caplog`.
