# Airflow MCP Server

[![MCP](https://img.shields.io/badge/MCP-Server-blueviolet)](https://modelcontextprotocol.io)
[![CI](https://github.com/madamak/apache-airflow-mcp-server/actions/workflows/ci.yml/badge.svg)](https://github.com/madamak/apache-airflow-mcp-server/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

Human entrypoint for running and using the Apache Airflow MCP server. This server exposes safe, focused tools to inspect Airflow DAGs, runs, and logs (with optional write operations gated by client approval). Responses are structured JSON objects (dicts) and include a `request_id` for traceability.

## Quickstart

### 1) Configure instances (required)

Set `AIRFLOW_MCP_INSTANCES_FILE` to a YAML file listing available Airflow instances. Values may reference environment variables using `${VAR}` syntax. Missing variables cause startup errors.

Example (`examples/instances.yaml`):

```yaml
# Data team staging instance
data-stg:
  host: https://airflow.data-stg.example.com/
  api_version: v1
  verify_ssl: true
  auth:
    type: basic
    username: ${AIRFLOW_INSTANCE_DATA_STG_USERNAME}
    password: ${AIRFLOW_INSTANCE_DATA_STG_PASSWORD}

# ML team staging instance
ml-stg:
  host: https://airflow.ml-stg.example.com/
  api_version: v1
  verify_ssl: true
  auth:
    type: basic
    username: ${AIRFLOW_INSTANCE_ML_STG_USERNAME}
    password: ${AIRFLOW_INSTANCE_ML_STG_PASSWORD}

# Bearer token (experimental)
# ml-prod:
#   host: https://airflow.ml-prod.example.com/
#   api_version: v1
#   verify_ssl: true
#   auth:
#     type: bearer
#     token: ${AIRFLOW_INSTANCE_ML_PROD_TOKEN}
```

Bearer token auth is **experimental**; basic auth remains the primary, well-tested path.

**Kubernetes deployment tip:** Provide `instances.yaml` via a Secret and mount it at `/config/instances.yaml` (set `AIRFLOW_MCP_INSTANCES_FILE=/config/instances.yaml`).

Environment variables:

- `AIRFLOW_MCP_INSTANCES_FILE` (required): path to registry YAML
- `AIRFLOW_MCP_DEFAULT_INSTANCE` (optional): default instance key
- `AIRFLOW_MCP_HTTP_HOST` (default: 127.0.0.1)
- `AIRFLOW_MCP_HTTP_PORT` (default: 8765)
- `AIRFLOW_MCP_TIMEOUT_SECONDS` (default: 30)
- `AIRFLOW_MCP_LOG_FILE` (optional)
- `AIRFLOW_MCP_HTTP_BLOCK_GET_ON_MCP` (default: true)

### 2) Run the server

- HTTP (recommended for tooling):

```bash
uv run airflow-mcp --transport http --host 127.0.0.1 --port 8765
```

- STDIO (CLI/terminal workflows):

```bash
uv run airflow-mcp --transport stdio
```

Health check (HTTP): `GET /health` → `200 OK`.

Tip: A `fastmcp.json` is included for discovery/config by FastMCP tooling:

```json
{
  "$schema": "https://gofastmcp.com/schemas/fastmcp_config/v1.json",
  "entrypoint": { "file": "src/airflow_mcp/server.py", "object": "mcp" },
  "deployment": { "transport": "http", "host": "127.0.0.1", "port": 8765 }
}
```

### 3) Typical incident workflow

Start from an Airflow UI URL (often in a Datadog alert):
1. `airflow_resolve_url(url)` → resolve `instance`, `dag_id`, `dag_run_id`, `task_id`.
2. `airflow_list_dag_runs(instance|ui_url, dag_id)` → confirm recent state.
3. `airflow_get_task_instance(instance|ui_url, dag_id, dag_run_id, task_id, include_rendered?, max_rendered_bytes?)` → inspect task metadata, attempts, and optional rendered fields.
4. `airflow_get_task_instance_logs(instance|ui_url, dag_id, dag_run_id, task_id, try_number, filter_level?, context_lines?, tail_lines?, max_bytes?)` → inspect failure with optional filtering and truncation.

All tools accept either `instance` or `ui_url`. If both are given and disagree, the call fails with `INSTANCE_MISMATCH`. `ui_url` must be a fully qualified http(s) Airflow URL; use `airflow_list_instances()` to discover valid hosts when you only have an instance key.

## Tool Reference (Structured JSON)

Discovery and URL utilities:
- `airflow_list_instances()` → list configured instance keys and default
- `airflow_describe_instance(instance)` → host, api_version, verify_ssl, `auth_type` (redacted)
- `airflow_resolve_url(url)` → resolve instance and identifiers from an Airflow UI URL

Read-only tools:
- `airflow_list_dags(instance|ui_url, limit?, offset?, state?/filters)` → compact DAGs with UI links
- `airflow_get_dag(instance|ui_url, dag_id)` → DAG details + UI link
- `airflow_list_dag_runs(instance|ui_url, dag_id, state?, limit?, offset?, order_by?, descending?)` → runs + per-run UI links. Defaults to `execution_date` descending ("latest first"). Accepts explicit ordering by `start_date`, `end_date`, or `execution_date`
- `airflow_get_dag_run(instance|ui_url, dag_id, dag_run_id)` → run details + UI link
- `airflow_list_task_instances(instance|ui_url, dag_id, dag_run_id, limit?, offset?, state?, task_ids?)` → task attempts for a run, with per-attempt log URLs, optional server-side filtering by state or task id, and a `filters` echo describing applied filters (`count` reflects filtered results; `total_entries` mirrors the API response when available)
- `airflow_get_task_instance(instance|ui_url, dag_id, dag_run_id, task_id, include_rendered?, max_rendered_bytes?)` → concise task metadata (state, timings, attempts, config) with optional rendered template fields and direct UI links; sensors increment `try_number` on every reschedule, so treat it as an attempt index (derived `retries_*` fields are heuristic)
- `airflow_get_task_instance_logs(instance|ui_url, dag_id, dag_run_id, task_id, try_number, filter_level?, context_lines?, tail_lines?, max_bytes?)` → log text with optional filtering; response includes `truncated`, `auto_tailed`, and stats
- `airflow_dataset_events(instance|ui_url, dataset_uri, limit?)` → dataset events (optional capability)

Write tools (require client approval; destructive):
- `airflow_trigger_dag(instance|ui_url, dag_id, conf?, logical_date?, dag_run_id?, note?)`
- `airflow_clear_task_instances(instance|ui_url, dag_id, task_ids?, start_date?, end_date?, include_*?, dry_run?)`
- `airflow_clear_dag_run(instance|ui_url, dag_id, dag_run_id, include_*?, dry_run?, reset_dag_runs?)`
- `airflow_pause_dag(instance|ui_url, dag_id)` / `airflow_unpause_dag(instance|ui_url, dag_id)`

Contract:
- Success: dict payloads include a `request_id` (traceable in logs). FastMCP serializes them automatically.
- Failure: tools raise an MCP `ToolError` whose payload remains a compact JSON string
  `{ "code": "INVALID_INPUT", "message": "...", "request_id": "...", "context"?: {...} }`.

### Log Filtering (`airflow_get_task_instance_logs`)

Efficient incident triage with server-side filtering and normalized payloads:

**Typical incident workflow:**
```python
# Example: Find errors in recent execution with context
airflow_get_task_instance_logs(
    dag_id="etl_pipeline",
    dag_run_id="scheduled__2025-10-30",
    task_id="transform_data",
    try_number=2,
    tail_lines=500,       # Last 500 lines only
    filter_level="error", # Show ERROR, CRITICAL, FATAL, Exception, Traceback
    context_lines=5       # Include 5 lines before/after each error
)
```

**Two-call pattern (required for `try_number`):**
1. `ti = airflow_get_task_instance(...)` → read `ti["attempts"]["try_number"]`.
2. `airflow_get_task_instance_logs(..., try_number=ti["attempts"]["try_number"])`.

Sensors and reschedules can increment `try_number` without consuming retries; keeping this explicit prevents the server from making incorrect assumptions about the “latest attempt.”

Sensors treat each reschedule as another attempt, so `try_number` is best interpreted as an **attempt index** rather than “number of retries consumed.” The derived `retries_consumed` / `retries_remaining` fields are heuristics based on configured retries and may not match Airflow’s notion for long-running sensors—always inspect the task metadata if you need authoritative counts.

**Parameters:**
- `filter_level`: `"error"` (strict) | `"warning"` (includes errors) | `"info"` (all levels)
- `context_lines`: Symmetric context (N before + N after each match), clamped to [0, 1000]
- `tail_lines`: Extract last N lines before filtering, clamped to [0, 100K]
- `max_bytes`: Hard cap (default: 100KB ≈ 25K tokens)
- Numeric inputs provided as floats/strings are coerced and clamped server-side so clients can pass user-entered values safely.

**Response shape:**
- `log`: Single normalized string. When the Airflow API returns host-segmented logs, headers of the form `--- [worker-1] ---` (or `--- [unknown-host] ---`) are inserted with blank lines between segments so LLMs can reason about execution locality.
- `truncated`: `true` if output exceeded `max_bytes`
- `auto_tailed`: `true` if log >100MB triggered automatic tail to last 10K lines
- `match_count`: Number of lines matching `filter_level` (before context expansion)
- `meta.filters`: Echo of effective filters applied (shows clamped values)
- `bytes_returned`, `original_lines`, `returned_lines`: stats that align with the normalized text seen by the client
### Failed Task Discovery (`airflow_list_task_instances`)

`airflow_list_task_instances` now exposes server-side filters that make the dedicated
`airflow_get_failed_task_instance` helper unnecessary. Use the same primitive for every
failed-task workflow:

```python
# All failed tasks for a known run (count reflects filtered results)
failed = airflow_list_task_instances(
    dag_id="etl_pipeline",
    dag_run_id="scheduled__2025-10-30",
    state=["failed"]
)

# Failed tasks for a subset of task IDs (case: only sensors + downloaders)
subset = airflow_list_task_instances(
    dag_id="etl_pipeline",
    dag_run_id="backfill__2025-10-30",
    state=["failed"],
    task_ids=["check_source", "download_payload"]
)

# Recipe: latest failed run → failed tasks
latest_failed = airflow_list_dag_runs(
    dag_id="etl_pipeline",
    state=["failed"],
    limit=1
)["dag_runs"][0]
failed_tasks = airflow_list_task_instances(
    dag_id="etl_pipeline",
    dag_run_id=latest_failed["dag_run_id"],
    state=["failed"]
)
```

**Why the change?**
- A single tool stays composable: filter by `state`, `task_ids`, or both.
- The response includes `filters` (echo) and `total_entries` when the API shares it, so callers can detect whether additional pagination is needed while still getting a filtered `count`.
- Agents that previously relied on `airflow_get_failed_task_instance` should migrate to `airflow_list_task_instances(state=["failed"])` (optionally preceded by `airflow_list_dag_runs` to resolve the relevant run).

### Task Instance Metadata (`airflow_get_task_instance`)

Pair metadata with logs for faster triage:

```python
# Example: Inspect failed task metadata and rendered fields
task_meta = airflow_get_task_instance(
    dag_id="etl_pipeline",
    dag_run_id="scheduled__2025-10-30",
    task_id="transform_data",
    include_rendered=True,
    max_rendered_bytes=100_000,
)

# Use try_number + ui_url.log to pull logs next
logs = airflow_get_task_instance_logs(
    dag_id="etl_pipeline",
    dag_run_id="scheduled__2025-10-30",
    task_id=task_meta["task_instance"]["task_id"],
    try_number=task_meta["attempts"]["try_number"],
    filter_level="error",
)
```

**Highlights:**
- `task_instance`: State, host, operator, timings (with computed `duration_ms`)
- `task_config`: Owner, retries, retry_delay (when available)
- `attempts`: Current try, retries consumed/remaining
- `rendered_fields` (optional): Rendered template values with byte cap (`max_rendered_bytes`, default 100KB; truncated payload returns `{ "_truncated": "Increase max_rendered_bytes" }`)
- `ui_url`: Direct `grid` and `log` links for the task attempt

## Client Notes

- MCP clients (e.g., IDEs) should call tools by name with JSON args and handle JSON responses.
- For URL-based calls, the server validates the hostname against the configured registry (SSRF guard).
- Write tools are annotated so MCP clients will prompt for confirmation before execution.

## Add to MCP clients

### Cursor – stdio (spawn on demand)

Add an entry to `~/.cursor/mcp.json`. Cursor will spawn the server process and communicate over stdio.

```json
{
  "mcpServers": {
    "airflow-stdio": {
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/apache-airflow-mcp-server",
        "run",
        "airflow-mcp",
        "--transport",
        "stdio"
      ],
      "env": {
        "AIRFLOW_MCP_INSTANCES_FILE": "/path/to/instances.yaml",
        "AIRFLOW_MCP_DEFAULT_INSTANCE": "production"
      }
    }
  }
}
```

Notes:
- Replace `/path/to/apache-airflow-mcp-server` and `/path/to/instances.yaml` with your paths.
- No long-running server is required; Cursor starts the process per session.

### Cursor – local HTTP

Run the server yourself, then point Cursor at the HTTP endpoint.

1) Start the server:

```bash
uv run airflow-mcp --transport http --host 127.0.0.1 --port 8765
```

2) In `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "airflow-local-http": {
      "url": "http://127.0.0.1:8765/mcp"
    }
  }
}
```

### Cursor – remote HTTP

Point Cursor at a deployed server. Optional headers can be added if fronted by an auth proxy.

```json
{
  "mcpServers": {
    "airflow-remote-http": {
      "url": "https://airflow-mcp.internal.example.com/mcp",
      "headers": {
        "X-API-Key": "your-token-if-proxied"
      }
    }
  }
}
```

### Other clients

- Many MCP clients accept either a stdio command or an HTTP URL; the examples above generalize.
- A `fastmcp.json` is included for FastMCP-aware tooling that can auto-discover entrypoints.

## Development

```bash
# Install dependencies
uv sync

# Run tests
uv run pytest

# Run linter
uv run ruff check .

# Run the server (stdio)
uv run airflow-mcp --transport stdio

# Run the server (HTTP)
uv run airflow-mcp --transport http --host 127.0.0.1 --port 8765
```

**Testing strategy:**
- No real network calls; tests patch `airflow_mcp.client_factory._import_airflow_client`.
- Reset registry cache in fixtures; assert `request_id` presence, URL precedence, and structured log fields.

**Observability:**
- Structured logs with `tool_start`, `tool_success/tool_error`, `duration_ms`, `response_bytes`, and context fields.

## License

Apache 2.0 - see [LICENSE](LICENSE) for details.
