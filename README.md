# Apache Airflow MCP Server

[![MCP](https://img.shields.io/badge/MCP-Server-blueviolet)](https://modelcontextprotocol.io)
[![PyPI](https://img.shields.io/pypi/v/apache-airflow-mcp-server)](https://pypi.org/project/apache-airflow-mcp-server/)
[![Python](https://img.shields.io/pypi/pyversions/apache-airflow-mcp-server)](https://pypi.org/project/apache-airflow-mcp-server/)
[![Airflow](https://img.shields.io/badge/Airflow-2.5%E2%80%932.11%20%7C%203.x-017CEE?logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![CI](https://github.com/madamak/apache-airflow-mcp-server/actions/workflows/ci.yml/badge.svg)](https://github.com/madamak/apache-airflow-mcp-server/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

Connect Claude, Cursor, and any [MCP](https://modelcontextprotocol.io) client to your Apache Airflow deployments — and let AI agents debug failed DAGs for you.

Paste an Airflow UI link from a PagerDuty/Datadog alert and ask *"why did this fail?"* — the agent resolves the URL, finds the failed tasks, pulls the error lines from the logs (server-side filtered so it doesn't blow the context window), and can re-trigger or clear runs **only with your approval**.

## Highlights

- 🔍 **Incident-response first** — resolve Airflow UI URLs straight to the failing task, filter logs by error level with context lines, follow `try_number` semantics correctly (sensors included)
- 🏢 **Multi-instance** — one server for dev/staging/prod across teams, with per-instance credentials and an SSRF guard that rejects unknown hosts
- 🔒 **Safe by default** — read-only mode (`AIRFLOW_MCP_READ_ONLY=true`), write tools annotated as destructive so clients prompt for confirmation, secrets never logged or echoed
- 📉 **Token-efficient** — log tailing, level filtering, byte caps, and truncation metadata designed for LLM context windows
- 🧭 **Airflow 2 and 3** — Airflow 2.5–2.11 (API v1) fully supported; Airflow 3 (API v2) supported experimentally, including JWT auth
- 📎 **Traceable** — every response carries a `request_id` that matches the structured server logs

## Quickstart

### 1. Install

```bash
uv tool install apache-airflow-mcp-server   # or: pip install apache-airflow-mcp-server
```

> **Airflow 2 (2.5–2.11)?** Install with the matching client: `uv tool install apache-airflow-mcp-server --with 'apache-airflow-client<3'` — see [Airflow compatibility](#airflow-compatibility). A plain install targets Airflow 3.

### 2. Connect your MCP client

The fastest path is a single instance configured entirely with environment variables — no config file needed.

<details open>
<summary><b>Claude Code</b></summary>

```bash
claude mcp add airflow \
  --env AIRFLOW_MCP_HOST=https://airflow.example.com \
  --env AIRFLOW_MCP_USERNAME=admin \
  --env AIRFLOW_MCP_PASSWORD=your-password \
  -- uvx --from apache-airflow-mcp-server airflow-mcp --transport stdio
```
</details>

<details>
<summary><b>Claude Desktop</b></summary>

Add to `claude_desktop_config.json` (Settings → Developer → Edit Config):

```json
{
  "mcpServers": {
    "airflow": {
      "command": "uvx",
      "args": ["--from", "apache-airflow-mcp-server", "airflow-mcp", "--transport", "stdio"],
      "env": {
        "AIRFLOW_MCP_HOST": "https://airflow.example.com",
        "AIRFLOW_MCP_USERNAME": "admin",
        "AIRFLOW_MCP_PASSWORD": "your-password"
      }
    }
  }
}
```
</details>

<details>
<summary><b>Cursor</b></summary>

Add to `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "airflow": {
      "command": "uvx",
      "args": ["--from", "apache-airflow-mcp-server", "airflow-mcp", "--transport", "stdio"],
      "env": {
        "AIRFLOW_MCP_HOST": "https://airflow.example.com",
        "AIRFLOW_MCP_USERNAME": "admin",
        "AIRFLOW_MCP_PASSWORD": "your-password"
      }
    }
  }
}
```
</details>

<details>
<summary><b>VS Code (Copilot)</b></summary>

Add to `.vscode/mcp.json`:

```json
{
  "servers": {
    "airflow": {
      "type": "stdio",
      "command": "uvx",
      "args": ["--from", "apache-airflow-mcp-server", "airflow-mcp", "--transport", "stdio"],
      "env": {
        "AIRFLOW_MCP_HOST": "https://airflow.example.com",
        "AIRFLOW_MCP_USERNAME": "admin",
        "AIRFLOW_MCP_PASSWORD": "your-password"
      }
    }
  }
}
```
</details>

<details>
<summary><b>Any client, over HTTP</b></summary>

Run the server yourself and point the client at the endpoint:

```bash
AIRFLOW_MCP_HOST=https://airflow.example.com \
AIRFLOW_MCP_USERNAME=admin AIRFLOW_MCP_PASSWORD=your-password \
airflow-mcp --transport http --host 127.0.0.1 --port 8765
```

```json
{ "mcpServers": { "airflow": { "url": "http://127.0.0.1:8765/mcp" } } }
```

Health check: `GET /health` → `200 OK`.
</details>

### 3. Ask your agent something

> *"Why did the latest run of `etl_pipeline` fail?"*
>
> *"https://airflow.example.com/dags/etl_pipeline/grid — what happened here, and is it safe to clear?"*
>
> *"Pause every DAG owned by data-eng on staging."*

## Airflow compatibility

The server talks to Airflow through the official `apache-airflow-client`, and supports both client majors. A plain install resolves the newest client (3.x, for Airflow 3); Airflow 2 users pin the 2.x client alongside:

| Your Airflow | REST API | Install | Status |
|---|---|---|---|
| 3.x | v2 | `uv tool install apache-airflow-mcp-server` (plain install) | 🧪 Experimental |
| 2.5 – 2.11 | v1 | `uv tool install apache-airflow-mcp-server --with 'apache-airflow-client<3'` (or `pip install apache-airflow-mcp-server 'apache-airflow-client<3'`) | ✅ Stable |

When `api_version` isn't set, the server assumes the API matching the installed client (`v1` for the 2.x client, `v2` for 3.x), so the pairings above work with no further configuration. Set `AIRFLOW_MCP_API_VERSION` (or `api_version:` per instance in the registry YAML) to `v1`/`v2` explicitly if you want to be sure — a mismatch between the client and the instance produces an error explaining exactly which client to install.

Airflow 3 notes:

- **Auth**: bearer tokens are passed through as JWTs; basic credentials are automatically exchanged for a JWT via `POST /auth/token` and refreshed periodically (`AIRFLOW_MCP_TOKEN_REFRESH_SECONDS`, default 3600 — keep it below your deployment's JWT expiry, and note there is no automatic re-auth on 401 yet).
- `execution_date` ordering maps to `logical_date`, datasets map to assets, and UI links use the Airflow 3 route scheme — tool inputs/outputs stay the same.
- Clear options that no longer exist in Airflow 3 (`include_subdags`/`include_parentdag`, and the `include_*`/`reset_dag_runs` options of `airflow_clear_dag_run`) are rejected with `INVALID_INPUT` rather than silently narrowing a destructive operation.

Both client majors are exercised in CI on every commit. Bug reports from real Airflow 3 deployments are very welcome!

## Configuration

### Single instance (env vars only)

| Variable | Required | Description |
|---|---|---|
| `AIRFLOW_MCP_HOST` | ✅ | Airflow base URL, e.g. `https://airflow.example.com` |
| `AIRFLOW_MCP_USERNAME` / `AIRFLOW_MCP_PASSWORD` | ✅* | Basic auth credentials |
| `AIRFLOW_MCP_TOKEN` | ✅* | Bearer/JWT token (used instead of basic auth) |
| `AIRFLOW_MCP_API_VERSION` | | `v1` (Airflow 2) or `v2` (Airflow 3); defaults to whichever matches the installed `apache-airflow-client` |
| `AIRFLOW_MCP_VERIFY_SSL` | | Verify TLS certificates (default `true`) |

\* provide either username+password or a token.

### Multiple instances (registry YAML)

Point `AIRFLOW_MCP_INSTANCES_FILE` at a YAML registry (it takes precedence over the single-instance env vars). Values may reference environment variables with `${VAR}`:

```yaml
data-stg:
  host: https://airflow.data-stg.example.com/
  api_version: v1        # Airflow 2
  verify_ssl: true
  auth:
    type: basic
    username: ${AIRFLOW_DATA_STG_USERNAME}
    password: ${AIRFLOW_DATA_STG_PASSWORD}

ml-prod:
  host: https://airflow.ml-prod.example.com/
  api_version: v2        # Airflow 3
  auth:
    type: bearer
    token: ${AIRFLOW_ML_PROD_TOKEN}
```

Every tool accepts either an `instance` key (`data-stg`) or a `ui_url` — a full http(s) Airflow UI URL whose host is resolved against the registry, with unknown hosts rejected (SSRF guard). `ui_url` also auto-fills `dag_id`/`dag_run_id`/`task_id` when the link contains them. If both `instance` and `ui_url` are passed and disagree, the call fails with `INSTANCE_MISMATCH` rather than guessing.

**Kubernetes tip:** mount the registry from a Secret at `/config/instances.yaml` and set `AIRFLOW_MCP_INSTANCES_FILE=/config/instances.yaml`.

### Server options

| Variable | Default | Description |
|---|---|---|
| `AIRFLOW_MCP_DEFAULT_INSTANCE` | | Default instance key (also names the env-var instance) |
| `AIRFLOW_MCP_READ_ONLY` | `false` | Don't register write tools at all |
| `AIRFLOW_MCP_HTTP_HOST` / `AIRFLOW_MCP_HTTP_PORT` | `127.0.0.1` / `8765` | HTTP transport bind |
| `AIRFLOW_MCP_TIMEOUT_SECONDS` | `30` | Airflow API timeout |
| `AIRFLOW_MCP_TOKEN_REFRESH_SECONDS` | `3600` | Airflow 3: JWT refresh interval for basic-auth instances |
| `AIRFLOW_MCP_LOG_FILE` | | Optional log file path |
| `AIRFLOW_MCP_ENABLE_EXTENDED_CLEAR_PARAMS` | `false` | Enable `include_*` clear params (Airflow ≥2.6) |
| `AIRFLOW_MCP_HTTP_BLOCK_GET_ON_MCP` | `true` | Return 405 for `GET /mcp` (SSE reads) on HTTP deployments |

### Read-only mode

Pointing an AI agent at production? Set `AIRFLOW_MCP_READ_ONLY=true` and the write tools (trigger, clear, pause/unpause) are never registered — the agent can inspect everything but change nothing. Even with writes enabled, write tools carry MCP `destructiveHint` annotations so well-behaved clients ask for confirmation first.

## Tools

**Discovery & URL utilities**

| Tool | Description |
|---|---|
| `airflow_list_instances` | List configured instance keys and the default |
| `airflow_describe_instance` | Host, API version, auth type (secrets redacted) |
| `airflow_resolve_url` | Parse an Airflow UI URL into instance + dag/run/task identifiers |

**Read**

| Tool | Description |
|---|---|
| `airflow_list_dags` | DAGs with pause state and UI links |
| `airflow_get_dag` | DAG details |
| `airflow_list_dag_runs` | Runs with state filters and ordering (latest first by default) |
| `airflow_get_dag_run` | Single run details |
| `airflow_list_task_instances` | Task attempts for a run; filter by `state` / `task_ids` server-side |
| `airflow_get_task_instance` | Task metadata, retries, timings, optional rendered template fields |
| `airflow_get_task_instance_logs` | Logs with level filtering, tailing, context lines, and byte caps |
| `airflow_dataset_events` | Dataset (Airflow 2) / asset (Airflow 3) events |

**Write** (require client approval; hidden entirely in read-only mode)

| Tool | Description |
|---|---|
| `airflow_trigger_dag` | Trigger a run with optional conf/logical date/note |
| `airflow_clear_task_instances` | Clear task instances across runs (supports `dry_run`) |
| `airflow_clear_dag_run` | Clear a whole run (supports `dry_run`) |
| `airflow_pause_dag` / `airflow_unpause_dag` | Toggle DAG scheduling |

Every success payload includes a `request_id` for log correlation; failures raise a structured `ToolError` with `{code, message, request_id, context}`.

## The incident workflow

This is the flow the tools were designed around — going from an alert link to a diagnosis in four calls:

```python
# 1. Alert contains an Airflow UI link → resolve it
airflow_resolve_url("https://airflow.example.com/dags/etl_pipeline/grid?dag_run_id=...")
#    → {instance, dag_id, dag_run_id, ...}

# 2. Which tasks failed in this run?
airflow_list_task_instances(dag_id="etl_pipeline", dag_run_id="scheduled__2026-01-01",
                            state=["failed"])

# 3. Get attempt metadata (authoritative try_number, retries, timings)
ti = airflow_get_task_instance(dag_id="etl_pipeline",
                               dag_run_id="scheduled__2026-01-01",
                               task_id="transform_data")

# 4. Pull only the error lines, with context, capped for the LLM
airflow_get_task_instance_logs(dag_id="etl_pipeline",
                               dag_run_id="scheduled__2026-01-01",
                               task_id="transform_data",
                               try_number=ti["attempts"]["try_number"],
                               tail_lines=500, filter_level="error", context_lines=5)
```

Log responses include `truncated`, `auto_tailed` (logs >100MB tail automatically), `match_count`, and byte/line stats so the agent knows exactly what it's looking at. Host-segmented logs are flattened with `--- [worker-1] ---` headers; Airflow 3 structured logs are rendered as plain lines.

> **Note on `try_number`:** sensors increment it on every reschedule, so treat it as an attempt index. Always read it from `airflow_get_task_instance` rather than guessing — the derived `retries_consumed`/`retries_remaining` fields are heuristics.

## Deployment

### Docker

```bash
docker build -t airflow-mcp .
docker run -p 8765:8765 \
  -e AIRFLOW_MCP_HOST=https://airflow.example.com \
  -e AIRFLOW_MCP_USERNAME=admin \
  -e AIRFLOW_MCP_PASSWORD=your-password \
  airflow-mcp
```

The container serves streamable HTTP on `:8765` (`/mcp` endpoint, `/health` for probes). Mount a registry YAML for multi-instance setups.

### FastMCP tooling

A `fastmcp.json` is included so FastMCP-aware tooling can auto-discover the entrypoint and deployment defaults.

## Development

```bash
uv sync                 # install dependencies
uv run pytest           # tests (no real network; the Airflow client is mocked)
uv run ruff check .     # lint
uv run ruff format .    # format
uv run airflow-mcp --transport stdio   # run locally
```

CI runs the suite against both `apache-airflow-client` majors on Python 3.10/3.12/3.13. See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines and [AGENTS.md](AGENTS.md) if you're pointing a coding agent at this repo (it's written for that).

## Contributing

Issues and PRs are welcome — especially:

- Reports from real Airflow 3 deployments (the v2 support is new)
- Additional tools (XComs, variables, pools, backfills)
- Client setup recipes for more MCP hosts

If this server saves you a debugging session, a ⭐ helps other Airflow teams find it.

## License

Apache 2.0 — see [LICENSE](LICENSE).
