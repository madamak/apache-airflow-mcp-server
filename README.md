# Apache Airflow MCP Server

<!-- mcp-name: io.github.madamak/apache-airflow-mcp-server -->

[![MCP](https://img.shields.io/badge/MCP-Server-blueviolet)](https://modelcontextprotocol.io)
[![PyPI](https://img.shields.io/pypi/v/apache-airflow-mcp-server)](https://pypi.org/project/apache-airflow-mcp-server/)
[![Python](https://img.shields.io/pypi/pyversions/apache-airflow-mcp-server)](https://pypi.org/project/apache-airflow-mcp-server/)
[![Airflow](https://img.shields.io/badge/live--tested-2.11%20%7C%203.3-017CEE?logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![CI](https://github.com/madamak/apache-airflow-mcp-server/actions/workflows/ci.yml/badge.svg)](https://github.com/madamak/apache-airflow-mcp-server/actions/workflows/ci.yml)
[![Security](https://github.com/madamak/apache-airflow-mcp-server/actions/workflows/security.yml/badge.svg)](https://github.com/madamak/apache-airflow-mcp-server/actions/workflows/security.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

> Independent community project; not affiliated with or endorsed by the Apache Software Foundation.

Connect Claude, Cursor, and any [MCP](https://modelcontextprotocol.io) client to your Apache Airflow deployments — and let AI agents debug failed DAGs for you.

Paste an Airflow UI link from a PagerDuty/Datadog alert and ask *"why did this fail?"* — the agent resolves the URL, finds the failed tasks, pulls the error lines from the logs (server-side filtered so it doesn't blow the context window), and can re-trigger or clear runs. Write tools carry destructive-operation annotations that MCP clients can use to request confirmation.

## Highlights

- 🔍 **Incident-response first** — resolve Airflow UI URLs straight to the failing task, filter logs by error level with context lines, follow `try_number` semantics correctly (sensors included)
- 🏢 **Multi-instance** — one server for same-API-family dev/staging/prod targets, with per-instance credentials and an SSRF guard that rejects unknown hosts
- 🔒 **Safety controls** — opt-in read-only mode (`AIRFLOW_MCP_READ_ONLY=true`) that never registers write tools; write tools annotated as destructive; configured credentials are redacted from instance responses and operation logs
- 📉 **Token-efficient** — log tailing, level filtering, byte caps, and truncation metadata designed for LLM context windows
- 🧭 **Airflow 2 and 3** — API v1/v2 adapters with live E2E against Airflow 2.11 and 3.3, including JWT auth
- 📎 **Traceable** — every response carries a `request_id` that matches the structured server logs

## Quickstart

### 1. Install

```bash
uv tool install apache-airflow-mcp-server \
  --with 'apache-airflow-client==3.3.0'  # replace 3.3.0 with your Airflow version
```

> **Airflow 2.11?** Use `--with 'apache-airflow-client==2.10.0'` instead; 2.10.0 is the final generated v1 client and targets Airflow 2's stable API. See [Airflow compatibility](#airflow-compatibility) before using a different release.

### 2. Connect your MCP client

The fastest path is a single instance configured entirely with environment variables — no config file needed.

<details open>
<summary><b>Claude Code</b></summary>

```bash
claude mcp add airflow \
  --env AIRFLOW_MCP_HOST=https://airflow.example.com \
  --env AIRFLOW_MCP_USERNAME=admin \
  --env AIRFLOW_MCP_PASSWORD=your-password \
  --env AIRFLOW_MCP_READ_ONLY=true \
  -- uvx --from apache-airflow-mcp-server \
  --with apache-airflow-client==3.3.0 airflow-mcp --transport stdio
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
      "args": ["--from", "apache-airflow-mcp-server", "--with", "apache-airflow-client==3.3.0", "airflow-mcp", "--transport", "stdio"],
      "env": {
        "AIRFLOW_MCP_HOST": "https://airflow.example.com",
        "AIRFLOW_MCP_USERNAME": "admin",
        "AIRFLOW_MCP_PASSWORD": "your-password",
        "AIRFLOW_MCP_READ_ONLY": "true"
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
      "args": ["--from", "apache-airflow-mcp-server", "--with", "apache-airflow-client==3.3.0", "airflow-mcp", "--transport", "stdio"],
      "env": {
        "AIRFLOW_MCP_HOST": "https://airflow.example.com",
        "AIRFLOW_MCP_USERNAME": "admin",
        "AIRFLOW_MCP_PASSWORD": "your-password",
        "AIRFLOW_MCP_READ_ONLY": "true"
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
      "args": ["--from", "apache-airflow-mcp-server", "--with", "apache-airflow-client==3.3.0", "airflow-mcp", "--transport", "stdio"],
      "env": {
        "AIRFLOW_MCP_HOST": "https://airflow.example.com",
        "AIRFLOW_MCP_USERNAME": "admin",
        "AIRFLOW_MCP_PASSWORD": "your-password",
        "AIRFLOW_MCP_READ_ONLY": "true"
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
AIRFLOW_MCP_READ_ONLY=true \
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
> *"Show the failed task's error context and tell me the smallest recovery action."*

## Airflow compatibility

The server talks to Airflow through the generated `apache-airflow-client`.
For Airflow 3, match the client release to your Airflow release: generated
models can change within a major version, and a newer client is not guaranteed
to deserialize an older server's responses correctly. Airflow 2.11 uses the
final v1 client release, 2.10.0, against Airflow 2's stable API.

| Your Airflow | REST API | Install | Live E2E status |
|---|---|---|---|
| 3.3 | v2 | `uv tool install apache-airflow-mcp-server --with 'apache-airflow-client==3.3.0'` | ✅ 3.3.0 |
| 2.11 | v1 | `uv tool install apache-airflow-mcp-server --with 'apache-airflow-client==2.10.0'` | ✅ Airflow 2.11 + final v1 client 2.10.0 |
| 3.0–3.2 | v2 | Pin the client to the deployed Airflow 3 version | 🧪 Not in the current live matrix |
| 2.5–2.10 | v1 | Use the final v1 client, `apache-airflow-client==2.10.0` | 🧪 Not in the current live matrix |

When `api_version` isn't set, the server assumes the API matching the installed
client (`v1` for a 2.x client, `v2` for 3.x). Set
`AIRFLOW_MCP_API_VERSION` (or `api_version:` in the registry) explicitly to
catch a major-version mismatch early.

One server process can currently load only one generated client major. All
instances in a registry must therefore use the same API family; run separate
MCP server processes for Airflow 2 and Airflow 3. Mixed-version support requires
a future client-adapter change and is not advertised as working today.

Airflow 3 notes:

- **Auth**: bearer tokens are passed through as JWTs; basic credentials are automatically exchanged for a JWT via `POST /auth/token` and refreshed periodically (`AIRFLOW_MCP_TOKEN_REFRESH_SECONDS`, default 3600 — keep it below your deployment's JWT expiry, and note there is no automatic re-auth on 401 yet).
- `execution_date` ordering maps to `logical_date`, datasets map to assets, and UI links use the Airflow 3 route scheme. Tool names and the core workflow stay stable; documented fields and options can differ by API family.
- Clear options that no longer exist in Airflow 3 (`include_subdags`/`include_parentdag`, and the `include_*`/`reset_dag_runs` options of `airflow_clear_dag_run`) are rejected with `INVALID_INPUT` rather than silently narrowing a destructive operation.

Both client families are exercised on relevant pull requests and main pushes. Bug reports from real Airflow deployments are very welcome!

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

data-prod:
  host: https://airflow.data-prod.example.com/
  api_version: v1        # Keep one client/API family per server process
  auth:
    type: bearer
    token: ${AIRFLOW_DATA_PROD_TOKEN}
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

The quickstarts set `AIRFLOW_MCP_READ_ONLY=true`: write tools (trigger, clear,
pause/unpause) are never registered. This prevents MCP mutations, but read tools
can still disclose sensitive logs, configuration, rendered fields, and DAG-run
data; use least-privilege Airflow credentials.

To enable recovery operations deliberately, set `AIRFLOW_MCP_READ_ONLY=false`.
Write tools then carry MCP `destructiveHint` annotations that clients can use
when deciding whether to request confirmation. Annotations are advisory, so do
not enable writes unless the Airflow credentials and MCP client's approval
behavior are appropriate for the target environment.

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

**Write** (annotated destructive so clients can require approval; hidden entirely in read-only mode)

| Tool | Description |
|---|---|
| `airflow_trigger_dag` | Trigger a run with optional conf/logical date/note |
| `airflow_clear_task_instances` | Clear task instances across runs (`dry_run=true` by default) |
| `airflow_clear_dag_run` | Clear a whole run (`dry_run=true` by default) |
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

> **Note on `try_number`:** reschedule-mode sensors could increment it on every
> reschedule through Airflow 2.9; Airflow 2.10+ no longer does. Always read it
> from `airflow_get_task_instance` rather than guessing—the derived
> `retries_consumed`/`retries_remaining` fields are heuristics.

## Deployment

### Docker

```bash
docker run -p 127.0.0.1:8765:8765 \
  -e AIRFLOW_MCP_HOST=https://airflow.example.com \
  -e AIRFLOW_MCP_USERNAME=admin \
  -e AIRFLOW_MCP_PASSWORD=your-password \
  -e AIRFLOW_MCP_READ_ONLY=true \
  ghcr.io/madamak/apache-airflow-mcp-server:latest
```

Or build locally with `docker build -t airflow-mcp .`

The release image contains the lockfile's Airflow 3.3 client and serves
streamable HTTP on `:8765` (`/mcp` endpoint, `/health` for probes). The MCP HTTP
endpoint has no built-in caller authentication: keep it loopback-bound or place
it behind an authenticated private proxy. Mount a same-API-family registry YAML
for multi-instance setups. Airflow 2 deployments should use the pinned local
installation path above until a separate v1 image is published.

CI audits the exact image's installed Python dependencies and scans both the
read-only and write-enabled MCP tool surfaces with a pinned Cisco MCP Scanner
release's YARA analyzer. The security workflow fails on incomplete scans or any
untriaged YARA finding. Starting with v1.0.1, release assets include the
machine-readable scan reports, and release image manifests carry attached SBOM
and provenance attestations covering their broader package inventory. These
are automated checks, not a security certification or substitute for
deployment-specific review.

### FastMCP tooling

A `fastmcp.json` is included so FastMCP-aware tooling can auto-discover the entrypoint and deployment defaults.

## Development

```bash
uv sync                 # install dependencies
uv run pytest           # unit tests (no real network; the Airflow client is mocked)
uv run ruff check .     # lint
uv run ruff format .    # format
uv run airflow-mcp --transport stdio   # run locally
./scripts/e2e.sh af2    # end-to-end against Airflow 2.11
./scripts/e2e.sh af3    # end-to-end against Airflow 3.3
                        # Both seed failures/noisy logs and drive every tool
                        # through MCP. Set E2E_KEEP=1 to keep the instance up.
```

CI runs the unit suite against both `apache-airflow-client` families on Python 3.10–3.13. Relevant pull requests, main pushes, nightly runs, and releases also exercise live dockerized Airflow 2.11 and 3.3. See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines and [AGENTS.md](AGENTS.md) if you're pointing a coding agent at this repo (it's written for that).

## Contributing

Issues and PRs are welcome — especially:

- Reports from real Airflow incident-response workflows and version combinations
- Bounded-log, diagnosis-safety, and URL-first workflow improvements
- Client setup recipes for more MCP hosts and deployment types

If this server saves you a debugging session, a ⭐ helps other Airflow teams find it.

## License

Apache 2.0 — see [LICENSE](LICENSE).
