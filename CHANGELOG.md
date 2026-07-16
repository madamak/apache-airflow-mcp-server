# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- **End-to-end test harness against real Airflow** (`scripts/e2e.sh`): spawns a
  dockerized Airflow 2.11 or 3.3 instance, seeds it with fixture
  DAG runs — including a failing task with retries, a noisy 3000-line log, a
  reschedule sensor, and dataset producer/consumer — then drives every MCP tool
  through FastMCP against the live REST API. The harness also installs the built
  wheel in an isolated environment and exercises its real console script. It
  runs in CI (`e2e.yml`: relevant PRs, main, nightly) and gates GitHub Releases, PyPI,
  and GHCR publication in `publish.yml`.

- **Zero-config single-instance mode**: configure one Airflow with just
  `AIRFLOW_MCP_HOST` + `AIRFLOW_MCP_USERNAME`/`AIRFLOW_MCP_PASSWORD` (or
  `AIRFLOW_MCP_TOKEN`) — no instances YAML required. The YAML registry remains
  the multi-instance path and takes precedence when set.
- **Read-only mode**: `AIRFLOW_MCP_READ_ONLY=true` skips registration of all
  write tools (trigger, clear, pause/unpause), preventing MCP mutations. Read
  tools can still return sensitive operational data and require least-privilege
  Airflow credentials.
- **Experimental Airflow 3 (API v2) support**: set `api_version: v2` (or
  `AIRFLOW_MCP_API_VERSION=v2`) and install the generated client version that
  matches the Airflow deployment.
  Includes JWT auth (basic credentials exchanged via `POST /auth/token` and
  refreshed on `AIRFLOW_MCP_TOKEN_REFRESH_SECONDS`), v2 endpoint and body
  mapping, `execution_date`→`logical_date` ordering translation, assets in
  place of datasets, structured log normalization, and Airflow 3 UI URL
  building/resolution. Live Airflow 3.3 coverage includes direct bearer auth and
  a forced basic-credential JWT refresh.
- CI now tests both generated client families on Python 3.10–3.13.

### Changed

- Clear operations now default to `dry_run=true`; callers must pass
  `dry_run=false` explicitly to mutate task or DAG-run state.
- `apache-airflow-client` dependency widened from `<3.0.0` to `<4.0.0`; pin
  the generated client release appropriate for your Airflow deployment (see
  the README compatibility matrix).
- When `api_version` is not set, it now defaults to whichever API matches the
  installed `apache-airflow-client` major (previously always `v1`).
  `api_version` values are validated (`v1`/`v2`).
- Release publication is now one semver-tag workflow. It validates the tag is
  on `main`, runs unit/lint and both live E2E entries, then publishes GHCR,
  PyPI, and finally the GitHub Release. Main/PR Docker jobs build without
  publishing an ungated `edge` image.
- `airflow_list_dag_runs` accepts `order_by="logical_date"` in addition to
  `execution_date` (each is mapped to whichever name the target API uses).
- Configuration and auth errors (missing credentials, client-major mismatch,
  JWT exchange failures) now surface as structured tool errors with codes
  `CONFIG_ERROR`/`AUTH_FAILED` and actionable messages, instead of being
  masked as `INTERNAL_ERROR`.
- Clear options that don't exist in the Airflow 3 API
  (`include_subdags`/`include_parentdag`; plus `include_*`/`reset_dag_runs`
  for `airflow_clear_dag_run`) are rejected with `INVALID_INPUT` on `v2`
  instances instead of being silently dropped.

### Fixed

- **Airflow 2 task logs were treated as a single line** (found by the new E2E
  suite): the v1 client returns log content as a `"[('host', 'text')]"` repr
  string with escaped newlines, but the parser only unwrapped it when the
  response itself was a string — real responses carry it in `.content`, so
  `tail_lines`, `filter_level`, `context_lines`, and line counts all operated
  on one giant line. The repr detection now applies to extracted content too.
- **`airflow_dataset_events` always failed on Airflow 2** (found by the new E2E
  suite): the v1 `get_dataset_events` endpoint has no `uri` parameter. The tool
  now resolves the dataset via `GET /datasets/{uri}` and filters events by
  `dataset_id`.
- **Airflow 3 clear-run responses could fail after a successful API call**: the
  generated 3.3 client cannot deserialize overlapping task-instance variants in
  the endpoint's union response. The v2 adapter now reads and validates the raw
  JSON response so a successful clear is not reported as an internal error.
- `airflow_get_task_instance` no longer errors when `max_rendered_bytes` is
  passed as a string/float.
- Airflow 3: `order_by` is sent in the list form newer clients require (with a
  string fallback), the `task_ids` filter no longer crashes against the 3.x
  client's single-value `task_id` parameter, trigger bodies always include the
  `logical_date` key (required-but-nullable on Airflow 3.0.x), and asset
  lookups paginate past the first 100 pattern matches.

## [0.1.x]

- Initial public release: multi-instance registry, discovery/read/write tools,
  UI URL resolution with SSRF guard, log filtering and truncation, structured
  JSON responses with `request_id`, stdio/HTTP transports.
