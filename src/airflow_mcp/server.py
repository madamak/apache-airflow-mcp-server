"""Airflow MCP Server using FastMCP v2 (Phase 1 with discovery tools)."""

import logging
import sys
from typing import Any, Literal

from fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import PlainTextResponse

from . import tools as airflow_tools
from ._version import get_version
from .config import config
from .errors import handle_errors

# Configure logging
handlers = [logging.StreamHandler(sys.stderr)]
if config.log_file:
    handlers.append(logging.FileHandler(config.log_file))

logging.basicConfig(
    level=getattr(logging, config.log_level.upper(), logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=handlers,
    force=True,
)
logger = logging.getLogger(__name__)


# Create FastMCP instance
PACKAGE_VERSION = get_version()
mcp = FastMCP(
    "airflow-mcp",
    instructions="MCP server for Apache Airflow operations",
    version=PACKAGE_VERSION,
    mask_error_details=True,
)


READ_ONLY_ANNOTATIONS = {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True}
WRITE_TOOL_ANNOTATIONS = {"readOnlyHint": False, "destructiveHint": True, "idempotentHint": False}


if config.http_block_get_on_mcp:

    @mcp.custom_route("/mcp", methods=["GET"])  # pragma: no cover - small wrapper
    async def block_get_mcp(request: Request) -> PlainTextResponse:
        logger.info("Blocking GET /mcp with 405 Method Not Allowed (SSE disabled)")
        return PlainTextResponse("Method Not Allowed", status_code=405, headers={"Allow": "POST"})

    @mcp.custom_route("/mcp/", methods=["GET"])  # pragma: no cover - small wrapper
    async def block_get_mcp_slash(request: Request) -> PlainTextResponse:
        logger.info("Blocking GET /mcp/ with 405 Method Not Allowed (SSE disabled)")
        return PlainTextResponse("Method Not Allowed", status_code=405, headers={"Allow": "POST"})


@mcp.custom_route("/health", methods=["GET"])
async def health_check(request: Request) -> PlainTextResponse:
    """Simple health check endpoint for HTTP deployments."""
    return PlainTextResponse("OK")


# Discovery tools (JSON-only)


@mcp.tool(annotations=READ_ONLY_ANNOTATIONS)
@handle_errors
def airflow_list_instances() -> dict[str, Any]:
    """List configured Airflow instance keys.

    Returns
    - Response dict: { "instances": [str], "default_instance": str | null, "request_id": str }
    - Raises: ToolError with compact JSON payload (`code`, `message`, `request_id`, optional `context`)
    """
    return airflow_tools.list_instances()


@mcp.tool(annotations=READ_ONLY_ANNOTATIONS)
@handle_errors
def airflow_describe_instance(instance: str) -> dict[str, Any]:
    """Describe a configured Airflow instance (host + metadata, never secrets).

    Parameters
    - instance: Instance key (e.g., "data-stg")

    Returns
    - Response dict: { "instance", "host", "api_version", "verify_ssl", "auth_type", "request_id": str }
    - Raises: ToolError with compact JSON payload (`code`, `message`, `request_id`, optional `context`)
    """
    return airflow_tools.describe_instance(instance)


@mcp.tool(annotations=READ_ONLY_ANNOTATIONS)
@handle_errors
def airflow_resolve_url(url: str) -> dict[str, Any]:
    """Parse an Airflow UI URL, resolve instance and identifiers.

    Parameters
    - url: Airflow UI URL (http/https)

    Returns
    - Response dict: { "instance", "dag_id"?, "dag_run_id"?, "task_id"?, "try_number"?, "route", "request_id" }
    - Raises: ToolError with compact JSON payload (`code`, `message`, `request_id`, optional `context`)
    """
    return airflow_tools.resolve_url(url)


@mcp.tool(annotations=READ_ONLY_ANNOTATIONS)
@handle_errors
def airflow_list_dags(
    instance: str | None = None,
    ui_url: str | None = None,
    limit: int | float | str = 100,
    offset: int | float | str = 0,
) -> dict[str, Any]:
    """List DAGs (pause state + UI link) for the target instance.

    Parameters
    - instance: Instance key (optional; mutually exclusive with ui_url)
    - ui_url: Airflow UI URL to resolve instance (optional; takes precedence - must match a configured host)
    - limit: Max results (default 100; accepts int/float/str, coerced to non-negative int, fractional values truncated)
    - offset: Offset for pagination (default 0; accepts int/float/str, coerced to non-negative int, fractional values truncated)

    Returns
    - Response dict: { "dags": [{ "dag_id", "is_paused", "ui_url" }], "count": int, "request_id": str }
    - Raises: ToolError with compact JSON payload (`code`, `message`, `request_id`, optional `context`)
    """
    return airflow_tools.list_dags(instance=instance, ui_url=ui_url, limit=limit, offset=offset)


@mcp.tool(annotations=READ_ONLY_ANNOTATIONS)
@handle_errors
def airflow_get_dag(
    instance: str | None = None, ui_url: str | None = None, dag_id: str | None = None
) -> dict[str, Any]:
    """Get DAG details and a UI link.

    Parameters
    - instance | ui_url: Provide one; `ui_url` auto-resolves/validates the host.
    - dag_id: Required when only `instance` is supplied.

    Returns
    - Response dict: { "dag": object, "ui_url": str, "request_id": str }
    - Raises: ToolError with compact JSON payload (`code`, `message`, `request_id`, optional `context`)
    """
    return airflow_tools.get_dag(instance=instance, ui_url=ui_url, dag_id=dag_id)


@mcp.tool(annotations=READ_ONLY_ANNOTATIONS)
@handle_errors
def airflow_list_dag_runs(
    instance: str | None = None,
    ui_url: str | None = None,
    dag_id: str | None = None,
    limit: int | float | str = 100,
    offset: int | float | str = 0,
    state: list[str] | None = None,
    order_by: Literal["start_date", "end_date", "execution_date"] | None = None,
    descending: bool = True,
) -> dict[str, Any]:
    """List DAG runs (defaults to execution_date DESC) with per-run UI URLs.

    Parameters
    - instance: Instance key (optional)
    - ui_url: Airflow UI URL to resolve instance/dag_id (optional)
    - dag_id: DAG identifier (required if ui_url not provided)
    - limit: Max results (default 100; accepts int/float/str, coerced to non-negative int, fractional values truncated)
    - offset: Offset for pagination (default 0; accepts int/float/str, coerced to non-negative int, fractional values truncated)
    - state: List of states to filter by (optional)
    - order_by: Optional `"start_date"`, `"end_date"`, or `"execution_date"` (omit to use ``execution_date``)
    - descending: Sort direction (default True). Ignored when order_by is omitted; defaults always use execution_date descending

    Returns
    - Response dict: { "dag_runs": [{ "dag_run_id", "state", "start_date", "end_date", "ui_url" }], "count": int, "request_id": str }
    - Raises: ToolError with compact JSON payload (`code`, `message`, `request_id`, optional `context`)
    """
    return airflow_tools.list_dag_runs(
        instance=instance,
        ui_url=ui_url,
        dag_id=dag_id,
        limit=limit,
        offset=offset,
        state=state,
        order_by=order_by,
        descending=descending,
    )


@mcp.tool(annotations=READ_ONLY_ANNOTATIONS)
@handle_errors
def airflow_get_dag_run(
    instance: str | None = None,
    ui_url: str | None = None,
    dag_id: str | None = None,
    dag_run_id: str | None = None,
) -> dict[str, Any]:
    """Get a single DAG run and a UI link.

    Parameters
    - instance: Instance key (optional)
    - ui_url: Airflow UI URL to resolve instance/dag/dag_run (optional)
    - dag_id: DAG identifier
    - dag_run_id: DAG run identifier

    Returns
    - Response dict: { "dag_run": object, "ui_url": str, "request_id": str }
    """
    return airflow_tools.get_dag_run(
        instance=instance, ui_url=ui_url, dag_id=dag_id, dag_run_id=dag_run_id
    )


@mcp.tool(annotations=READ_ONLY_ANNOTATIONS)
@handle_errors
def airflow_list_task_instances(
    instance: str | None = None,
    ui_url: str | None = None,
    dag_id: str | None = None,
    dag_run_id: str | None = None,
    limit: int | float | str = 100,
    offset: int | float | str = 0,
    state: list[str] | None = None,
    task_ids: list[str] | None = None,
) -> dict[str, Any]:
    """List task instances for a DAG run (state, try_number, per-attempt log URL).

    Parameters
    - instance: Instance key (optional)
    - ui_url: Airflow UI URL to resolve instance/dag/dag_run (optional)
    - dag_id: DAG identifier
    - dag_run_id: DAG run identifier
    - limit: Max results (default 100; accepts int/float/str, coerced to non-negative int, fractional values truncated)
    - offset: Offset for pagination (default 0; accepts int/float/str, coerced to non-negative int, fractional values truncated)
    - state: Optional list of task states (case-insensitive). When provided, only matching states are returned.
    - task_ids: Optional list of task identifiers to include.

    Returns
    - Response dict: {
        "task_instances": [{ "task_id", "state", "try_number", "ui_url" }],
        "count": int,
        "total_entries"?: int,
        "filters"?: { "state": [...], "task_ids": [...] },
        "request_id": str
      }
    - Raises: ToolError with compact JSON payload (`code`, `message`, `request_id`, optional `context`)
    """
    return airflow_tools.list_task_instances(
        instance=instance,
        ui_url=ui_url,
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        limit=limit,
        offset=offset,
        state=state,
        task_ids=task_ids,
    )


@mcp.tool(annotations=READ_ONLY_ANNOTATIONS)
@handle_errors
def airflow_get_task_instance(
    instance: str | None = None,
    ui_url: str | None = None,
    dag_id: str | None = None,
    dag_run_id: str | None = None,
    task_id: str | None = None,
    include_rendered: bool = False,
    max_rendered_bytes: int | float | str = 100_000,
) -> dict[str, Any]:
    """Return task metadata, config, attempt summary, optional rendered fields, and UI URLs.

    Parameters
    - instance | ui_url: Target selection (URL precedence)
    - dag_id, dag_run_id, task_id: Required identifiers (unless resolved from ui_url)
    - include_rendered: When true, include rendered template fields (truncated using max_rendered_bytes)
    - max_rendered_bytes: Byte cap for rendered fields payload (default 100KB; accepts int/float/str, coerced to positive int, fractional values truncated)

    Returns
    - Response dict: { "task_instance": {...}, "task_config": {...}, "attempts": {...}, "ui_url": {...}, "request_id": str, "rendered_fields"?: {...} }

    Notes
    - `attempts.try_number` is the authoritative input for `airflow_get_task_instance_logs`.
    - Rendered fields include `bytes_returned` and `truncated` metadata.
    - Sensors increment `try_number` on every reschedule, so treat it as an attempt index; the derived retries counters are heuristic.
    """

    return airflow_tools.get_task_instance(
        instance=instance,
        ui_url=ui_url,
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        include_rendered=include_rendered,
        max_rendered_bytes=max_rendered_bytes,
    )


@mcp.tool(annotations=READ_ONLY_ANNOTATIONS)
@handle_errors
def airflow_get_task_instance_logs(
    instance: str | None = None,
    ui_url: str | None = None,
    dag_id: str | None = None,
    dag_run_id: str | None = None,
    task_id: str | None = None,
    try_number: int | float | str | None = None,
    # New parameters
    filter_level: str | None = None,
    context_lines: int | float | str | None = None,
    tail_lines: int | float | str | None = None,
    max_bytes: int = 100_000,
) -> dict[str, Any]:
    """Fetch task instance logs with optional filtering and truncation.

    Large log handling: Logs >100MB automatically tail to last 10,000 lines (sets auto_tailed=true).
    Host-segmented responses are flattened into a single string using headers of the form `--- [worker] ---`,
    ensuring agents can reason about multi-host output.
    The tool requires an explicit `try_number`; callers should first retrieve it via `airflow_get_task_instance`.

    Filter order of operations:
    1. Auto-tail: If log >100MB, take last 10,000 lines
    2. tail_lines: Extract last N lines from log
    3. filter_level: Find matching lines by level (content filter)
    4. context_lines: Add surrounding lines around matches (symmetric: N before + N after)
    5. max_bytes: Hard cap on total output (UTF-8 safe truncation)

    Parameters
    - instance: Instance key (optional, mutually exclusive with ui_url)
    - ui_url: Airflow UI URL to resolve identifiers (optional)
    - dag_id, dag_run_id, task_id, try_number: Task instance identifiers (required)
    - filter_level: "error" | "warning" | "info" (optional) - Show only lines matching level
        * "error": ERROR, CRITICAL, FATAL, Exception, Traceback
        * "warning": WARN, WARNING + error patterns
        * "info": INFO + warning + error patterns
    - context_lines: N lines before/after each match (optional, clamped to [0, 1000]; accepts int/float/str, coerced to non-negative int, fractional values truncated)
    - tail_lines: Extract last N lines before filtering (optional, clamped to [0, 100000]; accepts int/float/str, coerced to non-negative int, fractional values truncated)
    - max_bytes: Maximum response size in bytes (default: 100KB ≈ 25K tokens, clamped to reasonable limit)

    Returns
    - Response dict with fields:
        * log: Normalized/filtered log text (host headers inserted when needed)
        * truncated: true if output exceeded max_bytes
        * auto_tailed: true if original log >100MB triggered auto-tail
        * bytes_returned: Actual byte size of returned log
        * original_lines: Line count before any filtering
        * returned_lines: Line count after all filtering/truncation
        * match_count: Number of lines matching filter_level (before context expansion)
        * meta.try_number: Attempt number for this task instance
        * meta.filters: Echo of effective filters applied (shows clamped values)
        * ui_url: Direct link to log view in Airflow UI
        * request_id: Correlates with server logs
    - Raises: ToolError with compact JSON payload (`code`, `message`, `request_id`, optional `context`)
    """
    return airflow_tools.get_task_instance_logs(
        instance=instance,
        ui_url=ui_url,
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        try_number=try_number,
        filter_level=filter_level,
        context_lines=context_lines,
        tail_lines=tail_lines,
        max_bytes=max_bytes,
    )


@mcp.tool(annotations=READ_ONLY_ANNOTATIONS)
@handle_errors
def airflow_dataset_events(
    instance: str | None = None,
    ui_url: str | None = None,
    dataset_uri: str | None = None,
    limit: int | float | str = 50,
) -> dict[str, Any]:
    """List dataset events.

    Parameters
    - instance: Instance key (optional)
    - ui_url: Airflow UI URL to resolve instance (optional)
    - dataset_uri: Dataset URI (required)
    - limit: Max results (default 50; accepts int/float/str, coerced to non-negative int, fractional values truncated)

    Returns
    - Response dict: { "events": [object], "count": int, "request_id": str }
    """
    return airflow_tools.dataset_events(
        instance=instance, ui_url=ui_url, dataset_uri=dataset_uri, limit=limit
    )


@mcp.tool(annotations=WRITE_TOOL_ANNOTATIONS)
@handle_errors
def airflow_trigger_dag(
    instance: str | None = None,
    ui_url: str | None = None,
    dag_id: str | None = None,
    dag_run_id: str | None = None,
    logical_date: str | None = None,
    conf: dict | str | None = None,
    note: str | None = None,
) -> str:
    """Trigger a DAG run with optional configuration.

    Parameters
    - instance: Instance key (optional; mutually exclusive with ui_url)
    - ui_url: Airflow UI URL to resolve instance (optional; takes precedence)
    - dag_id: DAG identifier (required if ui_url not provided)
    - dag_run_id: Custom run id (optional)
    - logical_date: Logical date/time for run (optional; ISO8601)
    - conf: Configuration object as dict or JSON string (optional)
    - note: Run note/comment (optional)

    Returns
    - Response dict: { "dag_run_id": str, "ui_url": str }
    - Raises: ToolError with compact JSON payload (`code`, `message`, `request_id`, optional `context`)
    """
    return airflow_tools.trigger_dag(
        instance=instance,
        ui_url=ui_url,
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        logical_date=logical_date,
        conf=conf,
        note=note,
    )


@mcp.tool(annotations=WRITE_TOOL_ANNOTATIONS)
@handle_errors
def airflow_clear_task_instances(
    instance: str | None = None,
    ui_url: str | None = None,
    dag_id: str | None = None,
    task_ids: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    include_subdags: bool | None = None,
    include_parentdag: bool | None = None,
    include_upstream: bool | None = None,
    include_downstream: bool | None = None,
    include_future: bool | None = None,
    include_past: bool | None = None,
    dry_run: bool | None = None,
    reset_dag_runs: bool | None = None,
) -> dict[str, Any]:
    """Clear task instances for a DAG across one or more runs using Airflow's native filter set (destructive).

    Parameters
    - instance: Instance key (optional; mutually exclusive with ui_url)
    - ui_url: Airflow UI URL to resolve instance (optional; takes precedence)
    - dag_id: DAG identifier (required if ui_url not provided)
    - task_ids: List of task IDs to clear (optional)
    - start_date: ISO8601 start date filter (optional)
    - end_date: ISO8601 end date filter (optional)
    - include_subdags: Include subDAGs (optional)
    - include_parentdag: Include parent DAG (optional)
    - include_upstream: Include upstream tasks (optional)
    - include_downstream: Include downstream tasks (optional)
    - include_future: Include future runs (optional)
    - include_past: Include past runs (optional)
    - dry_run: If true, perform a dry-run only (optional)
    - reset_dag_runs: Reset DagRun state (optional)

    Returns
    - Response dict: { "dag_id": str, "cleared": object, "request_id": str }
    - Raises: ToolError with compact JSON payload (`code`, `message`, `request_id`, optional `context`)
    """
    return airflow_tools.clear_task_instances(
        instance=instance,
        ui_url=ui_url,
        dag_id=dag_id,
        task_ids=task_ids,
        start_date=start_date,
        end_date=end_date,
        include_subdags=include_subdags,
        include_parentdag=include_parentdag,
        include_upstream=include_upstream,
        include_downstream=include_downstream,
        include_future=include_future,
        include_past=include_past,
        dry_run=dry_run,
        reset_dag_runs=reset_dag_runs,
    )


@mcp.tool(annotations=WRITE_TOOL_ANNOTATIONS)
@handle_errors
def airflow_clear_dag_run(
    instance: str | None = None,
    ui_url: str | None = None,
    dag_id: str | None = None,
    dag_run_id: str | None = None,
    include_subdags: bool | None = None,
    include_parentdag: bool | None = None,
    include_upstream: bool | None = None,
    include_downstream: bool | None = None,
    dry_run: bool | None = None,
    reset_dag_runs: bool | None = None,
) -> dict[str, Any]:
    """Clear all task instances in a specific DAG run (destructive).

    Parameters
    - instance: Instance key (optional; mutually exclusive with ui_url)
    - ui_url: Airflow UI URL to resolve instance (optional; takes precedence)
    - dag_id: DAG identifier (required if ui_url not provided)
    - dag_run_id: DAG run identifier (required if ui_url not provided)
    - include_subdags: Include subDAGs (optional)
    - include_parentdag: Include parent DAG (optional)
    - include_upstream: Include upstream tasks (optional)
    - include_downstream: Include downstream tasks (optional)
    - dry_run: If true, perform a dry-run only (optional)
    - reset_dag_runs: Reset DagRun state (optional)

    Returns
    - Response dict: { "dag_id": str, "dag_run_id": str, "cleared": object, "request_id": str }
    - Raises: ToolError with compact JSON payload (`code`, `message`, `request_id`, optional `context`)
    """
    return airflow_tools.clear_dag_run(
        instance=instance,
        ui_url=ui_url,
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        include_subdags=include_subdags,
        include_parentdag=include_parentdag,
        include_upstream=include_upstream,
        include_downstream=include_downstream,
        dry_run=dry_run,
        reset_dag_runs=reset_dag_runs,
    )


@mcp.tool(annotations=WRITE_TOOL_ANNOTATIONS)
@handle_errors
def airflow_pause_dag(
    instance: str | None = None, ui_url: str | None = None, dag_id: str | None = None
) -> dict[str, Any]:
    """Pause DAG scheduling (sets `is_paused=True` and returns UI link).

    Parameters
    - instance: Instance key (optional; mutually exclusive with ui_url)
    - ui_url: Airflow UI URL to resolve instance (optional; takes precedence)
    - dag_id: DAG identifier (required if ui_url not provided)

    Returns
    - Response dict: { "dag_id": str, "is_paused": true, "ui_url": str, "request_id": str }
    - Raises: ToolError with compact JSON payload (`code`, `message`, `request_id`, optional `context`)
    """
    return airflow_tools.pause_dag(instance=instance, ui_url=ui_url, dag_id=dag_id)


@mcp.tool(annotations=WRITE_TOOL_ANNOTATIONS)
@handle_errors
def airflow_unpause_dag(
    instance: str | None = None, ui_url: str | None = None, dag_id: str | None = None
) -> dict[str, Any]:
    """Resume DAG scheduling (sets `is_paused=False` and returns UI link).

    Parameters
    - instance: Instance key (optional; mutually exclusive with ui_url)
    - ui_url: Airflow UI URL to resolve instance (optional; takes precedence)
    - dag_id: DAG identifier (required if ui_url not provided)

    Returns
    - Response dict: { "dag_id": str, "is_paused": false, "ui_url": str, "request_id": str }
    - Raises: ToolError with compact JSON payload (`code`, `message`, `request_id`, optional `context`)
    """
    return airflow_tools.unpause_dag(instance=instance, ui_url=ui_url, dag_id=dag_id)


def run_server() -> None:
    """Run the server with selected transport.

    Supports --transport http|stdio|sse and optional --host/--port overrides.
    """

    try:
        # Determine transport
        transport = "stdio"
        host = config.http_host
        port = config.http_port

        if "--transport" in sys.argv:
            idx = sys.argv.index("--transport") + 1
            if idx < len(sys.argv):
                transport = sys.argv[idx]

        if "--host" in sys.argv:
            idx = sys.argv.index("--host") + 1
            if idx < len(sys.argv):
                host = sys.argv[idx]

        if "--port" in sys.argv:
            idx = sys.argv.index("--port") + 1
            if idx < len(sys.argv):
                try:
                    port = int(sys.argv[idx])
                except ValueError:
                    logger.warning(
                        f"Invalid --port value '{sys.argv[idx]}', falling back to {port}"
                    )

        logger.info(f"Starting Airflow MCP Server with {transport} transport")
        if transport in {"http", "sse"}:
            logger.info(f"Binding to {host}:{port}")

        if transport == "http":
            mcp.run(transport="http", host=host, port=port)
        elif transport == "sse":
            logger.warning(
                "SSE transport is legacy and retained for backward compatibility. Prefer 'http' transport."
            )
            mcp.run(transport="sse", host=host, port=port)
        else:
            mcp.run()  # stdio transport

    except KeyboardInterrupt:
        logger.info("Server shutdown requested")
    except Exception as e:  # pragma: no cover - defensive
        logger.error(f"Server error: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    run_server()
