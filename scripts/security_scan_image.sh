#!/usr/bin/env bash

set -uo pipefail

PIP_AUDIT_VERSION="2.10.1"
MCP_SCANNER_VERSION="4.8.0"

if [[ $# -ne 2 ]]; then
  echo "usage: $0 IMAGE REPORT_DIR" >&2
  exit 2
fi

image="$1"
report_dir="$2"
mkdir -p "$report_dir"

if ! command -v docker >/dev/null || ! command -v jq >/dev/null || ! command -v uv >/dev/null; then
  echo "docker, jq, and uv are required" >&2
  exit 2
fi

if ! docker image inspect "$image" >/dev/null 2>&1; then
  echo "image not found: $image" >&2
  exit 2
fi

result=0
requirements="$report_dir/installed-requirements.txt"
audit_report="$report_dir/pip-audit.json"

if ! docker run --rm --entrypoint uv "$image" \
  pip freeze --python /app/.venv/bin/python \
  --exclude apache-airflow-mcp-server --strict >"$requirements"; then
  echo "could not extract the image's installed Python environment" >&2
  result=1
fi

if grep -Eq '^(bandit|pytest|pytest-asyncio|pytest-cov|ruff)==' "$requirements"; then
  echo "development dependencies are present in the runtime image" >&2
  result=1
fi

if ! uvx --from "pip-audit==$PIP_AUDIT_VERSION" pip-audit \
  --requirement "$requirements" --no-deps --disable-pip --strict \
  --format json --output "$audit_report"; then
  echo "runtime dependency audit failed" >&2
  result=1
fi

scan_surface() {
  local surface="$1"
  local read_only="$2"
  local expected_count="$3"
  local report="$report_dir/mcp-scan-$surface.json"
  local stderr_log="$report_dir/mcp-scan-$surface.stderr.log"

  if ! uvx --python 3.12 --from "cisco-ai-mcp-scanner==$MCP_SCANNER_VERSION" \
    mcp-scanner --log-level error --analyzers yara --format raw \
    stdio --stdio-command docker \
    --stdio-arg=run --stdio-arg=--rm --stdio-arg=-i \
    --stdio-arg=-e --stdio-arg=AIRFLOW_MCP_HOST=https://airflow.invalid \
    --stdio-arg=-e --stdio-arg=AIRFLOW_MCP_TOKEN=test-only-token \
    --stdio-arg=-e --stdio-arg=AIRFLOW_MCP_API_VERSION=v2 \
    --stdio-arg=-e --stdio-arg="AIRFLOW_MCP_READ_ONLY=$read_only" \
    --stdio-arg=--entrypoint --stdio-arg=/app/.venv/bin/airflow-mcp \
    --stdio-arg="$image" --stdio-arg=--transport --stdio-arg=stdio \
    --stderr-file "$stderr_log" >"$report"; then
    echo "Cisco MCP Scanner failed for the $surface surface" >&2
    result=1
    return
  fi

  if ! jq -e --argjson expected "$expected_count" '
    .requested_analyzers == ["yara"] and
    (.scan_results | length == $expected) and
    ([.scan_results[].tool_name] | length == (unique | length)) and
    all(
      .scan_results[];
      .status == "completed" and
      .is_safe == true and
      .findings.yara_analyzer.severity == "SAFE" and
      .findings.yara_analyzer.total_findings == 0
    )
  ' "$report" >/dev/null; then
    echo "untriaged or incomplete Cisco MCP Scanner result for the $surface surface" >&2
    result=1
  fi
}

scan_surface "read-only" "true" 11
scan_surface "write-enabled" "false" 16

read_report="$report_dir/mcp-scan-read-only.json"
write_report="$report_dir/mcp-scan-write-enabled.json"
if [[ -s "$read_report" && -s "$write_report" ]]; then
  if ! jq -e -n --slurpfile read "$read_report" --slurpfile write "$write_report" '
    (([$read[0].scan_results[].tool_name] - [$write[0].scan_results[].tool_name]) | length) == 0 and
    (([$write[0].scan_results[].tool_name] - [$read[0].scan_results[].tool_name]) | sort) == [
      "airflow_clear_dag_run",
      "airflow_clear_task_instances",
      "airflow_pause_dag",
      "airflow_trigger_dag",
      "airflow_unpause_dag"
    ]
  ' >/dev/null; then
    echo "read-only and write-enabled tool surfaces differ from the enforced contract" >&2
    result=1
  fi
else
  result=1
fi

image_id="$(docker image inspect "$image" --format '{{.Id}}')"
commit="${GITHUB_SHA:-$(git rev-parse HEAD 2>/dev/null || echo unknown)}"
jq -n \
  --arg commit "$commit" \
  --arg image "$image" \
  --arg image_id "$image_id" \
  --arg pip_audit_version "$PIP_AUDIT_VERSION" \
  --arg mcp_scanner_version "$MCP_SCANNER_VERSION" \
  '{
    commit: $commit,
    image: $image,
    image_id: $image_id,
    pip_audit_version: $pip_audit_version,
    mcp_scanner_version: $mcp_scanner_version,
    mcp_analyzers: ["yara"],
    surfaces: ["read-only", "write-enabled"]
  }' >"$report_dir/scan-metadata.json"

exit "$result"
