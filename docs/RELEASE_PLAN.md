# Release Plan: Open Source + Hackathon Submission

**Created:** 2025-11-29
**Deadline:** November 30, 2025, 11:59 PM UTC (Hackathon)
**Target GitHub repo:** `madamak/apache-airflow-mcp-server`

---

## Project Overview

Apache Airflow MCP Server - A Model Context Protocol server that connects AI assistants (Claude, Cursor) to Apache Airflow instances. Built with FastMCP.

**Key capabilities:**
- Multi-instance support (connect to multiple Airflow deployments)
- Read operations: list DAGs, runs, task instances, logs
- Write operations: trigger DAGs, clear tasks, pause/unpause
- URL resolution: parse Airflow UI URLs to extract context
- Structured JSON responses with request_id traceability

---

## Current State (as of Nov 29)

### Completed
- [x] Code sanitization (removed internal company references)
- [x] Instance naming: `data-stg`, `ml-stg`, `local` (generic team-based names)
- [x] LICENSE: Apache 2.0, copyright "Adam Makhlouf"
- [x] .gitignore updated to exclude `.claude/`
- [x] CONTRIBUTING.md added
- [x] pyproject.toml updated with author/URLs
- [x] All 79 tests passing
- [x] Hackathon registration completed on HuggingFace

### Pending
- [ ] Phase 1: Tiered configuration system
- [ ] Phase 2: Squash git history
- [ ] Phase 3: HuggingFace Space creation
- [ ] Phase 4: Demo video + social post + submit

> These checklist items capture future improvements; the current open-source drop can ship without them.

---

## Key Decisions Made

### 1. Instance Naming Convention
**Decision:** Use generic team-based names
- `data-stg` - Data team staging
- `ml-stg` - ML team staging
- `local` - Local development

**Rationale:** Concise, follows `-stg` industry convention, realistic but generic.

### 2. Instance Registry Configuration (YAML-only, with simple + advanced patterns)
**Decision (updated):** Use a **single configuration mechanism** – a YAML registry file referenced by `AIRFLOW_MCP_INSTANCES_FILE`. “Simple” vs “advanced” refers to how many instances you define in YAML, not to a separate env-only mode.

| Tier   | Method                           | Use Case                     |
|--------|----------------------------------|------------------------------|
| Simple | YAML file with a single instance | One Airflow, quick setup     |
| Advanced | YAML file with multiple instances | Multi-instance deployments |

**Configuration entrypoint:**
```bash
export AIRFLOW_MCP_INSTANCES_FILE=/path/to/instances.yaml
```

**Simple YAML example (single instance):**
```yaml
default:
  host: https://airflow.example.com/
  api_version: v1
  verify_ssl: true
  auth:
    type: basic
    username: ${AIRFLOW_INSTANCE_DEFAULT_USERNAME}
    password: ${AIRFLOW_INSTANCE_DEFAULT_PASSWORD}
```

**Why this changed:** The earlier plan added a second, env-only configuration path (`AIRFLOW_HOST`, `AIRFLOW_USERNAME`, etc.) and a precedence layer between YAML and envs. That increased complexity (extra code path, naming and precedence questions) right before a tight hackathon deadline. The updated plan keeps **one code path (YAML)** and makes the “simple” path just a minimal YAML file with one instance, which still reads credentials from env vars via `${VAR}` substitution.

### 3. Auth Methods
**Decision:** Support both basic auth and bearer tokens, but treat bearer as **experimental** for the hackathon release.

| Method | YAML `auth.type` | Env Var Examples (used via `${VAR}` in YAML)             | Status         |
|--------|------------------|----------------------------------------------------------|----------------|
| Basic  | `basic`          | `AIRFLOW_INSTANCE_DEFAULT_USERNAME`, `..._PASSWORD`     | **Stable**     |
| Bearer | `bearer`         | `AIRFLOW_INSTANCE_ML_PROD_TOKEN`                         | **Experimental** |

**Notes:**
- The primary, well-tested path is **basic auth**, and that is what we will use in the demo video and examples.
- Bearer token support will be implemented and covered by a small set of focused tests but may not be fully validated across all Airflow versions yet.

### 4. HuggingFace Space Strategy
**Decision:** Separate branch, not separate repo; use a **Gradio Space** as a documentation/demo front-end, not as the primary MCP runtime.

```
main branch → GitHub (pure MCP server)
huggingface-space branch → HF Space (with Gradio UI)
```

**Rationale:** 
- Avoid maintaining two repos. Gradio adds dependencies not needed for core MCP; keeping them on a separate branch keeps `main` lean.
- The hackathon organizers require hosting on HF Spaces as a **Gradio or Docker** space; Gradio is the lighter-weight choice for a tool-explorer style UI.
- The Space will run only the `app.py` Gradio UI (tool explorer, docs, embedded video). The **full MCP server code** lives in the same repo/branch, but the server is intended to be installed and run locally by users (as shown in the demo), not as a live MCP endpoint inside the Space.

---

## Phase 1: Instance Registry & Auth Improvements (YAML-only)

### Files to Modify

#### 1.1 `src/airflow_mcp/config.py`
- **No new env-only settings.** We continue to use:
  - `instances_file` (backed by `AIRFLOW_MCP_INSTANCES_FILE`)
  - `default_instance` (backed by `AIRFLOW_MCP_DEFAULT_INSTANCE`)
- Optional: clarify docstrings to emphasize that YAML is the sole instance configuration source.

#### 1.2 `src/airflow_mcp/registry.py`
- Keep `load_registry_from_yaml()` as the only registry loader.
- Extend `InstanceConfig`/auth models to support both:
  - `auth.type == "basic"` with `username`/`password`
  - `auth.type == "bearer"` with `token`
- Ensure `default_instance` behavior is documented:
  - Single-instance simple pattern: recommend using `default` as the key, or set `AIRFLOW_MCP_DEFAULT_INSTANCE` explicitly.

#### 1.3 `src/airflow_mcp/client_factory.py`
- Add bearer token auth support:
  - When `auth.type == "basic"`, keep existing username/password behavior.
  - When `auth.type == "bearer"`, configure `Authorization: Bearer <token>` (implementation may depend on Airflow client version; document any quirks).

#### 1.4 `examples/instances.yaml`
Add bearer token example:
```yaml
# Bearer token auth example
ml-prod:
  host: https://airflow.ml-prod.example.com/
  api_version: v1
  verify_ssl: true
  auth:
    type: bearer
    token: ${AIRFLOW_INSTANCE_ML_PROD_TOKEN}
```

#### 1.5 Tests to Add
- `test_single_instance_yaml_basic_auth` - minimal one-instance registry
- `test_multi_instance_yaml_bearer_token` - bearer auth in YAML format
- `test_missing_instances_file_error` - helpful error when `AIRFLOW_MCP_INSTANCES_FILE` is not set
- `test_bearer_token_yaml` - bearer auth in YAML format

#### 1.6 `README.md`
Restructure to lead with YAML-based simple mode:
```markdown
## Quickstart

### Step 1: Point to your instance registry
export AIRFLOW_MCP_INSTANCES_FILE=/path/to/instances.yaml

### Step 2: Create a simple one-instance registry
default:
  host: https://airflow.example.com/
  api_version: v1
  verify_ssl: true
  auth:
    type: basic
    username: ${AIRFLOW_INSTANCE_DEFAULT_USERNAME}
    password: ${AIRFLOW_INSTANCE_DEFAULT_PASSWORD}
```

---

## Phase 2: Prepare for Public Release

### Squash Git History
Current history contains internal references ("Initial copy from internal monorepo"). Squash to clean commit:

```bash
git checkout --orphan clean-main
git add -A
git commit -m "Initial commit - Apache Airflow MCP Server"
git branch -D main
git branch -m main
```

### Files to Delete Before Public
- `docs/OPEN_SOURCE_PREPARATION.md` (internal tracking document)

### Files to Keep
- `docs/DESIGN_TIERED_CONFIGURATION.md` (useful for contributors)
- `docs/RELEASE_PLAN.md` (can delete after release, or keep for history)

---

## Phase 3: HuggingFace Space (Gradio demo + landing page)

### Branch Workflow
```bash
# After Phase 2 (clean main exists)
git checkout -b huggingface-space

# Add Gradio files...
git add .
git commit -m "Add HuggingFace Space with Gradio UI"

# Push to HuggingFace
git remote add space https://huggingface.co/spaces/MCP-1st-Birthday/apache-airflow-mcp
git push space huggingface-space:main

# Clean up
git checkout main
git remote remove space
# Keep huggingface-space branch locally for future updates
```

### Files to Create on HF Branch

#### `app.py` (Gradio Tool Explorer / Landing Page)
Simple UI that displays:
- List of all MCP tools with descriptions
- Example request/response JSON for each tool (based on local runs against Airflow)
- Link to GitHub for installation and configuration instructions
- Embedded demo video

This app is intentionally **documentation- and demo-focused**. It does not
spin up a live Airflow instance or expose the MCP server as a remote endpoint
from HF; instead, it explains how to install and run the MCP server locally.

#### `requirements.txt`
```
gradio
```

#### `README.md` Modifications
Add YAML frontmatter at top:
```yaml
---
title: Apache Airflow MCP Server
emoji: 🌬️
colorFrom: blue
colorTo: cyan
sdk: gradio
sdk_version: "5.0"
app_file: app.py
tags:
  - mcp
  - airflow
  - mcp-server
  - building-mcp-track-xx
pinned: false
---
```

---

## Phase 4: Hackathon Submission

### Requirements (Track 1 - Building MCP Servers)
1. **HuggingFace Space** in MCP-1st-Birthday organization
2. **Demo video** (1-5 minutes) showing integration with Claude Desktop/Cursor
3. **Social media post** (X or LinkedIn) about the project
4. **Documentation** explaining purpose and capabilities

### Demo Video Plan
- Record screen using Cursor IDE with the MCP server connected
- Show workflow: resolve URL → list dag runs → get task instance → view logs
- Use real Airflow instance but have the agent redact company-specific details
- Alternative: Use local Airflow standalone instance

### Video Content Outline (3-5 min)
1. Intro: What is the Airflow MCP Server (30s)
2. Setup: Show simple YAML-based configuration (instances file + env vars for credentials) (30s)
3. Demo: Investigate a failed DAG run (2-3 min)
   - Paste Airflow UI URL
   - Agent resolves and lists recent runs
   - Find failed task, get logs
   - Show log filtering (error level)
4. Outro: Link to GitHub, how to install (30s)

### Submission Checklist
- [ ] Space URL: `https://huggingface.co/spaces/MCP-1st-Birthday/apache-airflow-mcp`
- [ ] Demo video uploaded/embedded
- [ ] Social post published with link
- [ ] README has correct track tag
- [ ] All team member HF usernames in README

---

## Technical Context

### Project Structure
```
src/airflow_mcp/
├── server.py          # FastMCP entrypoint, tool registration
├── tools.py           # Business logic for MCP tools
├── registry.py        # Instance registry (YAML loader, cache)
├── client_factory.py  # Airflow API client builder
├── url_utils.py       # URL resolver/builder
├── validation.py      # Input validation, SSRF guard
├── observability.py   # Structured logging
├── errors.py          # Error types, handle_errors decorator
└── config.py          # Pydantic settings
```

### Key Patterns
- **Stateless tools**: Each call resolves instance independently
- **Structured errors**: `ToolError` with `{code, message, request_id, context}`
- **URL precedence**: `ui_url` and `instance` must agree if both provided
- **Client caching**: One API client per instance, cached in factory

### Running Locally
```bash
# Install
uv sync

# Run tests
uv run pytest

# Run server (stdio)
uv run airflow-mcp --transport stdio

# Run server (HTTP)
uv run airflow-mcp --transport http --host 127.0.0.1 --port 8765
```

---

## Timeline

| When | Task |
|------|------|
| Nov 29 (today) | Phase 1: Implement tiered config |
| Nov 29 | Phase 2: Squash history |
| Nov 29 | Phase 3: Create HF Space branch, push |
| Nov 29-30 | Phase 4: Record video, social post |
| Nov 30 before 11:59 PM UTC | Submit to hackathon |

---

## References

- [Airflow API Auth (v2)](https://airflow.apache.org/docs/apache-airflow/stable/security/api.html)
- [MCP 1st Birthday Hackathon](https://huggingface.co/MCP-1st-Birthday)
- [HuggingFace Spaces MCP Docs](https://huggingface.co/docs/hub/spaces-mcp-servers)
- [FastMCP Documentation](https://gofastmcp.com)
