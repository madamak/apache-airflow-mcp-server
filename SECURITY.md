# Security Policy

## Supported Versions

Security fixes are applied to the latest released version.

## Reporting a Vulnerability

Please **do not** open a public issue for security vulnerabilities.

Instead, use [GitHub private vulnerability reporting](https://github.com/madamak/apache-airflow-mcp-server/security/advisories/new)
for this repository. You should receive an acknowledgement within a few days.

## Security model

Useful context when assessing this server:

- **Credentials** are supplied via environment variables or a registry YAML
  (with `${VAR}` env substitution). They are never logged, and discovery tools
  redact auth details to the auth *type* only.
- **SSRF guard**: tools that accept an Airflow UI URL resolve its hostname
  against the configured instance registry and reject unknown hosts.
- **Write tools** (`trigger`, `clear`, `pause`/`unpause`) are annotated with
  MCP `destructiveHint`; that hint is advisory — clients that honor it prompt
  before execution, others may not. The enforced server-side protection is
  `AIRFLOW_MCP_READ_ONLY=true`, which removes write tools from the tool
  surface entirely.
- **Transport**: the HTTP transport binds to `127.0.0.1` by default and has no
  built-in authentication — put it behind an authenticating proxy before
  exposing it beyond localhost.
