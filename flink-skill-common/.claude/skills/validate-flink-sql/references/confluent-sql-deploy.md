# Deploy to Confluent Cloud Flink via confluent-sql

After translation, deploy source stub DDLs from `tests/`, then target DDL, then DML using the [confluent-sql](https://pypi.org/project/confluent-sql/) Python driver (REST API).

## Prerequisites

- Confluent Cloud account with Flink compute pool
- Flink regional API key and secret in the repo-root `.env` (or `DOTENV_FILE`)
- Python harness with `flink-skill-common` (includes `confluent-sql`)

## Output layout

```
out-dir/
  ddl.{target}.sql
  dml.{target}.sql
  tests/
    ddl.{source}.sql   # stub for each DML dependency not in target DDL
```

Source stubs are LLM-generated to match columns used in DML and the original source SQL.

## Statement naming

Flink statement names must match `[a-z0-9]([-a-z0-9]*[a-z0-9])?`. Normalize table names: replace `_` with `-`.

| Artifact | Statement name |
|----------|----------------|
| Source stub DDL | `{source-normalized}-ddl` |
| Target DDL | `{target-normalized}-ddl` |
| Target DML | `{target-normalized}-dml` |

Example: source `kma_chat_st` → `kma-chat-st-ddl`; target `kma_chat` → `kma-chat-ddl`, `kma-chat-dml`.

## Agno / MCP tool sequence (Cursor IDE)

In Cursor, call the **`flink-skill-common` MCP server** tools (same names as the Agno deploy fixer). Preflight: repo-root `.env` with Flink credentials (`DOTENV_FILE=.env` in `.cursor/mcp.json`).

Before deploy, run `validate_flink_sql_offline` on extracted DDL/DML; optionally `validate_flink_sql_remote` when credentials are configured.

| Step | MCP tool | Notes |
|------|----------|-------|
| Preflight | (env) | `FLINK_API_KEY`, `FLINK_API_SECRET`, pool IDs in repo `.env` |
| Offline validate | `validate_flink_sql_offline` | sqlglot; fix with `validate-flink-sql` skill |
| Remote validate | `validate_flink_sql_remote` | CC Flink parser (optional before deploy) |
| Deploy source DDL | `create_flink_statement` | For each `tests/ddl.*.sql`, sorted by table name |
| Poll source DDL | `wait_flink_statement_phase` | Until RUNNING, COMPLETED, or APPLIED |
| Deploy target DDL | `create_flink_statement` | After all source DDLs succeed |
| Poll target DDL | `wait_flink_statement_phase` | Until RUNNING, COMPLETED, or APPLIED |
| Deploy target DML | `create_flink_statement` | After target DDL succeeds |
| Poll target DML | `wait_flink_statement_phase` | Until RUNNING or FAILED |
| Verify | `check_flink_statement_health` | On DML statement when available |
| On failure | `get_flink_statement_exceptions` | Apply `validate-flink-sql` skill, then redeploy |

Deploy order is strict: source DDLs → target DDL → target DML.

## Agno harness tool sequence

When using the Python harness Agno deploy fixer (not Cursor MCP), the same tool names apply via `FlinkStatementLLMTools`:

| Step | Tool | Notes |
|------|------|-------|
| Preflight | (harness) `require_flink_deploy_ready()` | Validates env credentials |
| Offline validate | `validate_offline.py` script or `flink-skill-validate offline` CLI | sqlglot check before deploy |
| Remote validate | `validate_remote.py` script or `flink-skill-validate remote` CLI | CC Flink parser (optional) |
| Deploy source DDL | `create_flink_statement` | For each `tests/ddl.*.sql`, sorted by table name |
| Poll source DDL | `wait_flink_statement_phase` | Until RUNNING, COMPLETED, or APPLIED |
| Deploy target DDL | `create_flink_statement` | After all source DDLs succeed |
| Poll target DDL | `wait_flink_statement_phase` | Until RUNNING, COMPLETED, or APPLIED |
| Deploy target DML | `create_flink_statement` | After target DDL succeeds |
| Poll target DML | `wait_flink_statement_phase` | Until RUNNING or FAILED |
| Verify | `check_flink_statement_health` | On DML statement when available |
| On failure | `get_flink_statement_exceptions` | Feed agent retry loop |

Deploy order is strict: source DDLs → target DDL → target DML.

## create_flink_statement parameters

| Parameter | Source |
|-----------|--------|
| `statement_name` | `{table-normalized}-ddl` or `-dml` |
| `sql` | Full SQL text from `tests/ddl.{source}.sql`, `ddl.{table}.sql`, or `dml.{table}.sql` |

Credentials and pool settings come from environment variables.

## Agent deploy fixer

When `AGENT_FIXER_EXECUTION_ENABLED=1`, the harness convergence loop invokes an Agno agent with confluent-sql tools to fix SQL and redeploy (max retries from `AGENT_FIXER_EXECUTION_MAX_RETRIES`). The agent deploys source DDLs before target DDL/DML.

## Post-deploy triage

Use [flink-statement-triage](https://github.com/jerome/research/tree/main/flink-statement-troubleshooting) for metrics and issue detection.
