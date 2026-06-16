# Flink deploy setup for ksql-to-flink

The harness deploys translated Flink SQL to Confluent Cloud using the [confluent-sql](https://pypi.org/project/confluent-sql/) Python library (REST API, no Node.js or MCP server).

## Prerequisites

- Confluent Cloud account with Flink compute pool
- Flink regional API key and secret

## Configuration

1. Copy environment template:

```bash
cd harness
cp .env.example .env
# edit .env with your CC credentials
```

2. Required variables in `harness/.env`:

| Variable | Description |
|----------|-------------|
| `FLINK_API_KEY` | Flink regional API key |
| `FLINK_API_SECRET` | Flink regional API secret |
| `FLINK_ORG_ID` | Organization ID |
| `FLINK_ENV_ID` | Environment ID |
| `FLINK_COMPUTE_POOL_ID` | Compute pool ID (`lfcp-...`) |
| `FLINK_DATABASE_NAME` | Flink SQL database name |

3. Optional:

| Variable | Default | Description |
|----------|---------|-------------|
| `FLINK_REST_ENDPOINT` | inferred from cloud/region | Flink SQL REST base URL |
| `CLOUD_PROVIDER` | `aws` | Used when endpoint not set |
| `CLOUD_REGION` | `us-west-2` | Used when endpoint not set |
| `FLINK_DEPLOY_POLL_SECONDS` | `5` | Poll interval (legacy alias: `MCP_DEPLOY_POLL_SECONDS`) |
| `FLINK_DEPLOY_TIMEOUT_SECONDS` | `300` | Deploy timeout (legacy alias: `MCP_DEPLOY_TIMEOUT_SECONDS`) |

## Verify settings

```bash
cd harness
uv sync --extra dev
uv run python -c "from ksql_flink_skill.deploy import require_flink_deploy_ready; print(require_flink_deploy_ready())"
```

## Harness CLI

```bash
uv run ksql-flink-migrate --table dim_all_songs --file path/to.ksql --out-dir output/
```

Flags:

- `--skip-deploy` — translate only, do not deploy
- `--agent-deploy-on-failure` — invoke Agno agent with confluent-sql tools to fix and redeploy (max 2 retries)

## Agno deploy tools

The harness exposes these tools via `FlinkStatementAgnoTools` in `flink-skill-common`:

- `create_flink_statement`
- `get_flink_statement`
- `list_flink_statements`
- `delete_flink_statement`
- `get_flink_statement_exceptions`
- `wait_flink_statement_phase`
- `check_flink_statement_health`

## Troubleshooting

- Missing credentials — run preflight check above; fill all required `FLINK_*` vars
- Statement already exists — harness deletes and retries on HTTP 409 automatically
- Post-deploy issues — use `flink-statement-troubleshooting` skill
