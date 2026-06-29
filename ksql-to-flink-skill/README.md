# ksqlDB to Flink SQL migration skill

Portable agent skill for migrating Confluent ksqlDB scripts to Confluent Cloud for Flink SQL. Migration rules packaged for Claude Code, Cursor, and other `SKILL.md` runtimes.

See [SPEC.md](SPEC.md).

## Approach

1. **Skills** — agent playbooks loaded in Cursor/Claude:
   - `ksql-to-flink` — translation rules (`skill/SKILL.md` + `skill/references/`)
   - `validate-flink-sql` — DDL/DML normalization ([`../flink-skill-common/skill/`](../flink-skill-common/skill/))
2. **MCP tools** — validate and deploy via [`flink-skill-common` MCP server](../flink-skill-common/README.md#mcp-server-cursor-ide) (repo [`.cursor/mcp.json`](../.cursor/mcp.json))
3. **Harness CLI** — regression and integration tests only (`harness/`); shared Python utilities in [`../flink-skill-common/`](../flink-skill-common/)


## Harness CLI

Use the CLI for automated regression and integration tests using local LLM:

```bash
cp .env.example .env   # at repo root
cd ksql-to-flink-skill/harness
uv sync --extra dev
```

| Variable | Default |
|----------|---------|
| `SL_LLM_BASE_URL` | `http://localhost:7999/v1` |
| `SL_LLM_MODEL` | `Qwen3.6-27B-4bit` |
| `FLINK_DEPLOY_TIMEOUT_SECONDS` | `300` |
| `FLINK_API_KEY` / `FLINK_API_SECRET` | (required for deploy) |
| `AGENT_FIXER_EXECUTION_ENABLED` | `0` (set `1` for agent retry on failure) |

```bash
# Integration tests (live LLM)
uv run pytest tests/it

# Golden migrate
uv run ksql-flink-migrate \
  --table dim_all_songs \
  --file path/to/merge.ksql \
  --out-dir staging/ksql

# Translate only
uv run ksql-flink-migrate --table dim_all_songs --file path/to.ksql --out-dir output/ --skip-deploy
```

Deploy requires Flink API credentials in the repo-root `.env`. See [docs/FLINK_DEPLOY.md](docs/FLINK_DEPLOY.md).

## Deploy skill — Claude Code

```bash
mkdir -p ~/.claude/skills/ksql-to-flink ~/.claude/skills/validate-flink-sql
cp -r skill/* ~/.claude/skills/ksql-to-flink/
cp -r ../flink-skill-common/skill/* ~/.claude/skills/validate-flink-sql/
```

Project copy: `.claude/skills/ksql-to-flink/SKILL.md`

## Cursor quick start (primary workflow)

TBC


### 1. Environment

```bash
cp .env.example .env   # repo root — LLM + Flink credentials
cd flink-skill-common/harness && uv sync --extra dev
```

Enable **Cursor Settings → MCP** for the `flink-skill-common` server (configured in [`.cursor/mcp.json`](../.cursor/mcp.json)). Credentials load from repo-root `.env` via `DOTENV_FILE=.env`.

### 2. Install skills

Copy **both** skills into your Cursor skills directory:

```bash
mkdir -p ~/.cursor/skills/ksql-to-flink ~/.cursor/skills/validate-flink-sql
cp -r skill/* ~/.cursor/skills/ksql-to-flink/
cp -r ../flink-skill-common/skill/* ~/.cursor/skills/validate-flink-sql/
```

| Skill | Role |
|-------|------|
| `ksql-to-flink` | Translate ksqlDB → Flink DDL/DML |
| `validate-flink-sql` | Fix connector properties, PRIMARY KEY, DISTRIBUTED BY after translation |

### 3. Migrate in the IDE

Example prompt:

```
Migrate this ksql CREATE STREAM to Flink SQL for table kma_chat.
After translation, validate with MCP validate_flink_sql_offline and deploy with flink-skill-common MCP tools.
```

Workflow:

1. Agent loads `ksql-to-flink` skill → translates to DDL/DML
2. MCP `validate_flink_sql_offline` → on errors, apply `validate-flink-sql` skill
3. Optional MCP `validate_flink_sql_remote` (requires Flink credentials)
4. MCP deploy sequence: `create_flink_statement` → `wait_flink_statement_phase` → … (see [skill/references/confluent-sql-deploy.md](skill/references/confluent-sql-deploy.md))


## Golden pairs

Source for KSQL are under `references/sources`

TBC


| KSQL | Flink golden | Table |
|------|--------------|-------|
| `routing/filter.ksql` | `flink_ref/dimensions/songs/all_song/` | `dim_all_songs` |
| `routing/merge.ksql` | `flink_ref/dimensions/songs/all_song/` | `dim_all_songs` |
| `joins/stream_stream.ksql` | `flink_ref/joins/shipped_orders/` | `shipped_orders` |
| `routing/splitting.ksql` | `flink_ref/.../acting_events_drama/` | `dim_acting_events_drama` |


## Tests

* unit tests
```sh
# No LLM required
uv run pytest -v tests/ut -m "not integration"
```

* integration tests with local LLM
```sh
uv run pytest -vs tests/it
```

