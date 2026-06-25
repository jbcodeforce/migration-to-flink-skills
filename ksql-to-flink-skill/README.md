# ksqlDB to Flink SQL migration skill

Portable agent skill for migrating Confluent ksqlDB scripts to Confluent Cloud for Flink SQL. Migration rules packaged for Claude Code, Cursor, and other `SKILL.md` runtimes.

See [SPEC.md](SPEC.md).

## Approach

1. **Skill** (`skill/SKILL.md` + `skill/references/`) — agent playbook for ksqlDB → Flink translation. Can be used with Claude Code, Cursor 
2. **Harness** (`harness/`) — Solution based on Agno agent with `LocalSkills` loading the same skill, calling oMLX via `OpenAIChat` API. Shared Python utilities live in [`../flink-skill-common/`](../flink-skill-common/) (`compare`, `output`, `llm`, `deploy`, etc.).
3. **Text fixtures** — ksql inputs in the [./references/ksql/sources/](./references/ksql/sources), with goldens refernces in `flink_ref/` folder.
4. a Shell script to run the migration as a front end for the cli.

The Harness steps are: 
* clean source ksql (deterministic)
* agent migration via skill (LLM)
* parse DDL/DML
* write files
* generate source stub DDLs in `tests/` when DML references missing tables
* deploy source DDLs, target DDL, then DML to Confluent Cloud Flink via [confluent-sql](https://pypi.org/project/confluent-sql/).

Deploy requires Flink API credentials in the repo-root `.env` (or `DOTENV_FILE`). See [docs/FLINK_DEPLOY.md](docs/FLINK_DEPLOY.md).


## Quick start

* Prepare the python code for the ksql-to-flink cli:
```bash
cp .env.example .env   # at repo root
cd ksql-to-flink-skill/harness
uv sync --extra dev
```

* Set environment variables to access LLM model in the repo-root `.env`
| Variable | Default |
|----------|---------|
| `SL_LLM_BASE_URL` | `http://localhost:7999/v1` |
| `SL_LLM_MODEL` | `Qwen3.6-27B-4bit` |
| `KSQL_TUTORIAL_ROOT` | `../../../flink_project_demos/ksql_tutorial` |
| `FLINK_DEPLOY_TIMEOUT_SECONDS` | `300` |
| `FLINK_API_KEY` / `FLINK_API_SECRET` | (required for deploy) |
| `KSQL_FLINK_AGENT_DEPLOY` | `0` (set `1` to enable agent retry on failure) |

* Validate integration tests

```sh
# Need a live LLM (oMLX) - will run references
uv run pytest tests/it
# run one reference
uv run pytest tests/it/test_cli_migrate.py::test_migrate_filtering -vs
```

### Migrate one file

```bash
uv run ksql-flink-migrate \
  --table dim_all_songs \
  --file ../../../flink_project_demos/ksql_tutorial/sources/routing/merge.ksql \
  --out-dir output/
```

Deploy runs by default after writing SQL files. Translate only:

```bash
uv run ksql-flink-migrate --table dim_all_songs --file path/to.ksql --out-dir output/ --skip-deploy
```

On deploy failure, optional agent retry with confluent-sql tools:

```bash
uv run ksql-flink-migrate ... --agent-deploy-on-failure
```

### Agno agent

```bash
uv run ksql-flink-agent "Migrate merge.ksql to Flink SQL for dim_all_songs"
```

## Deploy skill — Cursor

```bash
mkdir -p ~/.cursor/skills/ksql-to-flink
cp -r skill/* ~/.cursor/skills/ksql-to-flink/
```

In Cursor IDE use `/ksql_to_flink`. Example of direct prompt:

```
using /ksql_to_flink translate this @KMA-CHAT.sql to the same table name in the flink folder under @demo/flink
```


## Deploy skill — Claude Code

```bash
mkdir -p ~/.claude/skills/ksql-to-flink
cp -r skill/* ~/.claude/skills/ksql-to-flink/
```

Project copy: `.claude/skills/ksql-to-flink/SKILL.md`

## Golden pairs

| KSQL | Flink golden | Table |
|------|--------------|-------|
| `sources/routing/merge.ksql` | `flink_ref/dimensions/songs/all_song/` | `dim_all_songs` |
| `sources/joins/stream_stream.ksql` | `flink_ref/joins/shipped_orders/` | `shipped_orders` |
| `sources/routing/splitting.ksql` | `flink_ref/.../acting_events_drama/` | `dim_acting_events_drama` |

See [assets/FIXTURES.md](assets/FIXTURES.md).

## When to use skill vs shift_left CLI

| Use skill | Use `shift_left table migrate --source-type ksql` |
|-----------|---------------------------------------------------|
| Agent session in Cursor/Claude | Production staging + deploy |
| Local oMLX testing | CC validation + refinement |
| No shift_left config | Full pipeline folder structure |

## Tests

* Unit tests do mock the LLM backend.
  ```sh
  # No LLM required
  uv run pytest tests/ut -m "not integration"
  ```


| File | LLM | Purpose |
|------|-----|---------|
| `test_pipeline_offline.py` | No | clean, split CREATE STREAM |
| `test_fixtures.py` | No | paths exist |
| `test_compare.py` | No | compare utility |
| `test_agent_skills.py` | No | LocalSkills loads ksql-to-flink skill |
| `test_output.py` | No | DDL/DML response parsing |
| `test_skill_patterns.py` | No | CTE + GROUP BY pattern in skill docs |
| `test_skill_deploy_docs.py` | No | confluent-sql deploy docs in skill |
| `test_deploy_statements.py` | No | statement naming and deploy order |
| `test_flink_deploy_preflight.py` | No | Flink deploy preflight errors |
| `test_cli_migrate.py` | No | `--skip-deploy` behavior |
| `test_confluent_sql_deploy_integration.py` | Live (`integration`) | live confluent-sql deploy to CC |
| `test_ksql_golden.py` | Live (`integration`) | live agent migration vs golden |

