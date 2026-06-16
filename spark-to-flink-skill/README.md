# Spark SQL to Flink SQL migration skill

Portable agent skill for migrating Apache Spark SQL to Confluent Cloud for Flink SQL. Encodes the same rules and pipeline as `shift_left_utils` `SparkToFlinkSqlAgent`, packaged for Claude Code, Cursor, and other `SKILL.md` runtimes.

See [SPEC.md](SPEC.md) for full specification.

## Approach

1. **Skill** (`skill/SKILL.md` + `skill/references/`) — agent playbook with translation and validation rules.
2. **Harness** (`harness/`) — Agno agent with `LocalSkills` loading the same skill, calling oMLX via `OpenAIChat`. Shared Python utilities live in [`../flink-skill-common/`](../flink-skill-common/) (`compare`, `output`, `llm`, etc.).
3. **Golden references** — Spark inputs in `flink_project_demos/customer_360/c360_spark_processing/`, Flink outputs in `c360_flink_processing/pipelines/**/sql-scripts/`.

Harness steps: clean → detect CREATE statements (deterministic) → agent migration via skill (LLM) → parse DDL/DML → write files.

## Quick start (harness)

```bash
cd harness
cp .env.example .env
uv sync --extra dev

# Offline tests (no LLM)
uv run pytest tests/ -m "not integration"

# All tests including live LLM (requires oMLX)
uv run pytest tests/
```

### Migrate one file (live LLM)

Start oMLX or any OpenAI-compatible server, then:

```bash
uv run spark-flink-migrate \
  --table src_c360_customers \
  --file ../../../flink_project_demos/customer_360/c360_spark_processing/sources/src_customers.sql \
  --out-dir output/
```

### Agno agent smoke test

```bash
uv run spark-flink-agent "Migrate src_customers.sql to Flink SQL for table src_c360_customers"
```

## Deploy skill — Cursor

**Personal (all projects):**

```bash
mkdir -p ~/.cursor/skills/spark-to-flink
cp -r skill/* ~/.cursor/skills/spark-to-flink/
```

**Project-local:**

```bash
mkdir -p .cursor/skills/spark-to-flink
cp -r skill/* .cursor/skills/spark-to-flink/
```

The skill auto-invokes when you ask to "migrate Spark SQL to Flink SQL" (trigger terms in frontmatter `description`).

## Deploy skill — Claude Code

**Personal:**

```bash
mkdir -p ~/.claude/skills/spark-to-flink
cp -r skill/* ~/.claude/skills/spark-to-flink/
```

**Project:**

```bash
mkdir -p .claude/skills/spark-to-flink
cp -r skill/* .claude/skills/spark-to-flink/
```

Reference the skill in prompts: "Use the spark-to-flink skill to migrate this Spark SQL."

## When to use skill vs shift_left CLI

| Use skill | Use `shift_left table migrate` |
|-----------|-------------------------------|
| Interactive agent session in Cursor/Claude | Production pipeline deployment |
| Learning/exploring migration rules | Writing to shift_left staging folder structure |
| Isolated test with local oMLX | CC live validation + refinement loop |
| No shift_left config required | Full inventory, metadata, deploy integration |

## Prompt maintenance

After editing prompts in `shift_left_utils`:

```bash
./scripts/sync-prompts.sh
```

## Fixtures

See [assets/FIXTURES.md](assets/FIXTURES.md) for golden pair paths.

## Tests

| Test file | LLM required | Purpose |
|-----------|--------------|---------|
| `test_pipeline_offline.py` | No | clean, split, detect |
| `test_fixtures.py` | No | c360 paths exist |
| `test_compare.py` | No | compare utility |
| `test_agent_skills.py` | No | LocalSkills loads spark-to-flink skill |
| `test_output.py` | No | DDL/DML response parsing |
| `test_c360_golden.py` | Yes (`integration`) | live agent migration vs golden |

## Environment

| Variable | Default |
|----------|---------|
| `SL_LLM_BASE_URL` | `http://localhost:1337/v1` |
| `SL_LLM_MODEL` | Must match an id from `GET $SL_LLM_BASE_URL/models` (see `.env.example`) |
| `SL_LLM_API_KEY` | `no_llm_key` |

### oMLX troubleshooting

- List models: `curl -s $SL_LLM_BASE_URL/models | jq '.data[].id'`
- `SL_LLM_MODEL` must use the exact id (e.g. `Qwen3.6-27B-4bit`, not `Qwen3.6:27b-4bit`). The CLI resolves common typos when possible.
- `Qwen3.6-27B-4bit` has a 4096-token context window and is rejected for migrations. Use `Qwen3.6-35B-A3B-UD-MLX-4bit` instead.
- If migration fails, the CLI exits non-zero instead of writing empty `ddl.*.sql` / `dml.*.sql` files.

## Out of scope (v1)

- Confluent Cloud validation tools (later phase)
- PySpark → Catalyst migration (phase 2)
