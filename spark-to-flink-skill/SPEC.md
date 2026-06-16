# Spark SQL → Flink SQL migration skill — specification

## Intent

Provide a portable agent skill (`SKILL.md`) that migrates Apache Spark SQL to Confluent Cloud for Flink SQL, using the same rules and multi-step pipeline as `shift_left_utils`. Agents invoke it on phrases like "migrate this Spark SQL to Flink SQL" without loading the full shift_left CLI.

## Resolved decisions

| Decision | Choice |
|----------|--------|
| Repository home | `research/spark-to-flink-skill/` |
| Golden references | `flink_project_demos/customer_360/c360_flink_processing/` (Flink output); Spark inputs in `c360_spark_processing/` |
| Feature test corpus | `flink_project_demos/spark-project/` (rich Spark SQL patterns, no golden DDL/DML required for CI) |
| Skill invocation | Auto-trigger on "migrate this spark sql to flink sql" and close variants; do not set `disable-model-invocation` |
| CC validation | Out of scope for v1; add as tools in a later phase |

## Problem statement

Migration knowledge today lives in:

| Location | Role |
|----------|------|
| `shift_left_utils/.../ai/prompts/spark_fsql/` | LLM system prompts |
| `shift_left_utils/.../ai/prompts/common/` | Validation and refinement prompts |
| `shift_left_utils/.../ai/translator_to_flink_sql.py` | Orchestration pipeline |
| `shift_left_utils/.cursor/skills/ksql_to_flink/SKILL.md` | Prior art (ksqlDB path) |
| `flink_project_demos/customer_360/c360_flink_processing/` | Production golden Flink DDL/DML from shift_left migration |

Agents outside shift_left cannot access this workflow unless it is packaged as a self-contained skill with an isolated test harness.

## Goals

1. Portable `SKILL.md` + reference files for Claude Code, Cursor, and other SKILL.md runtimes.
2. Prompt fidelity: rules derived from `spark_fsql/` and `common/` prompts; sync script from shift_left.
3. Golden regression: Spark input from `c360_spark_processing/` compared to Flink output in `c360_flink_processing/pipelines/**/sql-scripts/`.
4. Feature coverage: optional harness runs against `spark-project/` sources (syntax-validated Spark SQL).
5. Isolation harness: Agno agent + local oMLX (OpenAI-compatible API).
6. Documented deployment for Claude Code and Cursor.

## Non-goals (v1)

- Replacing `shift_left table migrate` or `SparkToFlinkSqlAgent`.
- Confluent Cloud live validation tools (later phase).
- RAG retrieval (`shift_left.ai.rag`).
- ksqlDB migration (see `ksql-to-flink` skill).

## Source inputs

### Prompt inventory (canonical, in shift_left_utils)

```
shift_left_utils/src/shift_left/shift_left/ai/prompts/
├── spark_fsql/
│   ├── translator.txt
│   ├── table_detection.txt
│   ├── ddl_creation.txt
│   └── pyspark_extract.txt          # phase 2
└── common/
    ├── mandatory_validation.txt
    └── refinement.txt               # used when CC validation exists
```

### Reference pipeline

```
Raw Spark SQL
  → clean (strip DROP, comments)
  → detect CREATE TABLE statements (deterministic split, LLM fallback)
  → translate (translator.txt) → JSON { flink_ddl_output, flink_dml_output }
  → mandatory validation (mandatory_validation.txt)
  → [CC validate + refinement]  # later phase only
  → output ddl.{table}.sql, dml.{table}.sql
```

### Prompt / code alignment

`translator.txt` currently says output starts with `INSERT INTO {table_name}` but `TranslatorToFlinkSqlAgent` expects JSON with `flink_ddl_output` and `flink_dml_output`. The skill and vendored prompts must use the JSON contract (aligned with `mandatory_validation.txt`).

## Test assets

### Golden pairs (regression, P0)

Spark sources and migrated Flink outputs form input/output pairs:

| Spark input | Flink golden (DDL + DML) |
|-------------|--------------------------|
| `flink_project_demos/customer_360/c360_spark_processing/sources/src_customers.sql` | `c360_flink_processing/pipelines/sources/c360/src_customers/sql-scripts/` |
| `.../sources/src_transactions.sql` | `.../src_transactions/sql-scripts/` |
| `.../sources/src_loyalty_program.sql` | `.../src_loyalty_program/sql-scripts/` |
| `.../intermediates/int_customer_transactions.sql` | `.../dimensions/c360/dim_customer_transactions/sql-scripts/` |
| `.../facts/fct_customer_360_profile.sql` | `.../facts/c360/fct_customer_360_profile/sql-scripts/` |
| `.../views/customer_analytics_c360.sql` | `.../views/c360/customer_analytics_c360/sql-scripts/` |

Naming in c360 Flink pipelines follows shift_left conventions: `ddl.{table}.sql`, `dml.{table}.sql` under each pipeline's `sql-scripts/` folder. Staging copies under `c360_flink_processing/staging/` are migration artifacts and may differ slightly from deployed pipelines; prefer `pipelines/**/sql-scripts/` as golden.

Match threshold: ≥80% unordered line match (same metric as `shift_left/tests/ai/test_spark_migration.py`).

### Feature corpus (coverage, P1)

`flink_project_demos/spark-project/` — windows, CTEs, pivots, set ops, temporal analytics. Used to exercise translation rules; golden comparison optional unless references are added later.

Pre-migration gate: `spark-project/tests/validate_spark_scripts.py` confirms Spark SQL is syntactically valid before LLM migration tests.

## Deliverables

### Repository layout

```
research/spark-to-flink-skill/
├── SPEC.md
├── README.md
├── skill/
│   ├── SKILL.md                     # <500 lines; auto-invoke on migrate phrases
│   ├── translation-rules.md
│   ├── validation-rules.md
│   ├── function-mapping.md
│   └── examples.md                  # c360 pairs as worked examples
├── harness/
│   ├── pyproject.toml
│   ├── .env.example
│   ├── src/spark_flink_skill/
│   │   ├── pipeline.py
│   │   ├── prompts/                 # synced from shift_left via scripts/sync-prompts.sh
│   │   └── agents/migrate_agent.py
│   └── tests/
│       ├── test_pipeline_offline.py
│       ├── test_c360_golden.py      # integration; compares to c360_flink_processing
│       └── test_spark_project.py    # optional feature scripts
├── scripts/
│   ├── sync-prompts.sh
│   └── run-migration.sh
└── assets/
    └── FIXTURES.md                  # documents paths into flink_project_demos (no copy)
```

External fixture paths (not vendored):

- Spark golden inputs: `../../../flink_project_demos/customer_360/c360_spark_processing/`
- Flink golden outputs: `../../../flink_project_demos/customer_360/c360_flink_processing/`
- Feature corpus: `../../../flink_project_demos/spark-project/`

### SKILL.md frontmatter

```yaml
---
name: spark-to-flink
description: >-
  Translates Apache Spark SQL and PySpark pipelines to Confluent Cloud Flink SQL
  with DDL/DML separation, connector properties, and mandatory validation.
  Use when the user asks to migrate Spark SQL to Flink SQL, convert Spark batch
  SQL to Flink streaming SQL, or move dbt/Spark scripts to Confluent Cloud Flink.
---
```

Do not set `disable-model-invocation`. The description must include trigger phrases so Cursor and Claude Code auto-load the skill on migration requests.

### SKILL.md body (sections)

1. Scope — Confluent Cloud for Flink; batch Spark → unbounded streaming semantics.
2. Required inputs — `table_name`, source `.sql` path or pasted SQL, optional `schema_context` (default `.flink-dev`).
3. Workflow checklist — clean → detect → translate → validate → write files.
4. Translation rules — types, functions, joins, naming (`_pk_fk` → `_sid`), CTE preservation.
5. DDL template — `CREATE TABLE IF NOT EXISTS`, `PRIMARY KEY NOT ENFORCED`, `DISTRIBUTED BY HASH`, WITH block.
6. Output contract — JSON then `ddl.{table}.sql` + `dml.{table}.sql`.
7. Validation checklist — from `mandatory_validation.txt`.
8. Examples — link to `examples.md` (c360 `fct_customer_360_profile` pair).
9. Anti-patterns — no `CREATE STREAM`, no bare `topic` in WITH, no explicit `$rowtime METADATA` column.

### Agno harness

| Component | Role |
|-----------|------|
| `pipeline.py` | Deterministic orchestration; unit-tested without Agno |
| `migrate_agent.py` | Agno Agent with instructions from `skill/SKILL.md` |
| LLM | OpenAI-compatible client → local oMLX |

Default env (matches shift_left):

```
SL_LLM_BASE_URL=http://localhost:1337/v1
SL_LLM_MODEL=qwen3-coder-30b-a3b-instruct-mlx-4bit
SL_LLM_API_KEY=no_llm_key
```

CLI: `uv run spark-flink-migrate --table NAME --file PATH [--out-dir DIR]`

Agno smoke: `uv run spark-flink-agent "Migrate c360 facts/fct_customer_360_profile.sql to Flink SQL"`

## Deployment

### Cursor

| Scope | Path |
|-------|------|
| Personal | `~/.cursor/skills/spark-to-flink/` |
| Project (research) | `research/spark-to-flink-skill/skill/` (symlink or copy) |

### Claude Code

| Scope | Path |
|-------|------|
| Project | `research/spark-to-flink-skill/.claude/skills/spark-to-flink/` |
| Personal | `~/.claude/skills/spark-to-flink/` |

README documents copy/symlink steps for both runtimes.

## Acceptance criteria

### Skill package

1. `skill/SKILL.md` ≤ 500 lines; third-person description with migrate trigger terms.
2. All rules from `spark_fsql/translator.txt` and `common/mandatory_validation.txt` present in skill or linked files.
3. JSON output contract documented and enforced in examples.
4. README covers approach, shift_left relationship, Cursor/Claude install, oMLX setup.

### Harness

1. `uv run pytest harness/tests/test_pipeline_offline.py` passes without LLM.
2. With oMLX running, `test_c360_golden.py` passes on at least `src_customers.sql` and `fct_customer_360_profile.sql` with ≥80% DDL/DML match vs `c360_flink_processing`.
3. `uv run spark-flink-migrate` writes DDL/DML for a given c360 Spark file.
4. Agno agent smoke completes without tool errors.

### Out of scope for v1 acceptance

- CC validation tools (`post_flink_statement`, refinement loop).
- PySpark file migration.
- Full spark-project golden matrix.

## Implementation phases

| Phase | Scope | Exit |
|-------|-------|------|
| 1 — SPEC | This document | Done |
| 2 — Skill content | Extract prompts → `skill/` + reference files | Review vs shift_left prompts |
| 3 — Pipeline lib | `pipeline.py` mirroring `TranslatorToFlinkSqlAgent` | Offline tests green |
| 4 — Harness + Agno | CLI, agent, c360 golden tests | T0 c360 tests ≥80% with oMLX |
| 5 — Docs + deploy | README, sync script, Cursor/Claude paths | Install copy-paste works |
| 6 — Extensions | PySpark path, CC validation tools, spark-project matrix | Separate SPEC addendum |

## Relationship diagram

```
shift_left_utils/prompts/spark_fsql
        │ sync-prompts.sh
        ▼
research/spark-to-flink-skill/skill/SKILL.md
        │
        ├── harness/pipeline.py  ←── reference: TranslatorToFlinkSqlAgent
        │
c360_spark_processing/*.sql  ──migrate──►  compare ──►  c360_flink_processing/pipelines/**/sql-scripts/
spark-project/sources/*.sql  ──migrate──►  (feature coverage, optional golden)
```

## Success metric

An agent with `skill/SKILL.md` loaded, given `c360_spark_processing/facts/fct_customer_360_profile.sql`, produces Flink DDL/DML scoring ≥80% against `c360_flink_processing/pipelines/facts/c360/fct_customer_360_profile/sql-scripts/` — without reading shift_left source at runtime.
