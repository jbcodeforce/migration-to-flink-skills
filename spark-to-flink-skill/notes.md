# Notes

## 2026-06-10 — Initial build

- Scaffolded `research/spark-to-flink-skill/` per SPEC.
- Synced prompts from `shift_left_utils`; appended JSON output contract to `translator.txt` (shift_left code expects JSON, prompt text was incomplete).
- Pipeline mirrors `TranslatorToFlinkSqlAgent`: clean, deterministic table detection, LLM translate, LLM validate.
- Golden pairs point to `c360_spark_processing` → `c360_flink_processing/pipelines/**/sql-scripts/`.
- Tests: offline always run; mock integration returns golden DDL/DML; live integration skips when oMLX down.
- CC validation deferred to later tools phase.

## oMLX

Default `SL_LLM_BASE_URL=http://localhost:7999/v1`. Integration tests skip when unreachable.

## 2026-06-10 — Agno skills harness simplification

- Replaced `MigrationPipeline` (duplicate shift_left prompts + 2-step LLM) with Agno `Agent` + `LocalSkills`.
- Harness loads `skill/SKILL.md` via `Skills(loaders=[LocalSkills(skill_dir(), validate=False)])`.
- oMLX called through `OpenAIChat` with `SL_LLM_BASE_URL` / `SL_LLM_MODEL`.
- Reference docs moved to `skill/references/` for `get_skill_reference` support.
- Removed harness `prompts/`, `testing.py`, and mock e2e test.
- CLI: deterministic clean/detect, then `run_migration` + `extract_sql_blocks` + `write_output`.
