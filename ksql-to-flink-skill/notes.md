# Notes

## 2026-06-10 — Initial build

- Mirrored `spark-to-flink-skill/` structure for ksqlDB path.
- Prompts synced from `shift_left_utils/.../prompts/ksql_fsql/`.
- Pipeline matches `KsqlToFlinkSqlAgent`: clean → single translate pass (no mandatory validation).
- Golden pairs from `ksql_tutorial/sources/` vs `ksql_tutorial/flink_ref/`.
- Three P0 golden pairs: merge, shipped_orders, acting_events_drama.
- filtering, deduplicate, count_pageviews documented as no golden yet.

## 2026-06-10 — Agno skills harness simplification

- Replaced `MigrationPipeline` (duplicate shift_left prompts + LLM adapter) with Agno `Agent` + `LocalSkills`.
- Harness loads `skill/SKILL.md` via `Skills(loaders=[LocalSkills(skill_dir(), validate=False)])`.
- oMLX called through `OpenAIChat` with `SL_LLM_BASE_URL` / `SL_LLM_MODEL`.
- Reference docs moved to `skill/references/` for `get_skill_reference` support.
- Removed harness `prompts/`, `testing.py`, and mock e2e test.
- CLI: deterministic clean, then `run_migration` + `extract_sql_blocks` + `write_output`.

## 2026-06-10 — GROUP BY + LATEST_BY_OFFSET CTE pattern

- Skill and translator prompt now require `WITH deduplicated AS` + outer `GROUP BY` when ksql uses `GROUP BY` with `LATEST_BY_OFFSET`.
- Added `KMA-CHAT.sql` → `kma_chat` worked example in `skill/references/examples.md`.
- Added `test_skill_patterns.py` to guard the documented pattern offline.

## 2026-06-10 — mcp-confluent deploy integration

- Harness deploys DDL then DML via `@confluentinc/mcp-confluent` MCP tools after translation.
- Default: always deploy; `--skip-deploy` for translate-only runs.
- `--agent-deploy-on-failure` enables Agno + MCPTools retry loop.
- Docs: `docs/MCP_SETUP.md`, `skill/references/mcp-deploy.md`.
