# ksqlDB to Flink SQL migration skill — specification

## Intent

Portable agent skill for migrating Confluent ksqlDB scripts to Confluent Cloud for Flink SQL, using prompts from `shift_left_utils/.../prompts/ksql_fsql/` and golden references in `flink_project_demos/ksql_tutorial/flink_ref/`.

## Resolved decisions

| Decision | Choice |
|----------|--------|
| Repository home | `research/ksql-to-flink-skill/` |
| Prompt source | `shift_left_utils/.../prompts/ksql_fsql/` + `common/mandatory_validation.txt` (optional) |
| KSQL inputs | `flink_project_demos/ksql_tutorial/sources/` |
| Golden references | `flink_project_demos/ksql_tutorial/flink_ref/**/sql-scripts/` |
| Pipeline | Matches `KsqlToFlinkSqlAgent`: clean → translate (no mandatory validation by default) |
| Skill invocation | Auto-trigger on "migrate ksql to flink sql" phrases |
| CC validation | Harness deploy via confluent-sql REST (default on migrate) |

## Golden pairs (P0)

| KSQL source | Flink golden | Table |
|-------------|--------------|-------|
| `sources/routing/merge.ksql` | `flink_ref/dimensions/songs/all_song/` | `dim_all_songs` |
| `sources/joins/stream_stream.ksql` | `flink_ref/joins/shipped_orders/` | `shipped_orders` |
| `sources/routing/splitting.ksql` | `flink_ref/dimensions/acting_events/acting_events_drama/` | `dim_acting_events_drama` |

KSQL files without `flink_ref` goldens (`filtering.ksql`, `deduplicate.ksql`, `count_pageviews.ksql`) are documented for manual testing only.

## Acceptance criteria

1. `uv run pytest harness/tests/ -m "not integration"` passes without LLM.
2. Mock golden tests ≥80% match vs `flink_ref` DDL/DML.
3. `skill/SKILL.md` under 500 lines with auto-invoke description.
4. README documents Cursor and Claude Code deployment.
