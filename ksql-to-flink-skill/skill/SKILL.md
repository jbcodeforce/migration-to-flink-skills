---
name: ksql-to-flink
description: >-
  Translates Confluent ksqlDB SQL scripts to Apache Flink SQL with proper streaming
  semantics. Use when converting ksqlDB to Flink, migrating CREATE STREAM scripts,
  or when the user asks to migrate ksql to Flink SQL.
---

# ksqlDB to Flink SQL migration

## Scope

Confluent Cloud for Flink. Every ksqlDB `CREATE STREAM` or `CREATE TABLE` becomes Flink `CREATE TABLE IF NOT EXISTS`. Flink has no `CREATE STREAM`.

## Required inputs

- `table_name` — target Flink table (lowercase)
- ksqlDB source — `.ksql` file or pasted SQL

## Workflow

```
- [ ] 1. Clean input (remove DROP, SET, comments)
- [ ] 2. Translate full script in one pass
- [ ] 3. Validate: no CREATE STREAM in DDL output
- [ ] 4. Write ddl.{table}.sql and dml.{table}.sql
- [ ] 5. Analyze DML FROM/JOIN dependencies; generate source stub DDL in tests/ddl.{source}.sql (LLM)
- [ ] 6. Deploy source DDLs from tests/ to Confluent Cloud Flink (confluent-sql)
- [ ] 7. Deploy target DDL after source DDLs reach RUNNING/COMPLETED
- [ ] 8. Deploy target DML after target DDL succeeds
- [ ] 9. Verify statement health; triage on failure
```

Harness `ksql-flink-migrate` runs steps 5–9 by default. Use `--skip-deploy` for translate-only runs.

## Mandatory DDL replacements (apply first)

- `CREATE STREAM` → `CREATE TABLE IF NOT EXISTS`
- ksqlDB `CREATE TABLE` → `CREATE TABLE IF NOT EXISTS`
- Never output `CREATE STREAM` in `flink_ddl_output`

## Output format

JSON only, no markdown fences:

```json
{
  "flink_ddl_output": "CREATE TABLE IF NOT EXISTS ...",
  "flink_dml_output": "INSERT INTO ..."
}
```

Source-only tables: empty `flink_dml_output`. Continuous queries: `INSERT INTO` replaces `EMIT CHANGES`.

## Stream vs table

- ksqlDB STREAM → Flink TABLE
- ksqlDB TABLE → Flink TABLE with PRIMARY KEY
- `KAFKA_TOPIC` property → table name in DDL (lowercase, unquoted)

## Types

- `VARCHAR` → `STRING`
- `TIMESTAMP` → `TIMESTAMP(3)`
- Do not add explicit `$rowtime TIMESTAMP(3) METADATA FROM 'timestamp'` in DDL

## Functions

| ksqlDB | Flink |
|--------|-------|
| `PROCTIME()` | `$rowtime` |
| `LATEST_BY_OFFSET(col)` | CTE + `ROW_NUMBER()` + outer `GROUP BY` (see Deduplication) |
| `INSTR(a,b,pos,occ)` | `LOCATE(b, a, pos)` |
| `LENGTH(s)` | `CHARACTER_LENGTH(s)` |
| `EXPLODE(arr)` | `CROSS JOIN UNNEST(arr) AS u (element)` |
| `TIMESTAMPTOSTRING(ts, fmt)` | `DATE_FORMAT(ts, fmt)` |

## Windowing

| ksqlDB | Flink |
|--------|-------|
| `WINDOW TUMBLING (SIZE X SECONDS)` | `TABLE(TUMBLE(TABLE src, DESCRIPTOR($rowtime), INTERVAL 'X' SECOND))` |
| `WINDOW HOPPING (SIZE X, ADVANCE BY Y)` | `TABLE(HOP(...))` |
| `WINDOW SESSION (TIMEOUT X)` | `TABLE(SESSION(...))` |

When source uses `WINDOW TUMBLING`, add to DDL:

```sql
window_start TIMESTAMP(3),
window_end TIMESTAMP(3),
```

## Connector WITH block

```
'value.format' = 'avro-registry',
'value.avro-registry.schema-context' = '.flink-dev',
'key.avro-registry.schema-context' = '.flink-dev',
'scan.startup.mode' = 'earliest-offset',
'value.fields-include' = 'all',
'kafka.retention.time' = '0',
'kafka.producer.compression.type' = 'snappy',
'scan.bounded.mode' = 'unbounded'
```

JSON sources: use `json-registry` instead of `avro-registry`.

## DML patterns

- `CREATE STREAM x AS SELECT ...` → separate DDL + `INSERT INTO x SELECT ...`
- `INSERT INTO target SELECT ...` → keep as Flink `INSERT INTO`
- Stream-table join: add `FOR SYSTEM_TIME AS OF s.$rowtime` on table side

## Deduplication (GROUP BY + LATEST_BY_OFFSET)

When ksql uses `GROUP BY` with `LATEST_BY_OFFSET(...)` — including `CREATE TABLE ... AS SELECT ... GROUP BY ... EMIT CHANGES` — the Flink DML must preserve the ksql `GROUP BY`. Do not emit a flat subquery that omits the outer `GROUP BY`.

Rules:

1. `PARTITION BY` in `ROW_NUMBER()` must list the same columns as ksql `GROUP BY` (exact set; order may differ).
2. Wrap deduplication in a CTE named `deduplicated`.
3. Outer query: `SELECT * FROM deduplicated GROUP BY <ksql group by columns>`.
4. Columns in ksql `GROUP BY` appear bare in the SELECT list.
5. Columns wrapped in `LATEST_BY_OFFSET(...)` in ksql become bare column references inside the CTE; `ROW_NUMBER()` picks the latest row per group.

```sql
-- ksqlDB
SELECT
  LATEST_BY_OFFSET(`msg_type`) `msg_type`,
  `msg_epoch`,
  LATEST_BY_OFFSET(`msg_body`) `msg_body`,
  `msg_from_id`,
  `msg_incoming`
FROM source_st
GROUP BY `msg_from_id`, `msg_incoming`, `msg_epoch`
EMIT CHANGES;

-- Flink
INSERT INTO target_table
WITH deduplicated AS (
    SELECT
        `msg_type`,
        `msg_epoch`,
        `msg_body`,
        `msg_from_id`,
        `msg_incoming`
    FROM (
        SELECT
            `msg_type`,
            `msg_epoch`,
            `msg_body`,
            `msg_from_id`,
            `msg_incoming`,
            ROW_NUMBER() OVER (
                PARTITION BY `msg_from_id`, `msg_incoming`, `msg_epoch`
                ORDER BY $rowtime DESC
            ) AS rn
        FROM source_st
    )
    WHERE rn = 1
)
SELECT * FROM deduplicated
GROUP BY `msg_from_id`, `msg_incoming`, `msg_epoch`;
```

See [examples.md](references/examples.md) for `KES-CHAT.sql` → `kes_ice_chat_deal`.

## DDL template

```sql
CREATE TABLE IF NOT EXISTS table_name (
    col STRING,
    PRIMARY KEY (col) NOT ENFORCED
) DISTRIBUTED BY HASH(col) INTO 1 BUCKETS
WITH ( ... );
```

## Quality checks

- `flink_ddl_output` must not contain `CREATE STREAM`
- Connector properties complete
- Window columns in DDL when tumbling windows used
- When ksql DML has `GROUP BY` with `LATEST_BY_OFFSET`, `flink_dml_output` must use `WITH deduplicated AS` and an outer `GROUP BY` matching ksql
- No explanations in output

## Source table stubs (tests/)

DML references upstream tables (`FROM`, `JOIN`) that may not exist in CC Flink. Before deploy:

1. Extract table names from DML not defined in target DDL
2. LLM-generate `CREATE TABLE IF NOT EXISTS` stubs matching DML column usage
3. Write `tests/ddl.{source_table}.sql` under the output directory

Example layout:

```
output/
  ddl.kes_ice_chat_deal.sql
  dml.kes_ice_chat_deal.sql
  tests/
    ddl.kes_ice_chat_deal_st.sql
```

## Deploy phase (confluent-sql)

After writing DDL/DML and source stubs, deploy to Confluent Cloud for Flink using Agno tools backed by the `confluent-sql` Python driver.

Prerequisites: Flink API credentials in `harness/.env`. See [docs/FLINK_DEPLOY.md](../../docs/FLINK_DEPLOY.md).

Statement names: `{table-with-hyphens}-ddl` and `{table-with-hyphens}-dml` (underscores → hyphens). Source stubs use the same `-ddl` suffix on the source table name.

Tool sequence:

1. `create_flink_statement` — submit each `tests/ddl.*.sql` source stub
2. `wait_flink_statement_phase` or `get_flink_statement` — poll each source DDL until RUNNING/COMPLETED/APPLIED
3. `create_flink_statement` — submit target DDL SQL
4. `wait_flink_statement_phase` — poll until target DDL phase is RUNNING/COMPLETED/APPLIED
5. `create_flink_statement` — submit target DML SQL
6. `wait_flink_statement_phase` — poll DML until RUNNING or FAILED
7. `check_flink_statement_health` — verify DML when available
8. On failure: `get_flink_statement_exceptions` then fix and redeploy

Full reference: [confluent-sql-deploy.md](references/confluent-sql-deploy.md). Post-deploy triage: `flink-statement-troubleshooting` skill.

## References

- [translation-rules.md](references/translation-rules.md)
- [examples.md](references/examples.md)
- [confluent-sql-deploy.md](references/confluent-sql-deploy.md)

## Harness

```bash
cd harness && uv sync --extra dev
uv run ksql-flink-migrate --table dim_all_songs --file <path>/merge.ksql --out-dir output/
# translate only: add --skip-deploy
# retry deploy via agent on failure: --agent-deploy-on-failure
```
