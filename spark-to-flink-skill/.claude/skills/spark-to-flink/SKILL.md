---
name: spark-to-flink
description: >-
  Translates Apache Spark SQL pipelines to Confluent Cloud Flink SQL
  with DDL/DML separation, connector properties, and mandatory validation.
  Use when the user asks to migrate Spark SQL to Flink SQL, convert Spark batch
  SQL to Flink streaming SQL.
---

# Spark SQL to Flink SQL migration

## Scope

Confluent Cloud for Flink only. Convert Spark batch SQL (and eventually PySpark) into unbounded Flink streaming SQL with Kafka registry connectors.

## Required inputs

- `table_name` — target Flink table (lowercase, unquoted)
- Spark SQL source — file path or pasted SQL
- Optional `schema_context` — default `.flink-dev`

## Workflow checklist

```
- [ ] 1. Clean input (remove DROP, comments)
- [ ] 2. Detect CREATE TABLE / TEMPORARY VIEW statements
- [ ] 3. Translate each statement to Flink DDL + DML
- [ ] 4. Run mandatory validation on DDL and DML
- [ ] 5. Write ddl.{table}.sql and dml.{table}.sql
```

## Step 1 — Clean input

Remove `DROP TABLE`, `DROP STREAM`, `--` line comments, and `/* */` block comments. Preserve query structure and indentation.

## Step 2 — Detect statements

If multiple `CREATE TABLE` or `CREATE OR REPLACE TEMPORARY VIEW` blocks exist, translate each separately.

## Step 3 — Translate

Apply rules from [translation-rules.md](references/translation-rules.md). Output JSON only:

```json
{
  "sql_input": "<original spark sql>",
  "flink_ddl_output": "CREATE TABLE IF NOT EXISTS ...",
  "flink_dml_output": "INSERT INTO ..."
}
```

Spark `CREATE OR REPLACE TEMPORARY VIEW` becomes Flink `INSERT INTO` continuous query reading upstream Kafka tables. CSV/temporary view setup in Spark is replaced by existing `src_*` raw Kafka tables in the Flink project.

## Step 4 — Mandatory validation

Apply [validation-rules.md](references/validation-rules.md):

- Every `CREATE TABLE` has `PRIMARY KEY (...) NOT ENFORCED`
- `DISTRIBUTED BY HASH(pk_column) INTO 1 BUCKETS` uses same column as PK
- No `'topic'` or `'connector' = 'kafka'` in WITH clause
- Default format: avro-registry with `.flink-dev` schema context
- Replace `!=` with `<>`
- Backtick SQL reserved words used as column names (`state`, `order`, etc.)

## Step 5 — Write output files

```
ddl.{table_name}.sql   — CREATE TABLE IF NOT EXISTS
dml.{table_name}.sql   — INSERT INTO ... SELECT ...
```

## Core translation rules (summary)

### Types

- `VARCHAR` → `STRING`
- `TIMESTAMP` → `TIMESTAMP(3)`
- `NUMERIC` → `DECIMAL(p,s)`

### Functions

- `surrogate_key(a,b)` → `MD5(CONCAT_WS(',', a, b))`
- `current_timestamp()` → `$rowtime` or `PROCTIME()` depending on context
- `DATEDIFF(end, start)` → `TIMESTAMPDIFF(DAY, CAST(start AS TIMESTAMP_LTZ(3)), end)`
- `CURRENT_DATE()` → `CURRENT_DATE`
- `date_trunc` → `DATE_FORMAT` where applicable

### Joins

- `LEFT ANTI JOIN` → `LEFT JOIN ... WHERE right.id IS NULL`

### Naming

- `_pk_fk` / `_primary_key` → `_sid`
- Lowercase table names, do not quote table names
- Preserve column casing from source

### DDL template

```sql
CREATE TABLE IF NOT EXISTS table_name (
    col1 STRING,
    col2 TIMESTAMP(3),
    PRIMARY KEY (col1) NOT ENFORCED
) DISTRIBUTED BY HASH(col1) INTO 1 BUCKETS
WITH (
    'changelog.mode' = 'append',
    'key.avro-registry.schema-context' = '.flink-dev',
    'value.avro-registry.schema-context' = '.flink-dev',
    'key.format' = 'avro-registry',
    'value.format' = 'avro-registry',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all'
);
```

Use `'changelog.mode' = 'upsert'` for deduplicated source tables keyed by natural id.

### DML template

```sql
INSERT INTO target_table
WITH step AS (
    SELECT ...
    FROM upstream_table
)
SELECT ... FROM step;
```

## Anti-patterns

- Do not emit `CREATE STREAM` (Flink uses `CREATE TABLE` only)
- Do not add explicit `$rowtime TIMESTAMP(3) METADATA FROM 'timestamp'` in DDL
- Do not leave markdown fences in output
- Do not include explanations in SQL output

## Examples

See [examples.md](references/examples.md) for c360 `src_customers` and `src_loyalty_program` pairs.

## Local harness (optional)

```bash
cd harness && uv sync --extra dev
uv run spark-flink-migrate --table src_c360_customers --file <spark-sql-path> --out-dir output/
```

Requires OpenAI-compatible LLM (oMLX) at `SL_LLM_BASE_URL`.

## References

- [translation-rules.md](references/translation-rules.md) — full rule set from shift_left prompts
- [validation-rules.md](references/validation-rules.md) — post-translation fix-up
- [function-mapping.md](references/function-mapping.md) — Spark → Flink function table
- [examples.md](references/examples.md) — worked c360 migrations
