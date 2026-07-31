# Translation rules


* PRESERVE the column name casing (camelCase for kpiName etc, or snake_case, etc.).

## Data types

- `VARCHAR` → `STRING`
- `TIMESTAMP` → `TIMESTAMP(3)` or `TIMESTAMP_LTZ(3)`
- `NUMERIC` → `DECIMAL(p,s)`
- Lowercase table names; do not quote table names

## DDL

- `CREATE TABLE` → `CREATE TABLE IF NOT EXISTS`
- Add `PRIMARY KEY NOT ENFORCED` on natural or surrogate key
- `DISTRIBUTED BY HASH(key_column) INTO N BUCKETS`

## DML
- Start `INSERT INTO {table_name}`
- End each `SELECT` clause line with comma (except the last)

## Connector WITH properties

| Spark / ksql | Flink |
|--------------|-------|
| `VALUE_FORMAT='JSON_SR'` | `'value.format' = 'json-registry'` |
| `'value_format' = 'JSON'` | `'value.format' = 'json-registry'` |
| `'value_format' = 'AVRO'` | `'value.format' = 'avro-registry'` |
| `'key_format' = 'KAFKA'` | `'key.format' = 'json-registry'` |

Always add:

```
'scan.startup.mode' = 'earliest-offset'
'value.fields-include' = 'all'
'kafka.retention.time' = '0'
'kafka.producer.compression.type' = 'snappy'
'scan.bounded.mode' = 'unbounded'
```

## Functions

- `surrogate_key(...)` → `MD5(CONCAT_WS(',', ...))`
- `current_timestamp()` → `$rowtime`
- `date_trunc()` → `DATE_FORMAT()` where applicable
- `split_part()` → `REGEXP_EXTRACT()` when possible
- `DATEDIFF()` → `TIMESTAMPDIFF()`
- `CURRENT_DATE()` → `CURRENT_DATE`
-  start CONCAT_WS with ',' as delimiter: `MD5(CONCAT_WS(',', ...))`
-  `DATEADD()` → ` TIMESTAMPDIFF(DAY, CAST(created_date AS TIMESTAMP_LTZ(3)), CURRENT_DATE) as `



## Joins

`LEFT ANTI JOIN t2 ON ...` → `LEFT JOIN t2 ON ... WHERE t2.<key> IS NULL`

## Naming

- `_pk_fk` / `_primary_key` → `_sid`

## CTEs

Keep `WITH` clauses and dependency order. Preserve filters and `GROUP BY` logic.

## Streaming

- Add watermarks when event-time columns drive aggregations
- Batch time windows → `TUMBLE` / `HOP` / `SESSION` table functions
- Deduplication: `ROW_NUMBER() OVER (PARTITION BY key ORDER BY $rowtime DESC)`

## Output format

JSON with `flink_ddl_output` and `flink_dml_output`. No markdown fences or prose.
