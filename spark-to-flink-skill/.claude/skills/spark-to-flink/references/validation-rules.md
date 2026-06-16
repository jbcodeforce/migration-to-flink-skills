# Validation rules

From `shift_left_utils/.../prompts/common/mandatory_validation.txt`.

## Primary key

- Every `CREATE TABLE` must declare `PRIMARY KEY (column) NOT ENFORCED`
- PK column must match `DISTRIBUTED BY HASH(column)`

## Columns

- DML column names must match DDL
- Remove `$rowtime TIMESTAMP(3) METADATA FROM 'timestamp'` from DDL
- Backtick reserved words: `` `state` ``, `` `order` ``, etc.

## Connector

Remove from WITH:

- `'topic' = '...'`
- `'connector' = 'kafka'`

Default avro-registry block:

```
'changelog.mode' = 'append',
'kafka.retention.time' = '0',
'kafka.producer.compression.type' = 'snappy',
'scan.bounded.mode' = 'unbounded',
'scan.startup.mode' = 'earliest-offset',
'value.fields-include' = 'all',
'key.avro-registry.schema-context' = '.flink-dev',
'value.avro-registry.schema-context' = '.flink-dev',
'key.format' = 'avro-registry',
'value.format' = 'avro-registry'
```

## Syntax

- `!=` → `<>`
- Valid Flink SQL only in output

## Expected structure

```sql
CREATE TABLE IF NOT EXISTS name (
    col TYPE,
    PRIMARY KEY (col) NOT ENFORCED
) DISTRIBUTED BY HASH(col) INTO 1 BUCKETS
WITH ( ... );
```

## Output

Return JSON:

```json
{
  "flink_ddl_output": "...",
  "flink_dml_output": "..."
}
```
