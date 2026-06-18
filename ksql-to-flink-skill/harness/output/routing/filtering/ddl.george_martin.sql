CREATE TABLE IF NOT EXISTS george_martin (
    bookid BIGINT,
    author STRING,
    title STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'george_martin_books',
    'value.format' = 'json-registry',
    'value.json-registry.schema-context' = '.flink-dev',
    'key.json-registry.schema-context' = '.flink-dev',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);