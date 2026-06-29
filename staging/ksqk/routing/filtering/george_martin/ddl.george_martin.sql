CREATE TABLE IF NOT EXISTS george_martin (
    id STRING,
    author STRING,
    title STRING,
    published_date STRING,
    publisher STRING,
    genre STRING,
    language STRING
) DISTRIBUTED BY HASH(id) INTO 1 BUCKETS
WITH (
    'topic' = 'george_martin_books',
    'value.format' = 'json-registry',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);