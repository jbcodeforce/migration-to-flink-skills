CREATE TABLE IF NOT EXISTS dim_acting_events_fantasy (
    name STRING,
    title STRING,
    PRIMARY KEY (name) NOT ENFORCED
) DISTRIBUTED BY HASH(name) INTO 1 BUCKETS WITH (
    'changelog.mode' = 'upsert',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'key.json-registry.schema-context' = '.flink-dev',
    'value.json-registry.schema-context' = '.flink-dev',
    'key.format' = 'json-registry',
    'value.format' = 'json-registry'
);