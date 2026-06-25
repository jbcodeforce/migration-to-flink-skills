CREATE TABLE IF NOT EXISTS raw_acting_events (
    name STRING,
    title STRING,
    genre STRING,
    PRIMARY KEY (name) NOT ENFORCED
) DISTRIBUTED BY HASH(name) INTO 1 BUCKETS WITH (
    'changelog.mode' = 'append',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'value.json-registry.schema-context' = '.flink-dev',
    'value.format' = 'json-registry'
);