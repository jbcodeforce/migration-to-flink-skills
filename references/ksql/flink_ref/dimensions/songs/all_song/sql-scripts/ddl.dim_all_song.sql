CREATE TABLE IF NOT EXISTS dim_all_songs (
    artist STRING,
    title STRING,
    genre STRING,
    PRIMARY KEY (artist) NOT ENFORCED
) DISTRIBUTED BY HASH(artist) INTO 1 BUCKETS WITH (
    'changelog.mode' = 'upsert',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'value.json-registry.schema-context' = '.flink-dev',
    'value.format' = 'json-registry'
);