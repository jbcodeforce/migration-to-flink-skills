CREATE TABLE IF NOT EXISTS all_songs (
    artist STRING,
    title STRING,
    genre STRING,
    PRIMARY KEY (genre) NOT ENFORCED
) DISTRIBUTED BY HASH(genre) INTO 1 BUCKETS
WITH (
    'value.format' = 'avro-registry',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);