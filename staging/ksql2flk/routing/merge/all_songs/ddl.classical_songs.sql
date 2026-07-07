CREATE TABLE IF NOT EXISTS classical_songs (
    artist STRING,
    title STRING,
    PRIMARY KEY (artist) NOT ENFORCED
) DISTRIBUTED BY HASH(artist) INTO 6 BUCKETS
WITH (
    'key.format' = 'avro-registry',
    'changelog.mode' = 'append',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'avro-registry',
    'value.fields-include' = 'all'
);