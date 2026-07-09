CREATE TABLE IF NOT EXISTS classical_songs (
    artist STRING,
    title STRING
) WITH (
    'value.format' = 'avro-registry',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);