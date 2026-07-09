CREATE TABLE IF NOT EXISTS all_songs (
    artist STRING,
    title STRING,
    genre STRING
) WITH (
    'value.format' = 'avro-registry',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'scan.bounded.mode' = 'unbounded'
);