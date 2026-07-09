CREATE TABLE IF NOT EXISTS rock_songs (
    artist STRING,
    title STRING
) WITH (
    'value.format' = 'avro-registry',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all'
);