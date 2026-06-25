-- Intended DDL for raw_classical_songs (artist and title columns, avro-registry format).
CREATE TABLE IF NOT EXISTS raw_classical_songs (
    artist STRING,
    title STRING,
    PRIMARY KEY (artist) NOT ENFORCED
) DISTRIBUTED BY HASH(artist) INTO 1 BUCKETS WITH (
    'changelog.mode' = 'append',
    'value.format' = 'avro-registry'
);
