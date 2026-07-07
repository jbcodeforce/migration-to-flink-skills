CREATE TABLE IF NOT EXISTS all_publications (
    `author` STRING,
    PRIMARY KEY (`author`) NOT ENFORCED
) DISTRIBUTED BY HASH(`author`) INTO 6 BUCKETS WITH (
    'changelog.mode' = 'append',
    'key.format' = 'avro-registry',
    'value.format' = 'avro-registry',
    'kafka.retention.time' = '0',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all'
);