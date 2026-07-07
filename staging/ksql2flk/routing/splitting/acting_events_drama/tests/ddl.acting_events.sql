CREATE TABLE IF NOT EXISTS `acting_events` (
    `name` STRING,
    `title` STRING,
    `genre` STRING,
    PRIMARY KEY (`name`) NOT ENFORCED
) DISTRIBUTED BY HASH(`name`) INTO 6 BUCKETS WITH (
    'changelog.mode' = 'append',
    'key.format' = 'avro-registry',
    'value.format' = 'avro-registry',
    'kafka.retention.time' = '0',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all'
);