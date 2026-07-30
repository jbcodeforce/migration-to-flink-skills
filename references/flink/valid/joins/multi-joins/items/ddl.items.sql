CREATE TABLE IF NOT EXISTS items (
    item_id STRING,
    item_name STRING,
    PRIMARY KEY (item_id) NOT ENFORCED
) DISTRIBUTED BY HASH(item_id) INTO 1 BUCKETS
WITH (
       'changelog.mode' = 'upsert',
    'key.format' = 'avro-registry',
    'value.format' = 'avro-registry',
    'kafka.retention.time' = '0',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all'
);