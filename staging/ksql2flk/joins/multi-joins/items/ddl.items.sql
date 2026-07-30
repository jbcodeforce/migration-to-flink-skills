CREATE TABLE IF NOT EXISTS items (
    item_id STRING,
    item_name STRING,
    PRIMARY KEY (item_id) NOT ENFORCED
) DISTRIBUTED BY HASH(item_id) INTO 1 BUCKETS
WITH (
    'value.format' = 'json-registry',
    'scan.startup.mode' = 'earliest-offset',
    'scan.bounded.mode' = 'unbounded'
);