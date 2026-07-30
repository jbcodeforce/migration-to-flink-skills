CREATE TABLE IF NOT EXISTS customers (
    customer_id STRING,
    customer_name STRING,
    PRIMARY KEY (customer_id) NOT ENFORCED
) DISTRIBUTED BY HASH(customer_id) INTO 6 BUCKETS
WITH (
    'changelog.mode' = 'upsert',
    'key.format' = 'json-registry',
    'value.format' = 'json-registry',
    'kafka.retention.time' = '0',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all'
);