CREATE TABLE IF NOT EXISTS customers (
    customer_id STRING,
    customer_name STRING,
    PRIMARY KEY (customer_id) NOT ENFORCED
) DISTRIBUTED BY HASH(customer_id) INTO 1 BUCKETS
WITH (
    'value.format' = 'json-registry',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all'
);