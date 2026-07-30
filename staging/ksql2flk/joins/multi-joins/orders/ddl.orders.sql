CREATE TABLE IF NOT EXISTS orders (
    order_id STRING,
    customer_id STRING,
    item_id STRING,
    purchase_date STRING,
    PRIMARY KEY (order_id) NOT ENFORCED
) DISTRIBUTED BY HASH(order_id) INTO 1 BUCKETS
WITH (
    'value.format' = 'json-registry',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);