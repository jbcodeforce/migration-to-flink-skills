CREATE TABLE IF NOT EXISTS orders_enriched (
    customer_id STRING,
    customer_name STRING,
    order_id STRING,
    purchase_date STRING,
    item_id STRING,
    item_name STRING,
    PRIMARY KEY (order_id) NOT ENFORCED
) DISTRIBUTED BY HASH(order_id) INTO 1 BUCKETS
WITH (
    'value.format' = 'json-registry',
    'scan.startup.mode' = 'earliest-offset',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'value.fields-include' = 'all'
);