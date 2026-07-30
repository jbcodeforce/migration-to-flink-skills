CREATE TABLE IF NOT EXISTS orders (
    order_id STRING,
    customer_id STRING,
    item_id STRING,
    purchase_date STRING,
    PRIMARY KEY (order_id) NOT ENFORCED
) DISTRIBUTED BY HASH(order_id) INTO 6 BUCKETS
WITH (
        'changelog.mode' = 'upsert',
    'key.format' = 'avro-registry',
    'value.format' = 'avro-registry',
    'kafka.retention.time' = '0',
    'scan.bounded.mode' = 'unbounded',
    'value.fields-include' = 'all',
    'scan.startup.mode' = 'earliest-offset'
);