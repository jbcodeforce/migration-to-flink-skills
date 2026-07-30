CREATE TABLE IF NOT EXISTS orders (
    order_id BIGINT,
    purchase_date TIMESTAMP,
    customer_id BIGINT,
    item_id BIGINT,
    PRIMARY KEY (order_id) NOT ENFORCED
) DISTRIBUTED BY HASH(order_id) INTO 6 BUCKETS WITH (
    'changelog.mode' = 'upsert',
    'key.format' = 'avro-registry',
    'value.format' = 'avro-registry',
    'kafka.retention.time' = '0',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all'
);