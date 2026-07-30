CREATE TABLE IF NOT EXISTS orders (
    customer_id BIGINT,
    item_id BIGINT,
    order_id BIGINT,
    purchase_date TIMESTAMP,
    PRIMARY KEY (customer_id, item_id) NOT ENFORCED
) DISTRIBUTED BY HASH(customer_id, item_id) INTO 6 BUCKETS WITH (
    'changelog.mode' = 'append',
    'key.format' = 'avro-registry',
    'value.format' = 'avro-registry',
    'kafka.retention.time' = '0',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all'
);