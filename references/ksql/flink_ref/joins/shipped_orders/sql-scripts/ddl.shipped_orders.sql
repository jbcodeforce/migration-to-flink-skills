CREATE TABLE IF NOT EXISTS shipped_orders (
    order_id STRING,
    order_ts STRING,
    total_amount DECIMAL(10, 2),
    customer_name STRING,
    shipment_id STRING,
    shipment_ts STRING,
    warehouse STRING,
    ship_time DECIMAL(10, 2),
    PRIMARY KEY (order_id) NOT ENFORCED
) DISTRIBUTED BY HASH(order_id) INTO 1 BUCKETS WITH (
    'changelog.mode' = 'append',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'key.json-registry.schema-context' = '.flink-dev',
    'value.json-registry.schema-context' = '.flink-dev',
    'key.format' = 'json-registry',
    'value.format' = 'json-registry'
);