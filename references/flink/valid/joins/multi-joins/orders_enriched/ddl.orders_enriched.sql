CREATE TABLE IF NOT EXISTS orders_enriched (
    customer_id STRING NOT NULL,
    order_id STRING NOT NULL,
    item_id STRING NOT NULL,
    customer_name STRING,
    purchase_date STRING,
    item_name STRING,
    PRIMARY KEY (customer_id, order_id, item_id) NOT ENFORCED
) DISTRIBUTED BY HASH(customer_id, order_id, item_id) INTO 6 BUCKETS
WITH (
    'key.format' = 'avro-registry',
    'value.format' = 'avro-registry',
    'changelog.mode' = 'upsert',
    'scan.startup.mode' = 'earliest-offset',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'value.fields-include' = 'all'
);