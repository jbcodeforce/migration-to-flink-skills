CREATE TABLE IF NOT EXISTS customer_purchases (
    id STRING,
    current_purchase DOUBLE,
    previous_purchase DOUBLE,
    txn_ts VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR
) WITH (
    'value.format' = 'json-registry',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.startup.mode' = 'earliest-offset',
    'scan.bounded.mode' = 'unbounded',
    'value.fields-include' = 'all'
);