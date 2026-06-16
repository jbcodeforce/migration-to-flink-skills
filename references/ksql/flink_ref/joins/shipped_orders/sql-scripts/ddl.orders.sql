CREATE TABLE IF NOT EXISTS orders (
    id INT,
    order_ts STRING,
    total_amount DOUBLE,
    customer_name STRING,
    PRIMARY KEY (id) NOT ENFORCED
) DISTRIBUTED BY HASH(id) INTO 4 BUCKETS WITH (
    'value.format' = 'json-registry',
    'value.json-registry.schema-context' = '.flink-dev',
    'value.fields-include' = 'all',
    'scan.startup.mode' = 'earliest-offset',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'changelog.mode' = 'append'
);