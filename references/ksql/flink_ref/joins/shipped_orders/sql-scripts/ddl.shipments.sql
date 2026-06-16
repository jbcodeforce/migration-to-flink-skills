CREATE TABLE IF NOT EXISTS shipments (
    id STRING,
    ship_ts STRING,
    order_id INT,
    warehouse STRING,
    PRIMARY KEY (id) NOT ENFORCED
) DISTRIBUTED BY HASH(id) INTO 4 BUCKETS WITH (
    'changelog.mode' = 'append',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'value.json-registry.schema-context' = '.flink-dev',
    'value.format' = 'json-registry'
);