CREATE TABLE IF NOT EXISTS source_table (
    id INT,
    name STRING,
    product_id INT,
    PRIMARY KEY (id) NOT ENFORCED
) DISTRIBUTED BY HASH(id) INTO 6 BUCKETS WITH (
    'changelog.mode' = 'upsert',
    'key.format' = 'avro-registry',
    'value.format' = 'avro-registry',
    'kafka.retention.time' = '0',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all'
);