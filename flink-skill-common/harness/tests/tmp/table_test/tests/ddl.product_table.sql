CREATE TABLE IF NOT EXISTS product_table (
    `id` INT,
    `product` STRING,
    `product_name` STRING,
    PRIMARY KEY (`id`) NOT ENFORCED
) DISTRIBUTED BY HASH(`id`) INTO 6 BUCKETS WITH (
    'changelog.mode' = 'append',
    'key.format' = 'avro-registry',
    'value.format' = 'avro-registry',
    'kafka.retention.time' = '0',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all'
);