CREATE TABLE IF NOT EXISTS event_stream (
    `user_id` BIGINT,
    `item_id` BIGINT,
    `behavior` STRING,
    `event_time` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
    `partition` BIGINT METADATA VIRTUAL,
    `offset` BIGINT METADATA VIRTUAL,
    PRIMARY KEY (`user_id`) NOT ENFORCED,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) DISTRIBUTED BY HASH(`user_id`) INTO 1 BUCKETS WITH (
    'changelog.mode' = 'append',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'value.format' = 'avro-registry'
);
