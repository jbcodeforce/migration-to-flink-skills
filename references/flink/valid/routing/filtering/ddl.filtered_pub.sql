CREATE TABLE filtered_publications (
    book_id INT NOT NULL,
    author STRING,
    title STRING,
    PRIMARY KEY (book_id) NOT ENFORCED
) DISTRIBUTED BY HASH(book_id) INTO 1 BUCKETS WITH (
    'changelog.mode' = 'upsert',
    'key.format' = 'avro-registry',
    'value.format' = 'avro-registry',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all'
);