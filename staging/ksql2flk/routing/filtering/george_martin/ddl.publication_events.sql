CREATE TABLE IF NOT EXISTS publication_events (
    bookid BIGINT,
    author STRING,
    title STRING,
    PRIMARY KEY (bookid) NOT ENFORCED
) WITH (
    'value.format' = 'json-registry',
    'num.partitions' = '1'
);