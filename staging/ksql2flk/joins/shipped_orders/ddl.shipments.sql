CREATE TABLE IF NOT EXISTS shipments (
    id STRING,
    ship_ts STRING,
    order_id INT,
    warehouse STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'value.format' = 'json-registry',
    'timestamp' = 'ship_ts',
    'timestamp.format' = 'yyyy-MM-dd''T''HH:mm:ssX',
    'partitions' = '4'
);