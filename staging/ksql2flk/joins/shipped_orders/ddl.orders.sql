CREATE TABLE IF NOT EXISTS orders (
    id INT,
    order_ts STRING,
    total_amount DOUBLE,
    customer_name STRING,
    PRIMARY KEY (id) NOT ENFORCED
) DISTRIBUTED BY HASH(id) INTO 4 BUCKETS
WITH (
    'value.format' = 'json-registry',
    'timestamp.format' = 'yyyy-MM-dd''T''HH:mm:ssX',
    'timestamp' = 'order_ts'
)