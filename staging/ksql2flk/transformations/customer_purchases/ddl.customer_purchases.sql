CREATE TABLE IF NOT EXISTS customer_purchases (
    id STRING,
    current_purchase DOUBLE,
    previous_purchase DOUBLE,
    txn_ts STRING,
    first_name STRING,
    last_name STRING,
    _host STRING,
    SCHEMA '<json>'
) PARTITIONED BY (id)
WITH (
    'value.format' = 'json-registry',
    'partitions' = '1'
);