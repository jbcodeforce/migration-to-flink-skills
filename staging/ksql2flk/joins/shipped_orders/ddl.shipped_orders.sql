CREATE TABLE IF NOT EXISTS shipped_orders (
    order_id INT,
    order_ts STRING,
    total_amount DOUBLE,
    customer_name STRING,
    shipment_id STRING,
    shipment_ts STRING,
    warehouse STRING,
    ship_time DOUBLE
) WITH (
    'kafka.retention.time' = '0',
    'scan.bounded.mode' = 'unbounded'
);