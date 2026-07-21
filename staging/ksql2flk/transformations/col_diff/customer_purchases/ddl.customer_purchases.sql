CREATE TABLE IF NOT EXISTS customer_purchases
(id STRING,
 PRIMARY KEY (id) NOT ENFORCED)
DISTRIBUTED BY HASH(id) INTO 1 BUCKETS
WITH (
    'value.format' = 'json-registry',
    'scan.startup.mode' = 'earliest-offset'
);