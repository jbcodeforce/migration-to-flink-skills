CREATE TABLE IF NOT EXISTS clicks (
    ip_address STRING,
    url STRING
) WITH (
    'value.format' = 'json-registry',
    'partitions' = '1'
)