CREATE TABLE IF NOT EXISTS raw_values_clicks (
    ip_address STRING,
    url STRING
) WITH (
    'value.format' = 'json-registry',
    'changelog.mode' = 'append'
)