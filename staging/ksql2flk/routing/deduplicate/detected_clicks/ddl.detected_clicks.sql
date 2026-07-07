CREATE TABLE IF NOT EXISTS detected_clicks (
    ip_address STRING,
    url STRING,
    ip_count BIGINT,
    timestamp STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    PRIMARY KEY (ip_address, url) NOT ENFORCED
) WITH (
    'changelog.mode' = 'upsert',
    'value.format' = 'json-registry'
);