INSERT INTO detected_clicks
SELECT
    ip_address,
    url,
    COUNT(ip_address) AS ip_count,
    DATE_FORMAT(MIN($rowtime), 'yyyy-MM-dd HH:mm:ss.SSS') AS timestamp
FROM TABLE(TUMBLE(TABLE clicks, DESCRIPTOR($rowtime), INTERVAL '2' MINUTE))
GROUP BY ip_address, url;