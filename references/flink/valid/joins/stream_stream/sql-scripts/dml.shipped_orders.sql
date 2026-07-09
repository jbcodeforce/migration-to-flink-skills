INSERT INTO shipped_orders
SELECT
    o.id AS order_id,
    TIMESTAMPTOSTRING(o.rowtime, 'yyyy-MM-dd HH:mm:ss', 'UTC') AS order_ts,
    o.total_amount,
    o.customer_name,
    s.id AS shipment_id,
    TIMESTAMPTOSTRING(s.rowtime, 'yyyy-MM-dd HH:mm:ss', 'UTC') AS shipment_ts,
    s.warehouse,
    (s.$rowtime - o.$rowtime) / 1000 / 60 AS ship_time
FROM orders AS o
JOIN shipments AS s
ON o.id = s.order_id;