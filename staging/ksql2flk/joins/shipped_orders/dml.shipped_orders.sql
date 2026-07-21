INSERT INTO shipped_orders
SELECT o.id AS order_id,
       DATE_FORMAT(o.rowtime, 'yyyy-MM-dd HH:mm:ss') AS order_ts,
       o.total_amount,
       o.customer_name,
       s.id AS shipment_id,
       DATE_FORMAT(s.rowtime, 'yyyy-MM-dd HH:mm:ss') AS shipment_ts,
       s.warehouse,
       ((CAST(s.rowtime AS BIGINT) - CAST(o.rowtime AS BIGINT)) / 1000 / 60) AS ship_time
FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(rowtime))) o
LATERAL VIEW TUMBLE(TABLE shipments, DESCRIPTOR(rowtime), INTERVAL '2' SECOND) AS s
WHERE o.id = s.order_id
EMIT CHANGES;