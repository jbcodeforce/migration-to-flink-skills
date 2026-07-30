INSERT INTO shipped_orders
SELECT
    o.id AS order_id,
    TO_TIMESTAMP_LTZ(o.order_ts, 'yyyy-mm-dd hh:mm:ss') as order_ts,
    o.total_amount,
    o.customer_name,
    s.id AS shipment_id,
    TO_TIMESTAMP_LTZ(s.ship_ts, 'yyyy-mm-dd hh:mm:ss') AS shipment_ts,
    s.warehouse,
    TIMESTAMPDIFF(MINUTE, TO_TIMESTAMP_LTZ(o.order_ts, 'yyyy-mm-dd hh:mm:ss'), TO_TIMESTAMP_LTZ(s.ship_ts, 'yyyy-mm-dd hh:mm:ss')) AS ship_time
FROM orders AS o
JOIN shipments AS s
ON o.id = s.order_id;