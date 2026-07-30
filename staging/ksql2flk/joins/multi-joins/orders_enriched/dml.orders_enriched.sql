INSERT INTO orders_enriched
SELECT
    orders.customer_id AS customer_id,
    orders.order_id AS order_id,
    items.item_id AS item_id,
    customers.customer_name AS customer_name,
    orders.purchase_date AS purchase_date,
    items.item_name AS item_name
FROM orders
LEFT JOIN customers ON orders.customer_id = customers.customer_id
LEFT JOIN items ON orders.item_id = items.item_id;