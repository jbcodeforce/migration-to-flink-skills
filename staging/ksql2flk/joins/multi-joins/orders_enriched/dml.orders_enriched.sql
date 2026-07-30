INSERT INTO orders_enriched
SELECT customers.customer_id AS customer_id, customers.customer_name AS customer_name,
       orders.order_id, orders.purchase_date,
       items.item_id, items.item_name
FROM orders
LEFT JOIN customers ON orders.customer_id = customers.customer_id
LEFT JOIN items ON orders.item_id = items.item_id;