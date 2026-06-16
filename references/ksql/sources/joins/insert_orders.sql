INSERT INTO orders (id, order_ts, total_amount, customer_name) VALUES (1, '2024-09-29T06:01:18Z', 133.84, 'Danica Fine');
INSERT INTO orders (id, order_ts, total_amount, customer_name) VALUES (2, '2024-09-29T17:02:20Z', 164.31, 'Tim Berglund');
INSERT INTO orders (id, order_ts, total_amount, customer_name) VALUES (3, '2024-09-29T13:44:10Z', 90.66, 'Sandon Jacobs');
INSERT INTO orders (id, order_ts, total_amount, customer_name) VALUES (4, '2024-09-29T11:58:25Z', 33.11, 'Viktor Gamov');

INSERT INTO shipments (id, ship_ts, order_id, warehouse) VALUES ('ship-ch83360', '2024-09-30T18:13:39Z', 1, 'UPS');
INSERT INTO shipments (id, ship_ts, order_id, warehouse) VALUES ('ship-xf72808', '2024-09-30T02:04:13Z', 2, 'UPS');
INSERT INTO shipments (id, ship_ts, order_id, warehouse) VALUES ('ship-kr47454', '2024-09-30T20:47:09Z', 3, 'DHL');