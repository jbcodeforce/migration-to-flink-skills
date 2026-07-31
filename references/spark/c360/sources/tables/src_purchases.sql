-- src_purchases: Source table for purchase transaction data
-- This table contains purchase history and transaction information

CREATE TABLE IF NOT EXISTS src_purchases (
    purchase_id BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,
    order_id STRING NOT NULL,
    product_id STRING,
    product_name STRING,
    category STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    amount DECIMAL(10,2) NOT NULL,
    discount_amount DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    currency_code STRING,
    payment_method STRING,
    payment_status STRING,
    purchase_date DATE NOT NULL,
    purchase_timestamp TIMESTAMP NOT NULL,
    shipping_address STRING,
    billing_address STRING,
    promotion_code STRING,
    sales_channel STRING,
    store_location STRING,
    sales_rep_id BIGINT,
    refund_amount DECIMAL(10,2),
    refund_date DATE,
    return_reason STRING,
    customer_satisfaction_score INT,
    created_date TIMESTAMP,
    updated_date TIMESTAMP
)
USING DELTA
PARTITIONED BY (DATE_TRUNC('MONTH', purchase_date))
TBLPROPERTIES (
    'description' = 'Purchase transaction and order data',
    'quality.expectations.purchase_id.not_null' = 'true',
    'quality.expectations.customer_id.not_null' = 'true',
    'quality.expectations.order_id.not_null' = 'true',
    'quality.expectations.amount.not_null' = 'true',
    'quality.expectations.purchase_date.not_null' = 'true',
    'quality.expectations.purchase_timestamp.not_null' = 'true',
    'quality.expectations.payment_status.values' = 'pending,completed,failed,refunded,cancelled',
    'quality.expectations.sales_channel.values' = 'online,retail,mobile,phone,partner'
);

-- Sample data insertion for testing (covering last 90 days as referenced in fact table)
INSERT INTO src_purchases (
    purchase_id,
    customer_id,
    order_id,
    product_id,
    product_name,
    category,
    quantity,
    unit_price,
    amount,
    discount_amount,
    tax_amount,
    total_amount,
    currency_code,
    payment_method,
    payment_status,
    purchase_date,
    purchase_timestamp,
    shipping_address,
    billing_address,
    promotion_code,
    sales_channel,
    store_location,
    sales_rep_id,
    customer_satisfaction_score,
    created_date,
    updated_date
) VALUES
-- Customer 1001 purchases
(200001, 1001, 'ORD-2024-001001', 'PROD_123', 'Winter Jacket Pro', 'clothing', 1, 299.99, 299.99, 30.00, 24.00, 293.99, 'USD', 'credit_card', 'completed', CURRENT_DATE - INTERVAL 2 DAYS, CURRENT_TIMESTAMP - INTERVAL 2 DAYS, '123 Admin St, San Francisco, CA 94102', '123 Admin St, San Francisco, CA 94102', 'WINTER20', 'online', 'web', NULL, 5, current_timestamp(), current_timestamp()),
(200002, 1001, 'ORD-2024-001002', 'PROD_456', 'Smart Watch X1', 'electronics', 1, 199.99, 199.99, 0.00, 16.00, 215.99, 'USD', 'credit_card', 'completed', CURRENT_DATE - INTERVAL 15 DAYS, CURRENT_TIMESTAMP - INTERVAL 15 DAYS, '123 Admin St, San Francisco, CA 94102', '123 Admin St, San Francisco, CA 94102', NULL, 'online', 'web', NULL, 4, current_timestamp(), current_timestamp()),
(200003, 1001, 'ORD-2024-001003', 'PROD_789', 'Wireless Headphones', 'electronics', 2, 89.99, 179.98, 18.00, 12.96, 174.94, 'USD', 'paypal', 'completed', CURRENT_DATE - INTERVAL 30 DAYS, CURRENT_TIMESTAMP - INTERVAL 30 DAYS, '123 Admin St, San Francisco, CA 94102', '123 Admin St, San Francisco, CA 94102', 'EARLY10', 'online', 'web', NULL, 5, current_timestamp(), current_timestamp()),

-- Customer 1002 purchases  
(200004, 1002, 'ORD-2024-002001', 'PROD_321', 'Programming Guide', 'books', 1, 49.99, 49.99, 5.00, 3.60, 48.59, 'USD', 'credit_card', 'completed', CURRENT_DATE - INTERVAL 5 DAYS, CURRENT_TIMESTAMP - INTERVAL 5 DAYS, '456 User Ave, Los Angeles, CA 90210', '456 User Ave, Los Angeles, CA 90210', 'BOOK10', 'online', 'web', NULL, 4, current_timestamp(), current_timestamp()),
(200005, 1002, 'ORD-2024-002002', 'PROD_654', 'Phone Case Premium', 'accessories', 1, 29.99, 29.99, 0.00, 2.40, 32.39, 'USD', 'apple_pay', 'completed', CURRENT_DATE - INTERVAL 20 DAYS, CURRENT_TIMESTAMP - INTERVAL 20 DAYS, '456 User Ave, Los Angeles, CA 90210', '456 User Ave, Los Angeles, CA 90210', NULL, 'mobile', 'mobile_app', NULL, 5, current_timestamp(), current_timestamp()),

-- Customer 1003 purchases
(200006, 1003, 'ORD-2024-003001', 'PROD_789', 'Gaming Laptop Pro', 'electronics', 1, 1299.99, 1299.99, 100.00, 95.99, 1295.98, 'USD', 'credit_card', 'completed', CURRENT_DATE - INTERVAL 1 DAYS, CURRENT_TIMESTAMP - INTERVAL 1 DAYS, '789 Power Blvd, Chicago, IL 60601', '789 Power Blvd, Chicago, IL 60601', 'GAMING15', 'online', 'web', NULL, 5, current_timestamp(), current_timestamp()),
(200007, 1003, 'ORD-2024-003002', 'PROD_111', 'Mechanical Keyboard', 'electronics', 1, 149.99, 149.99, 0.00, 12.00, 161.99, 'USD', 'credit_card', 'completed', CURRENT_DATE - INTERVAL 10 DAYS, CURRENT_TIMESTAMP - INTERVAL 10 DAYS, '789 Power Blvd, Chicago, IL 60601', '789 Power Blvd, Chicago, IL 60601', NULL, 'online', 'web', NULL, 4, current_timestamp(), current_timestamp()),
(200008, 1003, 'ORD-2024-003003', 'PROD_222', 'Gaming Mouse', 'electronics', 1, 79.99, 79.99, 8.00, 5.76, 77.75, 'USD', 'paypal', 'completed', CURRENT_DATE - INTERVAL 25 DAYS, CURRENT_TIMESTAMP - INTERVAL 25 DAYS, '789 Power Blvd, Chicago, IL 60601', '789 Power Blvd, Chicago, IL 60601', 'MOUSE10', 'online', 'web', NULL, 4, current_timestamp(), current_timestamp()),

-- Customer 1005 purchases
(200009, 1005, 'ORD-2024-005001', 'PROD_654', 'Phone Case Premium', 'accessories', 1, 29.99, 29.99, 0.00, 3.90, 33.89, 'CAD', 'credit_card', 'completed', CURRENT_DATE - INTERVAL 5 DAYS, CURRENT_TIMESTAMP - INTERVAL 5 DAYS, '654 Beta Way, Toronto, ON M5V 3A8', '654 Beta Way, Toronto, ON M5V 3A8', NULL, 'mobile', 'mobile_app', NULL, 5, current_timestamp(), current_timestamp()),
(200010, 1005, 'ORD-2024-005002', 'PROD_333', 'Bluetooth Speaker', 'electronics', 1, 129.99, 129.99, 13.00, 15.21, 132.20, 'CAD', 'debit_card', 'completed', CURRENT_DATE - INTERVAL 18 DAYS, CURRENT_TIMESTAMP - INTERVAL 18 DAYS, '654 Beta Way, Toronto, ON M5V 3A8', '654 Beta Way, Toronto, ON M5V 3A8', 'AUDIO10', 'online', 'web', NULL, 4, current_timestamp(), current_timestamp()),

-- Customer 1007 purchases (frequent buyer)
(200011, 1007, 'ORD-2024-007001', 'PROD_444', 'Luxury Watch', 'accessories', 1, 899.99, 899.99, 90.00, 64.79, 874.78, 'EUR', 'bank_transfer', 'completed', CURRENT_DATE - INTERVAL 3 DAYS, CURRENT_TIMESTAMP - INTERVAL 3 DAYS, '147 Frequent Str, Berlin, Berlin 10115', '147 Frequent Str, Berlin, Berlin 10115', 'VIP10', 'online', 'web', NULL, 5, current_timestamp(), current_timestamp()),
(200012, 1007, 'ORD-2024-007002', 'PROD_555', 'Designer Handbag', 'fashion', 1, 599.99, 599.99, 60.00, 43.19, 583.18, 'EUR', 'credit_card', 'completed', CURRENT_DATE - INTERVAL 8 DAYS, CURRENT_TIMESTAMP - INTERVAL 8 DAYS, '147 Frequent Str, Berlin, Berlin 10115', '147 Frequent Str, Berlin, Berlin 10115', 'FASHION10', 'online', 'web', NULL, 5, current_timestamp(), current_timestamp()),
(200013, 1007, 'ORD-2024-007003', 'PROD_666', 'Premium Skincare Set', 'beauty', 1, 199.99, 199.99, 20.00, 14.39, 194.38, 'EUR', 'paypal', 'completed', CURRENT_DATE - INTERVAL 12 DAYS, CURRENT_TIMESTAMP - INTERVAL 12 DAYS, '147 Frequent Str, Berlin, Berlin 10115', '147 Frequent Str, Berlin, Berlin 10115', 'BEAUTY10', 'online', 'web', NULL, 4, current_timestamp(), current_timestamp()),
(200014, 1007, 'ORD-2024-007004', 'PROD_777', 'Professional Camera', 'electronics', 1, 1599.99, 1599.99, 160.00, 115.19, 1555.18, 'EUR', 'credit_card', 'completed', CURRENT_DATE - INTERVAL 22 DAYS, CURRENT_TIMESTAMP - INTERVAL 22 DAYS, '147 Frequent Str, Berlin, Berlin 10115', '147 Frequent Str, Berlin, Berlin 10115', 'PHOTO15', 'online', 'web', NULL, 5, current_timestamp(), current_timestamp()),
(200015, 1007, 'ORD-2024-007005', 'PROD_888', 'Fitness Tracker', 'electronics', 2, 149.99, 299.98, 30.00, 19.43, 289.41, 'EUR', 'apple_pay', 'completed', CURRENT_DATE - INTERVAL 35 DAYS, CURRENT_TIMESTAMP - INTERVAL 35 DAYS, '147 Frequent Str, Berlin, Berlin 10115', '147 Frequent Str, Berlin, Berlin 10115', 'FITNESS20', 'online', 'web', NULL, 4, current_timestamp(), current_timestamp()),

-- Customer 1010 purchases  
(200016, 1010, 'ORD-2024-010001', 'PROD_999', 'Wine Collection', 'beverages', 6, 49.99, 299.94, 30.00, 19.67, 289.61, 'EUR', 'bank_transfer', 'completed', CURRENT_DATE - INTERVAL 7 DAYS, CURRENT_TIMESTAMP - INTERVAL 7 DAYS, '741 Night Blvd, Paris, Île-de-France 75001', '741 Night Blvd, Paris, Île-de-France 75001', 'WINE10', 'online', 'web', NULL, 5, current_timestamp(), current_timestamp()),
(200017, 1010, 'ORD-2024-010002', 'PROD_101', 'French Perfume', 'beauty', 1, 249.99, 249.99, 25.00, 16.19, 241.18, 'EUR', 'credit_card', 'completed', CURRENT_DATE - INTERVAL 28 DAYS, CURRENT_TIMESTAMP - INTERVAL 28 DAYS, '741 Night Blvd, Paris, Île-de-France 75001', '741 Night Blvd, Paris, Île-de-France 75001', 'FRAGRANCE10', 'online', 'web', NULL, 5, current_timestamp(), current_timestamp()),

-- Some older purchases outside 90-day window (should not appear in fact table aggregations)
(200018, 1001, 'ORD-2023-001004', 'PROD_OLD', 'Old Product', 'misc', 1, 99.99, 99.99, 0.00, 8.00, 107.99, 'USD', 'credit_card', 'completed', CURRENT_DATE - INTERVAL 120 DAYS, CURRENT_TIMESTAMP - INTERVAL 120 DAYS, '123 Admin St, San Francisco, CA 94102', '123 Admin St, San Francisco, CA 94102', NULL, 'online', 'web', NULL, 3, current_timestamp(), current_timestamp()),
(200019, 1002, 'ORD-2023-002003', 'PROD_OLD2', 'Another Old Product', 'misc', 1, 149.99, 149.99, 0.00, 12.00, 161.99, 'USD', 'paypal', 'completed', CURRENT_DATE - INTERVAL 100 DAYS, CURRENT_TIMESTAMP - INTERVAL 100 DAYS, '456 User Ave, Los Angeles, CA 90210', '456 User Ave, Los Angeles, CA 90210', NULL, 'online', 'web', NULL, 4, current_timestamp(), current_timestamp());