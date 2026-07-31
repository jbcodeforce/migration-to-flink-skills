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