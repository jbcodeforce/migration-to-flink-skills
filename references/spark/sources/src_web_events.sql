-- src_web_events: Source table for web event tracking data
-- This table contains raw web event data used for customer journey analysis and metrics

CREATE TABLE IF NOT EXISTS src_web_events (
    event_id BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,
    session_id STRING NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    event_type STRING NOT NULL,
    page_url STRING,
    page_title STRING,
    referrer_url STRING,
    user_agent STRING,
    ip_address STRING,
    device_type STRING,
    browser STRING,
    operating_system STRING,
    country STRING,
    region STRING,
    city STRING,
    utm_source STRING,
    utm_medium STRING,
    utm_campaign STRING,
    utm_content STRING,
    utm_term STRING,
    product_id STRING,
    category STRING,
    search_query STRING,
    session_duration_seconds BIGINT,
    page_load_time_ms INT,
    bounce_rate DOUBLE,
    conversion_value DECIMAL(10,2),
    created_date TIMESTAMP,
    updated_date TIMESTAMP
) 
USING DELTA
PARTITIONED BY (DATE_TRUNC('DAY', event_timestamp))
TBLPROPERTIES (
    'description' = 'Raw web events data from tracking systems',
    'quality.expectations.event_id.not_null' = 'true',
    'quality.expectations.customer_id.not_null' = 'true',
    'quality.expectations.session_id.not_null' = 'true',
    'quality.expectations.event_timestamp.not_null' = 'true',
    'quality.expectations.event_type.not_null' = 'true',
    'quality.expectations.event_type.values' = 'page_view,click,purchase,add_to_cart,remove_from_cart,search,login,logout,signup,download'
);

-- Sample data insertion for testing
INSERT INTO src_web_events (
    event_id,
    customer_id,
    session_id,
    event_timestamp,
    event_type,
    page_url,
    page_title,
    referrer_url,
    user_agent,
    ip_address,
    device_type,
    browser,
    operating_system,
    country,
    region,
    city,
    utm_source,
    utm_medium,
    utm_campaign,
    product_id,
    category,
    search_query,
    session_duration_seconds,
    page_load_time_ms,
    bounce_rate,
    conversion_value,
    created_date,
    updated_date
) VALUES 
-- Customer 1001 session events
(100001, 1001, 'sess_1001_001', CURRENT_TIMESTAMP - INTERVAL 2 HOURS, 'page_view', '/home', 'Homepage', 'https://google.com', 'Mozilla/5.0', '192.168.1.10', 'desktop', 'Chrome', 'Windows', 'US', 'CA', 'San Francisco', 'google', 'organic', 'winter_sale', NULL, 'homepage', NULL, 3600, 1200, 0.0, NULL, current_timestamp(), current_timestamp()),
(100002, 1001, 'sess_1001_001', CURRENT_TIMESTAMP - INTERVAL 2 HOURS + INTERVAL 30 SECONDS, 'click', '/products', 'Products', '/home', 'Mozilla/5.0', '192.168.1.10', 'desktop', 'Chrome', 'Windows', 'US', 'CA', 'San Francisco', 'google', 'organic', 'winter_sale', NULL, 'products', NULL, 3600, 800, 0.0, NULL, current_timestamp(), current_timestamp()),
(100003, 1001, 'sess_1001_001', CURRENT_TIMESTAMP - INTERVAL 2 HOURS + INTERVAL 5 MINUTES, 'search', '/search', 'Search Results', '/products', 'Mozilla/5.0', '192.168.1.10', 'desktop', 'Chrome', 'Windows', 'US', 'CA', 'San Francisco', 'google', 'organic', 'winter_sale', NULL, 'search', 'winter jackets', 3600, 950, 0.0, NULL, current_timestamp(), current_timestamp()),
(100004, 1001, 'sess_1001_001', CURRENT_TIMESTAMP - INTERVAL 2 HOURS + INTERVAL 8 MINUTES, 'page_view', '/product/123', 'Winter Jacket Pro', '/search', 'Mozilla/5.0', '192.168.1.10', 'desktop', 'Chrome', 'Windows', 'US', 'CA', 'San Francisco', 'google', 'organic', 'winter_sale', 'PROD_123', 'clothing', 'winter jackets', 3600, 1100, 0.0, NULL, current_timestamp(), current_timestamp()),
(100005, 1001, 'sess_1001_001', CURRENT_TIMESTAMP - INTERVAL 2 HOURS + INTERVAL 12 MINUTES, 'add_to_cart', '/product/123', 'Winter Jacket Pro', '/product/123', 'Mozilla/5.0', '192.168.1.10', 'desktop', 'Chrome', 'Windows', 'US', 'CA', 'San Francisco', 'google', 'organic', 'winter_sale', 'PROD_123', 'clothing', NULL, 3600, 500, 0.0, 299.99, current_timestamp(), current_timestamp()),
(100006, 1001, 'sess_1001_001', CURRENT_TIMESTAMP - INTERVAL 2 HOURS + INTERVAL 15 MINUTES, 'purchase', '/checkout/success', 'Purchase Complete', '/checkout', 'Mozilla/5.0', '192.168.1.10', 'desktop', 'Chrome', 'Windows', 'US', 'CA', 'San Francisco', 'google', 'organic', 'winter_sale', 'PROD_123', 'clothing', NULL, 3600, 750, 0.0, 299.99, current_timestamp(), current_timestamp()),

-- Customer 1002 session events
(100007, 1002, 'sess_1002_001', CURRENT_TIMESTAMP - INTERVAL 6 HOURS, 'page_view', '/home', 'Homepage', 'https://facebook.com', 'Chrome/120.0', '192.168.1.20', 'mobile', 'Chrome', 'Android', 'US', 'CA', 'Los Angeles', 'facebook', 'social', 'summer_promo', NULL, 'homepage', NULL, 1800, 2000, 0.0, NULL, current_timestamp(), current_timestamp()),
(100008, 1002, 'sess_1002_001', CURRENT_TIMESTAMP - INTERVAL 6 HOURS + INTERVAL 1 MINUTE, 'click', '/categories/electronics', 'Electronics', '/home', 'Chrome/120.0', '192.168.1.20', 'mobile', 'Chrome', 'Android', 'US', 'CA', 'Los Angeles', 'facebook', 'social', 'summer_promo', NULL, 'electronics', NULL, 1800, 1500, 0.0, NULL, current_timestamp(), current_timestamp()),
(100009, 1002, 'sess_1002_001', CURRENT_TIMESTAMP - INTERVAL 6 HOURS + INTERVAL 3 MINUTES, 'page_view', '/product/456', 'Smart Watch X1', '/categories/electronics', 'Chrome/120.0', '192.168.1.20', 'mobile', 'Chrome', 'Android', 'US', 'CA', 'Los Angeles', 'facebook', 'social', 'summer_promo', 'PROD_456', 'electronics', NULL, 1800, 1800, 0.0, NULL, current_timestamp(), current_timestamp()),

-- Customer 1003 session events  
(100010, 1003, 'sess_1003_001', CURRENT_TIMESTAMP - INTERVAL 1 DAY, 'page_view', '/home', 'Homepage', 'direct', 'Firefox/119.0', '192.168.1.30', 'desktop', 'Firefox', 'macOS', 'US', 'IL', 'Chicago', 'direct', 'direct', NULL, NULL, 'homepage', NULL, 2400, 900, 0.0, NULL, current_timestamp(), current_timestamp()),
(100011, 1003, 'sess_1003_001', CURRENT_TIMESTAMP - INTERVAL 1 DAY + INTERVAL 2 MINUTES, 'search', '/search', 'Search Results', '/home', 'Firefox/119.0', '192.168.1.30', 'desktop', 'Firefox', 'macOS', 'US', 'IL', 'Chicago', 'direct', 'direct', NULL, NULL, 'search', 'gaming laptop', 2400, 1100, 0.0, NULL, current_timestamp(), current_timestamp()),
(100012, 1003, 'sess_1003_001', CURRENT_TIMESTAMP - INTERVAL 1 DAY + INTERVAL 5 MINUTES, 'page_view', '/product/789', 'Gaming Laptop Pro', '/search', 'Firefox/119.0', '192.168.1.30', 'desktop', 'Firefox', 'macOS', 'US', 'IL', 'Chicago', 'direct', 'direct', NULL, 'PROD_789', 'electronics', 'gaming laptop', 2400, 1000, 0.0, NULL, current_timestamp(), current_timestamp()),
(100013, 1003, 'sess_1003_001', CURRENT_TIMESTAMP - INTERVAL 1 DAY + INTERVAL 10 MINUTES, 'add_to_cart', '/product/789', 'Gaming Laptop Pro', '/product/789', 'Firefox/119.0', '192.168.1.30', 'desktop', 'Firefox', 'macOS', 'US', 'IL', 'Chicago', 'direct', 'direct', NULL, 'PROD_789', 'electronics', NULL, 2400, 600, 0.0, 1299.99, current_timestamp(), current_timestamp()),
(100014, 1003, 'sess_1003_001', CURRENT_TIMESTAMP - INTERVAL 1 DAY + INTERVAL 15 MINUTES, 'purchase', '/checkout/success', 'Purchase Complete', '/checkout', 'Firefox/119.0', '192.168.1.30', 'desktop', 'Firefox', 'macOS', 'US', 'IL', 'Chicago', 'direct', 'direct', NULL, 'PROD_789', 'electronics', NULL, 2400, 800, 0.0, 1299.99, current_timestamp(), current_timestamp()),

-- Customer 1004 browsing session (no purchase)
(100015, 1004, 'sess_1004_001', CURRENT_TIMESTAMP - INTERVAL 3 DAYS, 'page_view', '/home', 'Homepage', 'https://bing.com', 'Safari/17.0', '192.168.1.40', 'desktop', 'Safari', 'macOS', 'US', 'CO', 'Denver', 'bing', 'organic', 'black_friday', NULL, 'homepage', NULL, 900, 1300, 0.8, NULL, current_timestamp(), current_timestamp()),
(100016, 1004, 'sess_1004_001', CURRENT_TIMESTAMP - INTERVAL 3 DAYS + INTERVAL 1 MINUTE, 'page_view', '/categories/books', 'Books', '/home', 'Safari/17.0', '192.168.1.40', 'desktop', 'Safari', 'macOS', 'US', 'CO', 'Denver', 'bing', 'organic', 'black_friday', NULL, 'books', NULL, 900, 1100, 0.8, NULL, current_timestamp(), current_timestamp()),
(100017, 1004, 'sess_1004_001', CURRENT_TIMESTAMP - INTERVAL 3 DAYS + INTERVAL 2 MINUTES, 'page_view', '/product/321', 'Programming Guide', '/categories/books', 'Safari/17.0', '192.168.1.40', 'desktop', 'Safari', 'macOS', 'US', 'CO', 'Denver', 'bing', 'organic', 'black_friday', 'PROD_321', 'books', NULL, 900, 950, 0.8, NULL, current_timestamp(), current_timestamp()),

-- Customer 1005 mobile session
(100018, 1005, 'sess_1005_001', CURRENT_TIMESTAMP - INTERVAL 5 HOURS, 'page_view', '/home', 'Homepage', 'https://twitter.com', 'Edge/119.0', '192.168.1.50', 'mobile', 'Edge', 'iOS', 'CA', 'ON', 'Toronto', 'twitter', 'social', 'holiday_deals', NULL, 'homepage', NULL, 600, 2500, 0.9, NULL, current_timestamp(), current_timestamp()),
(100019, 1005, 'sess_1005_001', CURRENT_TIMESTAMP - INTERVAL 5 HOURS + INTERVAL 30 SECONDS, 'click', '/sales', 'Current Sales', '/home', 'Edge/119.0', '192.168.1.50', 'mobile', 'Edge', 'iOS', 'CA', 'ON', 'Toronto', 'twitter', 'social', 'holiday_deals', NULL, 'sales', NULL, 600, 2200, 0.9, NULL, current_timestamp(), current_timestamp()),

-- Additional events for the last 7 days to match the fact table filter
(100020, 1001, 'sess_1001_002', CURRENT_TIMESTAMP - INTERVAL 1 DAY, 'page_view', '/home', 'Homepage', 'direct', 'Mozilla/5.0', '192.168.1.10', 'desktop', 'Chrome', 'Windows', 'US', 'CA', 'San Francisco', 'direct', 'direct', NULL, NULL, 'homepage', NULL, 1200, 800, 0.0, NULL, current_timestamp(), current_timestamp()),
(100021, 1002, 'sess_1002_002', CURRENT_TIMESTAMP - INTERVAL 2 DAYS, 'search', '/search', 'Search Results', '/home', 'Chrome/120.0', '192.168.1.20', 'mobile', 'Chrome', 'Android', 'US', 'CA', 'Los Angeles', 'google', 'organic', 'mobile_week', NULL, 'search', 'smartphone case', 900, 1600, 0.0, NULL, current_timestamp(), current_timestamp()),
(100022, 1003, 'sess_1003_002', CURRENT_TIMESTAMP - INTERVAL 3 DAYS, 'page_view', '/categories/accessories', 'Accessories', 'direct', 'Firefox/119.0', '192.168.1.30', 'desktop', 'Firefox', 'macOS', 'US', 'IL', 'Chicago', 'direct', 'direct', NULL, NULL, 'accessories', NULL, 1500, 750, 0.0, NULL, current_timestamp(), current_timestamp()),
(100023, 1004, 'sess_1004_002', CURRENT_TIMESTAMP - INTERVAL 4 DAYS, 'page_view', '/home', 'Homepage', 'https://reddit.com', 'Safari/17.0', '192.168.1.40', 'desktop', 'Safari', 'macOS', 'US', 'CO', 'Denver', 'reddit', 'social', 'weekend_deals', NULL, 'homepage', NULL, 300, 1200, 1.0, NULL, current_timestamp(), current_timestamp()),
(100024, 1005, 'sess_1005_002', CURRENT_TIMESTAMP - INTERVAL 5 DAYS, 'purchase', '/checkout/success', 'Purchase Complete', '/checkout', 'Edge/119.0', '192.168.1.50', 'mobile', 'Edge', 'iOS', 'CA', 'ON', 'Toronto', 'email', 'email', 'newsletter', 'PROD_654', 'accessories', NULL, 2100, 900, 0.0, 49.99, current_timestamp(), current_timestamp());