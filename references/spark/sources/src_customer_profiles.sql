-- src_customer_profiles: Source table for customer profile data
-- This table contains customer demographic and profile information

CREATE TABLE IF NOT EXISTS src_customer_profiles (
    customer_id BIGINT NOT NULL,
    first_name STRING,
    last_name STRING,
    email STRING NOT NULL,
    phone_number STRING,
    date_of_birth DATE,
    age_group STRING,
    gender STRING,
    location STRING,
    address_line1 STRING,
    address_line2 STRING,
    city STRING,
    state_province STRING,
    postal_code STRING,
    country STRING,
    membership_tier STRING,
    registration_date DATE NOT NULL,
    email_verified BOOLEAN,
    phone_verified BOOLEAN,
    marketing_opt_in BOOLEAN,
    preferred_language STRING,
    timezone STRING,
    account_status STRING,
    last_profile_update TIMESTAMP,
    created_date TIMESTAMP,
    updated_date TIMESTAMP
) 
USING DELTA
PARTITIONED BY (DATE_TRUNC('MONTH', registration_date))
TBLPROPERTIES (
    'description' = 'Customer profile and demographic data',
    'quality.expectations.customer_id.not_null' = 'true',
    'quality.expectations.email.not_null' = 'true',
    'quality.expectations.email.format' = 'email',
    'quality.expectations.registration_date.not_null' = 'true',
    'quality.expectations.membership_tier.values' = 'bronze,silver,gold,platinum,diamond',
    'quality.expectations.account_status.values' = 'active,inactive,suspended,closed'
);

-- Sample data insertion for testing
INSERT INTO src_customer_profiles (
    customer_id,
    first_name,
    last_name,
    email,
    phone_number,
    date_of_birth,
    age_group,
    gender,
    location,
    address_line1,
    city,
    state_province,
    postal_code,
    country,
    membership_tier,
    registration_date,
    email_verified,
    phone_verified,
    marketing_opt_in,
    preferred_language,
    timezone,
    account_status,
    last_profile_update,
    created_date,
    updated_date
) VALUES 
(1001, 'John', 'Admin', 'admin@company.com', '+1-555-0101', '1985-03-15', '35-44', 'M', 'San Francisco, CA', '123 Admin St', 'San Francisco', 'CA', '94102', 'US', 'platinum', '2022-01-15', true, true, true, 'en-US', 'America/New_York', 'active', current_timestamp(), current_timestamp(), current_timestamp()),
(1002, 'Jane', 'Doe', 'jane.doe@company.com', '+1-555-0102', '1990-07-22', '25-34', 'F', 'Los Angeles, CA', '456 User Ave', 'Los Angeles', 'CA', '90210', 'US', 'gold', '2022-03-20', true, true, true, 'en-US', 'America/Los_Angeles', 'active', current_timestamp(), current_timestamp(), current_timestamp()),
(1003, 'Bob', 'Power', 'power.user@company.com', '+1-555-0103', '1988-11-08', '35-44', 'M', 'Chicago, IL', '789 Power Blvd', 'Chicago', 'IL', '60601', 'US', 'silver', '2022-05-10', true, false, true, 'en-US', 'America/Chicago', 'active', current_timestamp(), current_timestamp(), current_timestamp()),
(1004, 'Alice', 'Standard', 'user@company.com', '+1-555-0104', '1995-02-14', '25-34', 'F', 'Denver, CO', '321 Standard Rd', 'Denver', 'CO', '80201', 'US', 'bronze', '2023-01-05', true, true, false, 'en-US', 'America/Denver', 'active', current_timestamp(), current_timestamp(), current_timestamp()),
(1005, 'Charlie', 'Beta', 'beta@company.com', '+1-555-0105', '1992-09-30', '25-34', 'M', 'Toronto, ON', '654 Beta Way', 'Toronto', 'ON', 'M5V 3A8', 'CA', 'gold', '2023-02-01', true, true, true, 'en-CA', 'America/Toronto', 'active', current_timestamp(), current_timestamp(), current_timestamp()),
(1006, 'Diana', 'Guest', 'guest@external.com', '+44-20-7946-0958', '1987-12-12', '35-44', 'F', 'London, UK', '987 Guest Lane', 'London', 'London', 'SW1A 1AA', 'UK', 'bronze', '2023-11-01', false, false, false, 'en-GB', 'Europe/London', 'active', current_timestamp(), current_timestamp(), current_timestamp()),
(1007, 'Eva', 'Frequent', 'frequent@company.com', '+49-30-12345678', '1983-05-25', '35-44', 'F', 'Berlin, Germany', '147 Frequent Str', 'Berlin', 'Berlin', '10115', 'DE', 'diamond', '2022-08-15', true, true, true, 'de-DE', 'Europe/Berlin', 'active', current_timestamp(), current_timestamp(), current_timestamp()),
(1008, 'Frank', 'Mobile', 'mobile@company.com', '+81-3-1234-5678', '1991-04-18', '25-34', 'M', 'Tokyo, Japan', '258 Mobile Ave', 'Tokyo', 'Tokyo', '100-0001', 'JP', 'silver', '2023-06-20', true, true, false, 'ja-JP', 'Asia/Tokyo', 'active', current_timestamp(), current_timestamp(), current_timestamp()),
(1009, 'Grace', 'Weekend', 'weekend@company.com', '+61-2-9876-5432', '1989-08-07', '35-44', 'F', 'Sydney, Australia', '369 Weekend Dr', 'Sydney', 'NSW', '2000', 'AU', 'gold', '2023-04-10', true, false, true, 'en-AU', 'Australia/Sydney', 'active', current_timestamp(), current_timestamp(), current_timestamp()),
(1010, 'Henry', 'Night', 'night@company.com', '+33-1-23-45-67-89', '1986-01-03', '35-44', 'M', 'Paris, France', '741 Night Blvd', 'Paris', 'Île-de-France', '75001', 'FR', 'platinum', '2022-12-01', true, true, true, 'fr-FR', 'Europe/Paris', 'active', current_timestamp(), current_timestamp(), current_timestamp());