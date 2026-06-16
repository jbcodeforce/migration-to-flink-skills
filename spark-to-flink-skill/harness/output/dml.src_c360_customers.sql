INSERT INTO src_c360_customers
SELECT * FROM src_c360_customers;

INSERT INTO src_c360_customers
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    date_of_birth,
    gender,
    registration_date,
    customer_segment,
    preferred_channel,
    address_line1,
    city,
    `STATE`,
    zip_code,
    country,
    TIMESTAMPDIFF(DAY, CAST(date_of_birth AS TIMESTAMP_LTZ(3)), CURRENT_DATE) / 365.25 AS age_years,
    TIMESTAMPDIFF(DAY, CAST(registration_date AS TIMESTAMP_LTZ(3)), CURRENT_DATE) AS days_since_registration,
    CASE
        WHEN TIMESTAMPDIFF(DAY, CAST(date_of_birth AS TIMESTAMP_LTZ(3)), CURRENT_DATE) / 365.25 < 25 THEN 'Gen Z'
        WHEN TIMESTAMPDIFF(DAY, CAST(date_of_birth AS TIMESTAMP_LTZ(3)), CURRENT_DATE) / 365.25 < 40 THEN 'Millennial'
        WHEN TIMESTAMPDIFF(DAY, CAST(date_of_birth AS TIMESTAMP_LTZ(3)), CURRENT_DATE) / 365.25 < 55 THEN 'Gen X'
        ELSE 'Boomer+'
    END AS generation_segment,
    CASE
        WHEN email IS NULL OR email = '' THEN 1
        ELSE 0
    END AS missing_email_flag,
    CASE
        WHEN phone IS NULL OR phone = '' THEN 1
        ELSE 0
    END AS missing_phone_flag
FROM (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        date_of_birth,
        gender,
        registration_date,
        customer_segment,
        preferred_channel,
        address_line1,
        city,
        `STATE`,
        zip_code,
        country,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY CAST(registration_date AS TIMESTAMP) DESC
        ) AS row_num
    FROM customers_raw
    WHERE customer_id IS NOT NULL
) ranked_customers
WHERE row_num = 1;