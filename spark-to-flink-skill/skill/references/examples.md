# Worked examples (c360 golden pairs)

Spark SQL Input
```
WITH user_data AS (
  SELECT id, name, created_at,
         surrogate_key(id, tenant_id) as user_key
  FROM users
  WHERE created_at >= current_timestamp() - INTERVAL 1 DAY
)
SELECT * FROM user_data;
```

Flink SQL Output:
```
INSERT INTO target_table
WITH user_data AS (
  SELECT id,
         name,
         created_at,
         MD5(CONCAT_WS(',', id, tenant_id)) as user_key
  FROM users
  WHERE created_at >= PROCTIME() - INTERVAL '1' DAY
)
SELECT id,
       name,
       created_at,
       user_key
FROM user_data;
```
