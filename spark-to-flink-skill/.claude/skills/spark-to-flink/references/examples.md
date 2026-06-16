# Worked examples (c360 golden pairs)

Golden Flink output lives in `flink_project_demos/customer_360/c360_flink_processing/`. Spark input in `c360_spark_processing/`.

## src_customers

Spark: `c360_spark_processing/sources/src_customers.sql`

- Spark creates CSV temp view + deduplicated `src_customers` view
- Flink reads `customers_raw` Kafka topic, dedupes on `customer_id` with `ROW_NUMBER() OVER (... ORDER BY $rowtime DESC)`
- `DATEDIFF` age calculations become `TIMESTAMPDIFF(YEAR/DAY, ...)`
- DDL: `src_c360_customers` with PK `customer_id`, `changelog.mode = upsert`

Golden:

- `pipelines/sources/c360/src_customers/sql-scripts/ddl.src_c360_customers.sql`
- `pipelines/sources/c360/src_customers/sql-scripts/dml.src_c360_customers.sql`

## src_loyalty_program

Spark: `c360_spark_processing/sources/src_loyalty_program.sql`

- Simple SELECT with derived tier_rank, value_segment, redemption_rate
- `DATEDIFF(CURRENT_DATE(), tier_start_date)` → `TIMESTAMPDIFF(DAY, ...)`

Golden:

- `pipelines/sources/c360/src_loyalty_program/sql-scripts/ddl.src_c360_loyalty_program.sql`
- `pipelines/sources/c360/src_loyalty_program/sql-scripts/dml.src_c360_loyalty_program.sql`

## fct_customer_360_profile

Spark: `c360_spark_processing/facts/fct_customer_360_profile.sql`

- Multi-CTE join across support, app usage, loyalty, transactions
- Complex fact table with surrogate key on `customer_id`

Golden:

- `pipelines/facts/c360/fct_customer_360_profile/sql-scripts/ddl.c360_fct_customer_profile.sql`
- `pipelines/facts/c360/fct_customer_360_profile/sql-scripts/dml.c360_fct_customer_profile.sql`
