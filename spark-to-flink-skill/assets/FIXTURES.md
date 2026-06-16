# Fixture paths

Paths are relative to `Code/` (sibling repos: `research/` and `flink_project_demos/`).

## C360 golden pairs

| Name | Spark input | Flink DDL | Flink DML |
|------|-------------|-----------|-----------|
| src_customers | `flink_project_demos/customer_360/c360_spark_processing/sources/src_customers.sql` | `flink_project_demos/customer_360/c360_flink_processing/pipelines/sources/c360/src_customers/sql-scripts/ddl.src_c360_customers.sql` | `.../dml.src_c360_customers.sql` |
| src_loyalty_program | `.../sources/src_loyalty_program.sql` | `.../src_loyalty_program/sql-scripts/ddl.src_c360_loyalty_program.sql` | `.../dml.src_c360_loyalty_program.sql` |
| src_transactions | `.../sources/src_transactions.sql` | `.../src_transactions/sql-scripts/` | |
| dim_customer_transactions | `.../intermediates/int_customer_transactions.sql` | `.../dim_customer_transactions/sql-scripts/` | |
| fct_customer_360_profile | `.../facts/fct_customer_360_profile.sql` | `.../fct_customer_360_profile/sql-scripts/` | |

## Feature corpus (optional)

`../../flink_project_demos/spark-project/sources/src_*.sql` — rich Spark SQL patterns for manual testing. Validate Spark syntax with `spark-project/tests/validate_spark_scripts.py` before migration experiments.
