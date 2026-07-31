# Customer 360 demonstration in Spark SQL 

This directory contains the Spark SQLs, test scripts and a validation tool to ensure the Spark SQLs are syntactically correct and executable.

## Spark primer (for beginners)

### What is PySpark?

**Apache Spark** is a distributed compute engine for large datasets. You write transformations (SQL or DataFrame APIs); Spark builds a plan and executes it across workers.

**PySpark** is the Python API. In this demo you mostly use:

1. `SparkSession` — entry point (starts a local Spark “app”)
2. DataFrames / temp views — in-memory tables created from Python sample rows
3. `spark.sql("...")` — run Spark SQL against those views

You do **not** need a cluster here: one process on your laptop runs driver + executors.

### Execution mode in this project

| Setting | Value here | Meaning |
|---------|------------|---------|
| Master | `local[*]` | All work on this machine; `*` = use all CPU cores |
| Tables | temp views | Created in `validate_spark_scripts.py`, not persisted to disk |
| SQL files | batch queries | Each `.sql` is one analytical `SELECT` (or CTE chain), run once |
| “Streaming” script | still batch | `src_streaming_aggregations.sql` uses `WINDOW(ts, '5 minutes')` for **time-bucketed** aggregations on a static sample — not a live Kafka/Structured Streaming job |

Mental model of `validate_spark_scripts.py`:

```text
JDK + PySpark
    → SparkSession (local[*])
    → create sample DataFrames → createOrReplaceTempView("...")
    → for each sources/src_*.sql: spark.sql(content).count() / show()
```

Spark is **lazy**: `spark.sql(...)` builds a plan; work runs when you call an action like `count()` or `show()`.

### Spark SQL constructs used in the demo

| Construct | What it does | Where to look |
|-----------|--------------|---------------|
| **CTEs** (`WITH ... AS`) | Named subqueries you can reuse in one statement | `src_customer_journey`, `src_set_operations`, `src_temporal_analytics`, `src_advanced_transformations` |
| **Joins** | Combine tables (`LEFT` / `INNER` / `CROSS`) | `src_customer_journey`, `src_set_operations` |
| **Aggregations** | `COUNT`, `SUM`, `AVG`, `STDDEV`, `GROUP BY` | Almost every script |
| **Window functions** (`OVER`) | Row-level metrics without collapsing groups: `LAG`, `RANK`, `NTILE`, `PERCENT_RANK`, running `SUM`/`AVG`, `COLLECT_LIST`/`COLLECT_SET` | `src_product_analytics`, `src_temporal_analytics`, `src_advanced_transformations`, `src_event_processing` |
| **Time windows** | `WINDOW(timestamp, '5 minutes')` buckets events by time | `src_streaming_aggregations` |
| **Set ops** | `UNION` / `UNION ALL`, `EXCEPT` | `src_set_operations` |
| **Pivot-style reshaping** | Conditional aggregates / growth vs prior periods | `src_sales_pivot` |
| **Complex types** | `STRUCT` / nested fields, `ARRAY` (`SIZE`, `ARRAY_CONTAINS`, `ARRAY_JOIN`), `MAP` (`MAP_KEYS`, `MAP_VALUES`) | `src_event_processing` |
| **JSON helpers** | `GET_JSON_OBJECT`, `from_json`-style patterns via string JSON columns | `src_event_processing`, `src_advanced_transformations` |
| **Time helpers** | `DATE_TRUNC`, `HOUR`/`MONTH`/`DAYOFWEEK`, `INTERVAL`, `DATEDIFF`, `CURRENT_TIMESTAMP` | `src_temporal_analytics`, filters across scripts |
| **Conditional logic** | `CASE WHEN`, `NULLIF` (avoid divide-by-zero) | Widespread |

**Spark gotchas this repo already accounts for:**

- `COUNT(DISTINCT x) OVER (...)` is **not** supported → use `SIZE(COLLECT_SET(x) OVER (...))` (see `src_advanced_transformations.sql`).
- `EXPLODE` is a generator; it generally cannot sit next to normal columns in the same `SELECT` without a lateral/explode pattern.
- Filters like `CURRENT_TIMESTAMP - INTERVAL n DAYS` only return rows if sample timestamps are recent (the validator builds relative dates).

## Prerequisites

Install PySpark (creates `.venv` and installs deps):

```bash
uv sync
```

PySpark also needs a **JDK 17+**. On macOS with Homebrew:

```bash
brew install openjdk@17
export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
```

(`validate_spark_scripts.py` will set `JAVA_HOME` to that path automatically if unset.)

## Directory Structure

```
c360/
├── sources/                         # Analytical Spark SQL demos (windows, CTEs, struct...)
│   ├── src_product_analytics.sql
│   ├── src_customer_journey.sql
│   ├── src_event_processing.sql
│   ├── src_sales_pivot.sql
│   ├── src_streaming_aggregations.sql
│   ├── src_set_operations.sql
│   ├── src_temporal_analytics.sql
│   ├── src_advanced_transformations.sql
│   ├── tables/                      # Delta-style DDL + seed data (not run by validator)
│   └── users/
├── tests/                           # unittest helpers + customer-metrics tests
├── validate_spark_scripts.py        # Local Spark runner / syntax+exec check
├── pyproject.toml
└── README.md
```

## Spark SQL Test Scripts

The test scripts cover various Spark SQL features:

1. **`src_product_analytics.sql`** - Window functions, time-based aggregations
2. **`src_customer_journey.sql`** - Complex CTEs, multiple joins, segmentation
3. **`src_event_processing.sql`** - Array/struct operations, JSON parsing
4. **`src_sales_pivot.sql`** - Pivot operations, growth calculations
5. **`src_streaming_aggregations.sql`** - Time windows, real-time analytics
6. **`src_set_operations.sql`** - Set operations (UNION, EXCEPT), subqueries
7. **`src_temporal_analytics.sql`** - Time series analysis, seasonality
8. **`src_advanced_transformations.sql`** - UDF-like operations, ML features

## Running the Validation

### Validate All Scripts

```bash
uv run python validate_spark_scripts.py
```

### Expected Output

```
🚀 Starting Spark SQL Script Validation
==================================================
Creating sample data...
✓ Sample data created successfully

Found 8 SQL scripts to validate:
  - src_product_analytics.sql
  - src_customer_journey.sql
  - ...

🔍 Validating: src_product_analytics.sql
  Executing: src_product_analytics.sql
    ✓ Query executed successfully, returned 5 rows

...

==================================================
📊 VALIDATION SUMMARY
==================================================
Total Scripts: 8
✅ Successful: 8
❌ Failed: 0
Success Rate: 100.0%

✅ SUCCESSFUL SCRIPTS:
  - src_advanced_transformations.sql: 3 rows
  - src_sales_pivot.sql: 0 rows
  - src_temporal_analytics.sql: 6 rows
  - src_streaming_aggregations.sql: 3 rows
  - src_set_operations.sql: 6 rows
  - src_product_analytics.sql: 2 rows
  - src_customer_journey.sql: 3 rows
  - src_event_processing.sql: 1 rows

🎉 All scripts validated successfully!
```

## What the Validator Does

1. **Sets up a local Spark session** with optimized configurations
2. **Creates sample data** for all tables referenced in the SQL scripts:
   - `raw_product_events`
   - `web_events`, `customer_profiles`, `purchases`
   - `raw_events`
   - `sales_data`
   - `streaming_events`
   - `user_activities`, `feature_usage`
   - `user_events`
   - `raw_transactions`

3. **Executes each SQL script** as a Spark job
4. **Reports results** including:
   - Success/failure status
   - Row counts returned
   - Detailed error messages for failures
   - Overall success rate

## Troubleshooting

### PySpark Not Installed
```
ERROR: PySpark not installed. Please run: pip install pyspark
```
**Solution:** Install PySpark using `pip install pyspark`

### Java Not Found
```
JAVA_HOME is not set
```
**Solution:** Install Java 11+ and set JAVA_HOME environment variable

### Memory Issues
If you encounter memory errors, you can adjust Spark configurations by modifying the `_create_spark_session` method in `validate_spark_scripts.py`:

```python
.config("spark.driver.memory", "2g") \
.config("spark.executor.memory", "2g") \
```


## Adding New Test Scripts

1. Create a new `.sql` file in the `sources/` directory with prefix `src_`
2. Ensure it references existing sample tables or add new sample data in `validate_spark_scripts.py`
3. Run the validator to ensure it works