# Customer Metrics Fact Table Test Suite

This directory contains a comprehensive PySpark test suite for validating the `fct_customer_metrics` fact table and its supporting source tables.

## Overview

The test suite validates:
- **Source Tables**: `src_web_events`, `src_customer_profiles`, `src_purchases`
- **Fact Table**: `fct_customer_metrics` 
- **Business Logic**: Customer segmentation, session analysis, purchase aggregations
- **Data Quality**: Date filtering, metric calculations, aggregation logic

## Files

### Test Files
- `test_fct_customer_metrics.py` - Main test suite with comprehensive test cases
- `run_fct_customer_metrics_test.py` - Test runner script with dependency checking
- `TEST_CUSTOMER_METRICS.md` - This documentation file

### Source Tables (Dependencies)
- `sources/src_web_events.sql` - Web event tracking data
- `sources/src_customer_profiles.sql` - Customer profile and demographic data  
- `sources/src_purchases.sql` - Purchase transaction history
- `facts/cj/fct_customer_metrics.sql` - Customer metrics fact table

## Test Cases

### 1. Basic Execution Test
- **Purpose**: Ensures the fact table SQL executes without errors
- **Validates**: Query syntax, table joins, column presence
- **Expected**: Non-zero result set with all expected columns

### 2. Customer Segmentation Logic Test
- **Purpose**: Validates customer lifecycle segmentation
- **Validates**: Date-based segmentation (new/regular/veteran customers)
- **Expected**: Proper assignment of customer segments based on registration date

### 3. Session Metrics Calculation Test
- **Purpose**: Tests web event session aggregation
- **Validates**: Session-level metrics (events per session, unique pages, purchases)
- **Expected**: Accurate session-level aggregations from web events

### 4. Purchase History Aggregation Test
- **Purpose**: Tests purchase transaction aggregation
- **Validates**: Customer purchase metrics (total spent, average amount, purchase counts)
- **Expected**: Correct purchase aggregations within 90-day window

### 5. Final Metrics Aggregation Test
- **Purpose**: Tests the complete fact table logic end-to-end
- **Validates**: Final customer segment metrics and business KPIs
- **Expected**: Logical consistency in final aggregated metrics

### 6. Date Filtering Test
- **Purpose**: Ensures date filters work correctly
- **Validates**: 7-day filter for web events, 90-day filter for purchases
- **Expected**: Proper exclusion of data outside time windows

## Requirements

### Software Dependencies
- **Python 3.7+**
- **PySpark 3.0+** (`pip install pyspark`)
- **Java 8+** with `JAVA_HOME` environment variable set

### Data Dependencies
All source table SQL files must exist and be syntactically correct.

## Running the Tests

### Option 1: Using the Test Runner (Recommended)
```bash
# Run the complete test suite with dependency checking
python3 run_fct_customer_metrics_test.py
```

### Option 2: Direct unittest execution
```bash
# Run individual test classes
python3 -m unittest test_fct_customer_metrics.TestFctCustomerMetrics -v

# Run specific test methods
python3 -m unittest test_fct_customer_metrics.TestFctCustomerMetrics.test_fct_customer_metrics_basic_execution -v
```

### Option 3: Using pytest (if installed)
```bash
pip install pytest
pytest test_fct_customer_metrics.py -v
```

## Test Data

The test suite creates realistic synthetic data that covers:

### Web Events Data
- **Volume**: ~17 events across 4 customers and 7 sessions
- **Time Range**: Last 7 days (matching fact table filter)
- **Event Types**: page_view, search, purchase
- **Coverage**: Multiple sessions per customer, purchases and non-purchases

### Customer Profiles Data  
- **Volume**: 5 customers
- **Segments**: Mix of new, regular, and veteran customers
- **Tiers**: Bronze, silver, gold, platinum, diamond membership levels
- **Demographics**: Various age groups and geographic locations

### Purchase Data
- **Volume**: ~10 purchases across multiple customers
- **Time Range**: Last 90 days (matching fact table filter)
- **Value Range**: $29.99 to $1,299.99
- **Currency**: USD, GBP, EUR for international testing

## Expected Test Results

When all tests pass, you should see output similar to:

```
✓ Fact table query executed successfully
✓ Returned X rows
✓ All expected columns are present
✓ Customer segmentation logic is working correctly
✓ Session metrics calculation is working correctly
✓ Purchase history aggregation is working correctly
✓ Final metrics aggregation is working correctly
✓ Date filtering logic is working correctly
```

## Troubleshooting

### Common Issues

**1. PySpark Import Error**
```
ModuleNotFoundError: No module named 'pyspark'
```
**Solution**: Install PySpark: `pip install pyspark`

**2. Java Not Found Error**
```
JAVA_HOME is not set and could not find java
```
**Solution**: Install Java 8+ and set JAVA_HOME environment variable

**3. SQL File Not Found**
```
FileNotFoundError: [Errno 2] No such file or directory
```
**Solution**: Ensure all source SQL files exist in correct directories

**4. Test Data Issues**
```
AssertionError: Fact table returned 0 rows
```
**Solution**: Check date filters and ensure test data timestamps are recent

### Debug Mode

To see detailed query results during testing, uncomment the `show()` statements in the test methods:

```python
# Uncomment these lines to see intermediate results
result_df.show(truncate=False)
```

## Customization

### Adding New Test Cases

To add new test cases, extend the `TestFctCustomerMetrics` class:

```python
def test_my_custom_validation(self):
    """Test custom business logic"""
    # Setup test data
    self._create_test_web_events()
    self._create_test_customer_profiles()
    self._create_test_purchases()
    
    # Your custom test logic here
    # ...
    
    # Assertions
    self.assertEqual(expected, actual, "Custom validation message")
```

### Modifying Test Data

To modify test data, update the corresponding methods:
- `_create_test_web_events()` - Web events data
- `_create_test_customer_profiles()` - Customer profiles
- `_create_test_purchases()` - Purchase transactions

## Integration with CI/CD

This test suite can be integrated into CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
test_spark_sql:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v2
    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.8'
    - name: Install dependencies
      run: |
        pip install pyspark
    - name: Run Spark SQL tests
      run: |
        cd spark-project
        python3 run_fct_customer_metrics_test.py
```

## Performance Considerations

- Tests run on local Spark with `local[*]` master
- Adaptive query execution is enabled for optimal performance
- Test data is kept small for fast execution
- Each test method is isolated and cleans up temporary views

## Contributing

When adding new tests:
1. Follow the existing naming convention (`test_*`)
2. Include comprehensive docstrings
3. Create realistic test data that matches production patterns
4. Add appropriate assertions with meaningful error messages
5. Clean up resources in `tearDown()` method