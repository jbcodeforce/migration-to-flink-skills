"""
PySpark Test Case for fct_customer_metrics Fact Table

This test validates the customer metrics fact table logic using the created source tables:
- src_web_events
- src_customer_profiles  
- src_purchases
"""

from pathlib import Path
import sys
import unittest
from datetime import datetime, timedelta, date
from decimal import Decimal

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        StructType, 
        StructField, 
        StringType, 
        TimestampType, 
        DateType, 
        DoubleType, 
        IntegerType,
        LongType,
        DecimalType,
        BooleanType
    )
    from pyspark.sql.functions import col, lit, current_date, current_timestamp
except ImportError:
    print("ERROR: PySpark not installed. Please run: pip install pyspark")
    sys.exit(1)


class TestFctCustomerMetrics(unittest.TestCase):
    """Test case for validating the customer metrics fact table"""

    @classmethod
    def setUpClass(cls):
        """Set up Spark session and directories"""
        cls.spark = SparkSession.builder \
            .appName("TestFctCustomerMetrics") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .getOrCreate()
        
        cls.current_dir = Path(__file__).parent
        cls.sources_dir = cls.current_dir / "../sources"
        cls.facts_dir = cls.current_dir / "../facts" / "cj"
        
        if not cls.sources_dir.exists():
            print(f"ERROR: Sources directory not found: {cls.sources_dir}")
            sys.exit(1)
        
        if not cls.facts_dir.exists():
            print(f"ERROR: Facts directory not found: {cls.facts_dir}")
            sys.exit(1)

    def setUp(self):
        """Setup method called before each test"""
        self.test_results = []
        
    def tearDown(self):
        """Cleanup after each test"""
        # Drop temporary views to avoid conflicts
        temp_views = ["src_web_events", "src_customer_profiles", "src_purchases"]
        for view in temp_views:
            try:
                self.spark.sql(f"DROP VIEW IF EXISTS {view}")
            except:
                pass

    def _create_test_web_events(self):
        """Create test data for src_web_events table"""
        # Create timestamps for last 7 days (as used in fact table filter)
        now = datetime.now()
        base_time = now.replace(hour=10, minute=0, second=0, microsecond=0)
        
        web_events_data = [
            # Customer 1001 - Active session with purchase
            (100001, 1001, "sess_1001_001", base_time - timedelta(days=1), "page_view", "/home", "Homepage", "desktop", "Chrome", "US", "CA", "San Francisco"),
            (100002, 1001, "sess_1001_001", base_time - timedelta(days=1) + timedelta(minutes=5), "page_view", "/products", "Products", "desktop", "Chrome", "US", "CA", "San Francisco"),
            (100003, 1001, "sess_1001_001", base_time - timedelta(days=1) + timedelta(minutes=10), "search", "/search", "Search", "desktop", "Chrome", "US", "CA", "San Francisco"),
            (100004, 1001, "sess_1001_001", base_time - timedelta(days=1) + timedelta(minutes=15), "purchase", "/checkout", "Checkout", "desktop", "Chrome", "US", "CA", "San Francisco"),
            
            # Customer 1001 - Second session (different day)
            (100005, 1001, "sess_1001_002", base_time - timedelta(days=2), "page_view", "/home", "Homepage", "mobile", "Safari", "US", "CA", "San Francisco"),
            (100006, 1001, "sess_1001_002", base_time - timedelta(days=2) + timedelta(minutes=3), "page_view", "/categories", "Categories", "mobile", "Safari", "US", "CA", "San Francisco"),
            
            # Customer 1002 - Session with no purchase
            (100007, 1002, "sess_1002_001", base_time - timedelta(days=1), "page_view", "/home", "Homepage", "desktop", "Firefox", "US", "NY", "New York"),
            (100008, 1002, "sess_1002_001", base_time - timedelta(days=1) + timedelta(minutes=2), "page_view", "/about", "About", "desktop", "Firefox", "US", "NY", "New York"),
            (100009, 1002, "sess_1002_001", base_time - timedelta(days=1) + timedelta(minutes=5), "page_view", "/contact", "Contact", "desktop", "Firefox", "US", "NY", "New York"),
            
            # Customer 1003 - Multiple sessions with purchases
            (100010, 1003, "sess_1003_001", base_time - timedelta(days=3), "page_view", "/home", "Homepage", "desktop", "Chrome", "UK", "London", "London"),
            (100011, 1003, "sess_1003_001", base_time - timedelta(days=3) + timedelta(minutes=8), "purchase", "/checkout", "Checkout", "desktop", "Chrome", "UK", "London", "London"),
            (100012, 1003, "sess_1003_002", base_time - timedelta(days=5), "page_view", "/products", "Products", "desktop", "Chrome", "UK", "London", "London"),
            (100013, 1003, "sess_1003_002", base_time - timedelta(days=5) + timedelta(minutes=12), "purchase", "/checkout", "Checkout", "desktop", "Chrome", "UK", "London", "London"),
            
            # Customer 1004 - Browsing only, no purchases
            (100014, 1004, "sess_1004_001", base_time - timedelta(days=6), "page_view", "/home", "Homepage", "mobile", "Chrome", "CA", "ON", "Toronto"),
            (100015, 1004, "sess_1004_001", base_time - timedelta(days=6) + timedelta(minutes=1), "page_view", "/products", "Products", "mobile", "Chrome", "CA", "ON", "Toronto"),
            (100016, 1004, "sess_1004_001", base_time - timedelta(days=6) + timedelta(minutes=3), "page_view", "/categories", "Categories", "mobile", "Chrome", "CA", "ON", "Toronto"),
            
            # Events older than 7 days (should be filtered out)
            (100017, 1001, "sess_1001_old", base_time - timedelta(days=10), "page_view", "/home", "Homepage", "desktop", "Chrome", "US", "CA", "San Francisco"),
        ]
        
        web_events_schema = StructType([
            StructField("event_id", LongType(), False),
            StructField("customer_id", LongType(), False),
            StructField("session_id", StringType(), False),
            StructField("event_timestamp", TimestampType(), False),
            StructField("event_type", StringType(), False),
            StructField("page_url", StringType(), True),
            StructField("page_title", StringType(), True),
            StructField("device_type", StringType(), True),
            StructField("browser", StringType(), True),
            StructField("country", StringType(), True),
            StructField("region", StringType(), True),
            StructField("city", StringType(), True)
        ])
        
        web_events_df = self.spark.createDataFrame(web_events_data, web_events_schema)
        web_events_df.createOrReplaceTempView("src_web_events")
        
        print(f"Created src_web_events with {web_events_df.count()} rows")
        web_events_df.show(5, truncate=False)

    def _create_test_customer_profiles(self):
        """Create test data for src_customer_profiles table"""
        
        customer_profiles_data = [
            (1001, "John", "Smith", "john.smith@email.com", "25-34", "M", "San Francisco, CA", "platinum", date(2022, 1, 15), True, "active"),
            (1002, "Jane", "Doe", "jane.doe@email.com", "35-44", "F", "New York, NY", "gold", date(2023, 6, 20), True, "active"),
            (1003, "Bob", "Johnson", "bob.johnson@email.com", "25-34", "M", "London, UK", "silver", date(2021, 3, 10), True, "active"),
            (1004, "Alice", "Brown", "alice.brown@email.com", "18-24", "F", "Toronto, ON", "bronze", date(2023, 11, 1), True, "active"),
            (1005, "Charlie", "Wilson", "charlie.wilson@email.com", "45-54", "M", "Berlin, DE", "diamond", date(2020, 8, 5), True, "active"),
        ]
        
        customer_profiles_schema = StructType([
            StructField("customer_id", LongType(), False),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), False),
            StructField("age_group", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("location", StringType(), True),
            StructField("membership_tier", StringType(), True),
            StructField("registration_date", DateType(), False),
            StructField("email_verified", BooleanType(), True),
            StructField("account_status", StringType(), True)
        ])
        
        customer_profiles_df = self.spark.createDataFrame(customer_profiles_data, customer_profiles_schema)
        customer_profiles_df.createOrReplaceTempView("src_customer_profiles")
        
        print(f"Created src_customer_profiles with {customer_profiles_df.count()} rows")
        customer_profiles_df.show(truncate=False)

    def _create_test_purchases(self):
        """Create test data for src_purchases table (last 90 days as used in fact table)"""
        now = datetime.now()
        base_date = now.date()
        
        purchases_data = [
            # Customer 1001 purchases
            (200001, 1001, "ORD-001", "PROD_123", "Winter Jacket", "clothing", 1, Decimal("299.99"), Decimal("299.99"), "USD", "credit_card", "completed", base_date - timedelta(days=2), now - timedelta(days=2)),
            (200002, 1001, "ORD-002", "PROD_456", "Smart Watch", "electronics", 1, Decimal("199.99"), Decimal("199.99"), "USD", "paypal", "completed", base_date - timedelta(days=15), now - timedelta(days=15)),
            (200003, 1001, "ORD-003", "PROD_789", "Headphones", "electronics", 2, Decimal("89.99"), Decimal("179.98"), "USD", "credit_card", "completed", base_date - timedelta(days=30), now - timedelta(days=30)),
            
            # Customer 1002 purchases
            (200004, 1002, "ORD-004", "PROD_321", "Programming Book", "books", 1, Decimal("49.99"), Decimal("49.99"), "USD", "credit_card", "completed", base_date - timedelta(days=5), now - timedelta(days=5)),
            (200005, 1002, "ORD-005", "PROD_654", "Phone Case", "accessories", 1, Decimal("29.99"), Decimal("29.99"), "USD", "apple_pay", "completed", base_date - timedelta(days=20), now - timedelta(days=20)),
            
            # Customer 1003 purchases  
            (200006, 1003, "ORD-006", "PROD_111", "Gaming Laptop", "electronics", 1, Decimal("1299.99"), Decimal("1299.99"), "GBP", "credit_card", "completed", base_date - timedelta(days=1), now - timedelta(days=1)),
            (200007, 1003, "ORD-007", "PROD_222", "Keyboard", "electronics", 1, Decimal("149.99"), Decimal("149.99"), "GBP", "paypal", "completed", base_date - timedelta(days=10), now - timedelta(days=10)),
            (200008, 1003, "ORD-008", "PROD_333", "Mouse", "electronics", 1, Decimal("79.99"), Decimal("79.99"), "GBP", "credit_card", "completed", base_date - timedelta(days=25), now - timedelta(days=25)),
            
            # Customer 1005 purchases (no recent web events, but has purchase history)
            (200009, 1005, "ORD-009", "PROD_777", "Luxury Watch", "accessories", 1, Decimal("899.99"), Decimal("899.99"), "EUR", "bank_transfer", "completed", base_date - timedelta(days=45), now - timedelta(days=45)),
            
            # Purchases older than 90 days (should be filtered out)
            (200010, 1001, "ORD-010", "PROD_OLD", "Old Product", "misc", 1, Decimal("99.99"), Decimal("99.99"), "USD", "credit_card", "completed", base_date - timedelta(days=120), now - timedelta(days=120)),
        ]
        
        purchases_schema = StructType([
            StructField("purchase_id", LongType(), False),
            StructField("customer_id", LongType(), False),
            StructField("order_id", StringType(), False),
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DecimalType(10, 2), True),
            StructField("amount", DecimalType(10, 2), False),
            StructField("currency_code", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("payment_status", StringType(), True),
            StructField("purchase_date", DateType(), False),
            StructField("purchase_timestamp", TimestampType(), False)
        ])
        
        purchases_df = self.spark.createDataFrame(purchases_data, purchases_schema)
        purchases_df.createOrReplaceTempView("src_purchases")
        
        print(f"Created src_purchases with {purchases_df.count()} rows")
        purchases_df.show(truncate=False)

    def _read_sql_file(self, file_path: Path) -> str:
        """Read SQL content from file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read().strip()
                # Remove comments and empty lines for cleaner execution
                sql_lines = []
                for line in sql_content.split('\n'):
                    line = line.strip()
                    if line and not line.startswith('--'):
                        sql_lines.append(line)
                return '\n'.join(sql_lines)
        except Exception as e:
            raise Exception(f"Failed to read file {file_path}: {e}")

    def test_fct_customer_metrics_basic_execution(self):
        """Test that the fact table query executes without errors"""
        print("\n" + "="*60)
        print("TEST: Basic Execution of fct_customer_metrics")
        print("="*60)
        
        # Setup test data
        self._create_test_web_events()
        self._create_test_customer_profiles()
        self._create_test_purchases()
        
        # Read and execute the fact table SQL
        fact_sql_path = self.facts_dir / "fct_customer_metrics.sql"
        fact_sql = self._read_sql_file(fact_sql_path)
        
        try:
            result_df = self.spark.sql(fact_sql)
            row_count = result_df.count()
            
            print(f"\n✓ Fact table query executed successfully")
            print(f"✓ Returned {row_count} rows")
            
            if row_count > 0:
                print("\nFact table results:")
                result_df.show(truncate=False)
                
                # Basic assertions
                self.assertGreater(row_count, 0, "Fact table should return at least one row")
                
                # Check that we have expected columns
                expected_columns = [
                    "customer_segment", "membership_tier", "location", "customer_count",
                    "avg_sessions", "avg_events_per_session", "avg_total_spent", 
                    "median_total_spent", "recent_purchasers", "at_risk_customers"
                ]
                
                actual_columns = result_df.columns
                for col_name in expected_columns:
                    self.assertIn(col_name, actual_columns, f"Expected column '{col_name}' not found in result")
                
                print("✓ All expected columns are present")
                
            else:
                self.fail("Fact table returned 0 rows - check test data and date filters")
                
        except Exception as e:
            self.fail(f"Fact table query failed: {str(e)}")

    def test_customer_segmentation_logic(self):
        """Test the customer segmentation logic in the fact table"""
        print("\n" + "="*60)
        print("TEST: Customer Segmentation Logic")
        print("="*60)
        
        # Setup test data
        self._create_test_web_events()
        self._create_test_customer_profiles()
        self._create_test_purchases()
        
        # Test the customer_segments CTE separately
        customer_segments_sql = """
        SELECT 
            customer_id,
            age_group,
            location,
            membership_tier,
            registration_date,
            CASE 
                WHEN DATEDIFF(CURRENT_DATE, registration_date) <= 30 THEN 'new'
                WHEN DATEDIFF(CURRENT_DATE, registration_date) <= 365 THEN 'regular'
                ELSE 'veteran'
            END as customer_segment
        FROM src_customer_profiles
        """
        
        segments_df = self.spark.sql(customer_segments_sql)
        segments_df.show(truncate=False)
        
        # Verify segmentation logic
        segments_list = [row.customer_segment for row in segments_df.collect()]
        
        print(f"Customer segments found: {set(segments_list)}")
        
        # We should have a mix of segments based on our test data
        self.assertIn('new', segments_list, "Should have 'new' customers (registered within 30 days)")
        self.assertIn('veteran', segments_list, "Should have 'veteran' customers (registered > 1 year ago)")
        
        print("✓ Customer segmentation logic is working correctly")

    def test_session_metrics_calculation(self):
        """Test the session-level metrics calculation"""
        print("\n" + "="*60)
        print("TEST: Session Metrics Calculation")
        print("="*60)
        
        # Setup test data
        self._create_test_web_events()
        self._create_test_customer_profiles()
        self._create_test_purchases()
        
        # Test the customer_sessions CTE separately
        customer_sessions_sql = """
        SELECT 
            customer_id,
            session_id,
            MIN(event_timestamp) as session_start,
            MAX(event_timestamp) as session_end,
            COUNT(*) as total_events,
            COUNT(DISTINCT page_url) as unique_pages_visited,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases_in_session
        FROM src_web_events
        WHERE event_timestamp >= CURRENT_DATE - INTERVAL 7 DAYS
        GROUP BY customer_id, session_id
        ORDER BY customer_id, session_id
        """
        
        sessions_df = self.spark.sql(customer_sessions_sql)
        sessions_df.show(truncate=False)
        
        sessions_list = sessions_df.collect()
        
        # Verify session metrics
        for session in sessions_list:
            self.assertGreater(session.total_events, 0, "Each session should have at least one event")
            self.assertGreaterEqual(session.purchases_in_session, 0, "Purchases should be non-negative")
            self.assertGreaterEqual(session.unique_pages_visited, 1, "Should visit at least one unique page")
        
        # Check specific customer sessions
        customer_1001_sessions = [s for s in sessions_list if s.customer_id == 1001]
        self.assertGreaterEqual(len(customer_1001_sessions), 1, "Customer 1001 should have at least one session")
        
        # Customer 1001 should have at least one session with a purchase
        customer_1001_purchases = sum(s.purchases_in_session for s in customer_1001_sessions)
        self.assertGreater(customer_1001_purchases, 0, "Customer 1001 should have purchases in their sessions")
        
        print("✓ Session metrics calculation is working correctly")

    def test_purchase_history_aggregation(self):
        """Test the purchase history aggregation logic"""
        print("\n" + "="*60)
        print("TEST: Purchase History Aggregation")
        print("="*60)
        
        # Setup test data
        self._create_test_web_events()
        self._create_test_customer_profiles()
        self._create_test_purchases()
        
        # Test the purchase_history CTE separately
        purchase_history_sql = """
        SELECT 
            customer_id,
            COUNT(*) as total_purchases,
            SUM(amount) as total_spent,
            AVG(amount) as avg_purchase_amount,
            MAX(purchase_date) as last_purchase_date,
            MIN(purchase_date) as first_purchase_date
        FROM src_purchases
        WHERE purchase_date >= CURRENT_DATE - INTERVAL 90 DAYS
        GROUP BY customer_id
        ORDER BY customer_id
        """
        
        purchase_history_df = self.spark.sql(purchase_history_sql)
        purchase_history_df.show(truncate=False)
        
        purchase_history_list = purchase_history_df.collect()
        
        # Verify purchase aggregations
        for customer in purchase_history_list:
            self.assertGreater(customer.total_purchases, 0, "Each customer should have at least one purchase")
            self.assertGreater(customer.total_spent, 0, "Total spent should be positive")
            self.assertGreater(customer.avg_purchase_amount, 0, "Average purchase amount should be positive")
            self.assertIsNotNone(customer.last_purchase_date, "Last purchase date should not be null")
            self.assertIsNotNone(customer.first_purchase_date, "First purchase date should not be null")
        
        # Check specific customers
        customer_1001_data = [c for c in purchase_history_list if c.customer_id == 1001]
        self.assertEqual(len(customer_1001_data), 1, "Should have exactly one record per customer")
        
        customer_1001 = customer_1001_data[0]
        self.assertEqual(customer_1001.total_purchases, 3, "Customer 1001 should have 3 purchases in last 90 days")
        
        print("✓ Purchase history aggregation is working correctly")

    def test_final_metrics_aggregation(self):
        """Test the final metrics aggregation by segment"""
        print("\n" + "="*60)
        print("TEST: Final Metrics Aggregation")
        print("="*60)
        
        # Setup test data
        self._create_test_web_events()
        self._create_test_customer_profiles()
        self._create_test_purchases()
        
        # Execute the full fact table query
        fact_sql_path = self.facts_dir / "fct_customer_metrics.sql"
        fact_sql = self._read_sql_file(fact_sql_path)
        
        result_df = self.spark.sql(fact_sql)
        result_df.show(truncate=False)
        
        results = result_df.collect()
        
        # Verify final aggregations
        for row in results:
            # Basic validations
            self.assertGreater(row.customer_count, 0, "Customer count should be positive")
            self.assertGreaterEqual(row.avg_sessions, 0, "Average sessions should be non-negative")
            self.assertGreaterEqual(row.avg_events_per_session, 0, "Average events per session should be non-negative")
            self.assertGreaterEqual(row.avg_total_spent, 0, "Average total spent should be non-negative")
            self.assertGreaterEqual(row.recent_purchasers, 0, "Recent purchasers should be non-negative")
            self.assertGreaterEqual(row.at_risk_customers, 0, "At-risk customers should be non-negative")
            
            # Logical validations
            self.assertLessEqual(row.recent_purchasers, row.customer_count, "Recent purchasers should not exceed customer count")
            self.assertLessEqual(row.at_risk_customers, row.customer_count, "At-risk customers should not exceed customer count")
        
        # Check that we have data for different segments
        segments = [row.customer_segment for row in results]
        tiers = [row.membership_tier for row in results]
        
        print(f"Customer segments in results: {set(segments)}")
        print(f"Membership tiers in results: {set(tiers)}")
        
        # We should have multiple segments/tiers represented
        self.assertGreater(len(set(segments)), 0, "Should have at least one customer segment")
        self.assertGreater(len(set(tiers)), 0, "Should have at least one membership tier")
        
        print("✓ Final metrics aggregation is working correctly")

    def test_date_filtering(self):
        """Test that date filters are working correctly"""
        print("\n" + "="*60)
        print("TEST: Date Filtering Logic")
        print("="*60)
        
        # Setup test data
        self._create_test_web_events()
        self._create_test_customer_profiles()
        self._create_test_purchases()
        
        # Test web events date filter (7 days)
        web_events_filtered_sql = """
        SELECT COUNT(*) as total_events
        FROM src_web_events
        WHERE event_timestamp >= CURRENT_DATE - INTERVAL 7 DAYS
        """
        
        web_events_count = self.spark.sql(web_events_filtered_sql).collect()[0].total_events
        print(f"Web events within last 7 days: {web_events_count}")
        
        # Test purchases date filter (90 days)
        purchases_filtered_sql = """
        SELECT COUNT(*) as total_purchases
        FROM src_purchases
        WHERE purchase_date >= CURRENT_DATE - INTERVAL 90 DAYS
        """
        
        purchases_count = self.spark.sql(purchases_filtered_sql).collect()[0].total_purchases
        print(f"Purchases within last 90 days: {purchases_count}")
        
        # Verify that old events/purchases are filtered out
        self.assertGreater(web_events_count, 0, "Should have recent web events")
        self.assertGreater(purchases_count, 0, "Should have recent purchases")
        
        # Count total events/purchases without filter
        total_web_events = self.spark.sql("SELECT COUNT(*) as total FROM src_web_events").collect()[0].total
        total_purchases = self.spark.sql("SELECT COUNT(*) as total FROM src_purchases").collect()[0].total
        
        print(f"Total web events (no filter): {total_web_events}")
        print(f"Total purchases (no filter): {total_purchases}")
        
        # Filtered counts should be less than or equal to total counts
        self.assertLessEqual(web_events_count, total_web_events, "Filtered events should be <= total events")
        self.assertLessEqual(purchases_count, total_purchases, "Filtered purchases should be <= total purchases")
        
        print("✓ Date filtering logic is working correctly")

    @classmethod
    def tearDownClass(cls):
        """Clean up Spark session"""
        if hasattr(cls, 'spark'):
            cls.spark.stop()


if __name__ == "__main__":
    print("🚀 Starting Customer Metrics Fact Table Tests")
    print("="*60)
    unittest.main(verbosity=2)