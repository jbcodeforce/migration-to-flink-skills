#!/usr/bin/env python3
"""
Script to run the Customer Metrics Fact Table tests

This script runs the comprehensive test suite for the fct_customer_metrics fact table.

Usage:
    python3 run_fct_customer_metrics_test.py

Requirements:
    - PySpark installed: pip install pyspark
    - Java 8+ installed and JAVA_HOME set
"""

import sys
import os
from pathlib import Path

# Add the current directory to Python path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir))

def check_requirements():
    """Check if required dependencies are available"""
    try:
        import pyspark
        print(f"✓ PySpark version: {pyspark.__version__}")
    except ImportError:
        print("❌ PySpark not installed. Please run: pip install pyspark")
        return False
    
    # Check for Java
    java_home = os.environ.get('JAVA_HOME')
    if not java_home:
        print("⚠️  JAVA_HOME not set. PySpark may not work properly.")
        print("   Please install Java 8+ and set JAVA_HOME environment variable.")
    else:
        print(f"✓ JAVA_HOME: {java_home}")
    
    # Check if required SQL files exist
    required_files = [
        current_dir / "sources" / "src_web_events.sql",
        current_dir / "sources" / "src_customer_profiles.sql", 
        current_dir / "sources" / "src_purchases.sql",
        current_dir / "facts" / "cj" / "fct_customer_metrics.sql"
    ]
    
    for file_path in required_files:
        if file_path.exists():
            print(f"✓ Found: {file_path.name}")
        else:
            print(f"❌ Missing: {file_path}")
            return False
    
    return True

def run_tests():
    """Run the test suite"""
    if not check_requirements():
        print("\n❌ Requirements check failed. Please fix the issues above.")
        sys.exit(1)
    
    print("\n" + "="*80)
    print("STARTING CUSTOMER METRICS FACT TABLE TEST SUITE")
    print("="*80)
    
    # Import and run the tests
    try:
        import unittest
        from test_fct_customer_metrics import TestFctCustomerMetrics
        
        # Create test suite
        loader = unittest.TestLoader()
        suite = loader.loadTestsFromTestCase(TestFctCustomerMetrics)
        
        # Run tests with detailed output
        runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout, buffer=False)
        result = runner.run(suite)
        
        # Print summary
        print("\n" + "="*80)
        print("TEST SUMMARY")
        print("="*80)
        print(f"Tests run: {result.testsRun}")
        print(f"Failures: {len(result.failures)}")
        print(f"Errors: {len(result.errors)}")
        
        if result.failures:
            print("\nFAILURES:")
            for test, traceback in result.failures:
                print(f"  - {test}: {traceback}")
        
        if result.errors:
            print("\nERRORS:")
            for test, traceback in result.errors:
                print(f"  - {test}: {traceback}")
        
        if result.wasSuccessful():
            print("\n✅ ALL TESTS PASSED!")
            return True
        else:
            print(f"\n❌ {len(result.failures + result.errors)} TEST(S) FAILED!")
            return False
            
    except Exception as e:
        print(f"\n❌ Failed to run tests: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main function"""
    print("Customer Metrics Fact Table Test Runner")
    print("="*40)
    
    success = run_tests()
    
    if success:
        print("\n🎉 Test suite completed successfully!")
        sys.exit(0)
    else:
        print("\n💥 Test suite failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()