"""
SQLDQ Data Quality Framework Demo Script

This script demonstrates the SQLDQ framework with comprehensive examples
including sample data generation, DuckDB backend usage, and extensive
reporting and export functionality.

Includes:
- Generating sample data with intentional data quality issues
- DuckDB example with multiple data quality checks
- Comprehensive reporting (Text, Markdown, HTML)
- Export functionality (CSV, Parquet, local and S3)
- Programmatic post-processing examples
"""

import logging
import os
from datetime import datetime

import polars as pl


def create_sample_data():
    """
    Create sample data with intentional data quality issues for testing.

    The sample data includes the following data quality issues:
    - Range check: age 150 is an implausible outlier
    - Duplicates: Duplicate user_id 4
    - Regex: email is invalid for user_id 3
    - Business Rule violation (multi-column): True: A, B; False: C, not met for user_id 3 (False, B)
    - Business Rule violation (multi-dataset): "orders"(order_id=3) includes user_id=5 which is missing in "users"
    """
    print("=== Creating Sample Data ===")

    df_users = pl.DataFrame({
        "user_id": [1, 2, 3, 4, 4],  # Duplicate user_id 4
        "age": [25, 150, 22, 45, 30],  # Age 150 is outlier
        "email": [
            "user1@example.com",
            "user2@example.com",
            "invalid-email",
            "user4@example.com",
            "user5@example.com",
        ],  # Invalid email for user_id 3
        "score": [85.5, 92.0, 78.3, 88.7, 95.2],
        "is_active": [True, True, False, True, False],
        "category": ["A", "B", "B", "A", "C"],  # User_id 3: False, B violates business rule
    })

    df_orders = pl.DataFrame({
        "order_id": [1, 2, 3],
        "user_id": [1, 2, 5],  # user_id 5 missing in users table
        "order_date": ["2021-01-01", "2021-01-02", "2021-01-03"],
        "order_amount": [100, 200, 300],
    })

    print(f"Created users table with {len(df_users)} rows")
    print(f"Created orders table with {len(df_orders)} rows")
    print("Sample data includes intentional data quality issues for testing\n")

    return df_users, df_orders


def setup_duckdb_connection(df_users, df_orders):
    """Setup DuckDB connection and register DataFrames."""
    try:
        import duckdb

        print("=== Setting up DuckDB Connection ===")

        con = duckdb.connect()
        con.register("users", df_users)
        con.register("orders", df_orders)

        print("✅ DuckDB connection established")
        print("✅ Registered 'users' and 'orders' tables")
        return con
    except ImportError as e:
        print(f"❌ DuckDB not available: {e}")
        return None


def create_duckdb_data_quality_checks(dq):
    """
    Add DuckDB-specific data quality checks to SQLDQ instance.
    Uses DuckDB's regex syntax (~) and specific SQL features.

    Returns SQLDQ instance with all checks configured.
    """
    print("=== Configuring DuckDB Data Quality Checks ===")

    dq = (
        dq.add_check(
            name="Age between 18 and 80", failure_rows_query="SELECT * FROM users WHERE age NOT BETWEEN 18 AND 80"
        )
        .add_check(
            name="Duplicate user IDs",
            failure_rows_query="""
            WITH duplicates AS (
                SELECT user_id, COUNT(*) AS count
                FROM users
                GROUP BY user_id
                HAVING COUNT(*) > 1
            )
            SELECT * FROM users WHERE user_id IN (SELECT user_id FROM duplicates)""",
            columns=["user_id", "email"],
        )
        .add_check(
            name="Email format valid",
            failure_rows_query=r"SELECT * FROM users WHERE email !~ '^[^@]+@[^@]+\.[^@]+'",
            columns=["email"],
        )
        .add_check(
            name="Missing user IDs in orders",
            failure_rows_query="""SELECT * FROM orders o
            LEFT JOIN users u ON o.user_id = u.user_id
            WHERE u.user_id IS NULL""",
            columns=["order_id", "user_id"],
        )
        .add_check(
            name="Active users in category A or B only",
            failure_rows_query="""
            SELECT * FROM users
            WHERE is_active = TRUE AND category NOT IN ('A', 'B')""",
            columns=["user_id", "is_active", "category"],
        )
        .add_check(
            name="Inactive users in category C only",
            failure_rows_query="""SELECT * FROM users
            WHERE is_active = FALSE AND category != 'C'""",
            columns=["user_id", "is_active", "category"],
        )
    )

    print("✅ Configured 6 DuckDB-specific data quality checks")
    return dq


def create_pyspark_data_quality_checks(dq):
    """
    Add PySpark-specific data quality checks to SQLDQ instance.
    Uses PySpark's RLIKE syntax and specific SQL features.
    """
    print("=== Configuring PySpark Data Quality Checks ===")

    dq = (
        dq.add_check(name="Age between 18 and 80", failure_rows_query="SELECT * FROM users WHERE age < 18 OR age > 80")
        .add_check(
            name="Duplicate user IDs",
            failure_rows_query="""
            WITH duplicates AS (
                SELECT user_id, COUNT(*) AS count
                FROM users
                GROUP BY user_id
                HAVING COUNT(*) > 1
            )
            SELECT * FROM users WHERE user_id IN (SELECT user_id FROM duplicates)""",
            columns=["user_id", "email"],
        )
        .add_check(
            name="Email format valid",
            failure_rows_query=r"SELECT * FROM users WHERE NOT (email RLIKE '^[^@]+@[^@]+\.[^@]+')",
            columns=["email"],
        )
        .add_check(name="Score within range", failure_rows_query="SELECT * FROM users WHERE score < 0 OR score > 100")
        .add_check(
            name="Active users in category A or B only",
            failure_rows_query="""
            SELECT * FROM users
            WHERE is_active = TRUE AND category NOT IN ('A', 'B')""",
            columns=["user_id", "is_active", "category"],
        )
        .add_check(
            name="Inactive users in category C only",
            failure_rows_query="""SELECT * FROM users
            WHERE is_active = FALSE AND category != 'C'""",
            columns=["user_id", "is_active", "category"],
        )
    )

    print("✅ Configured 6 PySpark-specific data quality checks")
    return dq


def create_postgresql_data_quality_checks(dq):
    """
    Add PostgreSQL-specific data quality checks to SQLDQ instance.
    Uses PostgreSQL's regex syntax (~) and specific features.
    """
    print("=== Configuring PostgreSQL Data Quality Checks ===")

    dq = (
        dq.add_check(name="Age between 18 and 80", failure_rows_query="SELECT * FROM users WHERE age < 18 OR age > 80")
        .add_check(
            name="Duplicate user IDs",
            failure_rows_query="""
            WITH counts AS(
                SELECT user_id, COUNT(*) AS count
                FROM users
                GROUP BY user_id
            )
            SELECT * FROM counts
            WHERE count > 1""",
            columns=["user_id", "count"],
        )
        .add_check(
            name="Email format valid",
            failure_rows_query=r"SELECT * FROM users WHERE email !~ '^[^@]+@[^@]+\.[^@]+'",
            columns=["email"],
        )
        .add_check(
            name="Missing user IDs in orders",
            failure_rows_query="""SELECT o.order_id, o.user_id FROM orders o
            LEFT JOIN users u ON o.user_id = u.user_id
            WHERE u.user_id IS NULL""",
            columns=["order_id", "user_id"],
        )
        .add_check(
            name="Active users in category A or B only",
            failure_rows_query="""
            SELECT * FROM users
            WHERE is_active = TRUE AND category NOT IN ('A', 'B')""",
            columns=["user_id", "is_active", "category"],
        )
        .add_check(
            name="Inactive users in category C only",
            failure_rows_query="""SELECT * FROM users
            WHERE is_active = FALSE AND category != 'C'""",
            columns=["user_id", "is_active", "category"],
        )
    )

    print("✅ Configured 6 PostgreSQL-specific data quality checks")
    return dq


def create_athena_data_quality_checks(dq):
    """
    Add Athena-specific data quality checks to SQLDQ instance.
    Uses Athena's regexp_like function and specific SQL features.
    """
    print("=== Configuring Athena Data Quality Checks ===")

    dq = (
        dq.add_check(name="Age between 18 and 80", failure_rows_query="SELECT * FROM users WHERE age < 18 OR age > 80")
        .add_check(
            name="Email format valid",
            failure_rows_query=r"SELECT * FROM users WHERE NOT regexp_like(email, '^[^@]+@[^@]+\\.[^@]+')",
            columns=["email"],
        )
        .add_check(
            name="Active users in category A or B only",
            failure_rows_query="""
            SELECT * FROM users
            WHERE is_active = TRUE AND category NOT IN ('A', 'B')""",
            columns=["user_id", "is_active", "category"],
        )
        .add_check(
            name="Inactive users in category C only",
            failure_rows_query="""SELECT * FROM users
            WHERE is_active = FALSE AND category != 'C'""",
            columns=["user_id", "is_active", "category"],
        )
    )

    print("✅ Configured 4 Athena-specific data quality checks")
    return dq


def run_duckdb_example():
    """Run comprehensive DuckDB example with full reporting and export demo."""
    print("=" * 60)
    print("=== DuckDB Data Quality Example ===")
    print("=" * 60)

    try:
        from sqldq import SQLDQ

        # Create sample data
        df_users, df_orders = create_sample_data()

        # Setup DuckDB
        con = setup_duckdb_connection(df_users, df_orders)
        if con is None:
            return

        # Initialize SQLDQ
        dq = SQLDQ.from_duckdb(con, default_max_rows=20)
        dq = create_duckdb_data_quality_checks(dq)

        # Execute checks
        print("=== Executing Data Quality Checks ===")
        results = dq.execute()

        # Basic summary
        summary = results.get_summary()
        print("✅ Execution complete!")
        print(f"📊 Summary: {summary}")
        print(f"❌ Has failures: {results.has_failures()}\n")

        # Demonstrate programmatic post-processing
        demonstrate_programmatic_processing(results)

        # Demonstrate reporting
        demonstrate_reporting(results)

        # Demonstrate export functionality
        demonstrate_exports(results)

        print("=" * 60)
        print("=== DuckDB Example Complete ===")
        print("=" * 60)

    except ImportError as e:
        print(f"❌ Required dependencies not available: {e}")
        print("Install with: pip install sqldq duckdb polars")


def demonstrate_programmatic_processing(results):
    """Demonstrate programmatic post-processing of results."""
    print("=" * 50)
    print("=== Programmatic Post-processing ===")
    print("=" * 50)

    # Check if we should send alerts
    if results.has_failures():
        print("⚠️  Data quality issues detected - alerts should be sent")
    else:
        print("✅ All checks passed - no alerts needed")

    # Analyze individual check results
    print("\n--- Individual Check Analysis ---")
    for name, check_result in results.results.items():
        status = "❌ FAILED" if check_result.failure_count > 0 else "✅ PASSED"
        print(f"{name}: {status}")
        if check_result.failure_count > 0:
            print(f"  └─ Failed rows: {check_result.failure_count}")
        if check_result.error_message:
            print(f"  └─ Error: {check_result.error_message}")

    print("\n📈 Overall Statistics:")
    summary = results.get_summary()
    print(f"  Total checks: {summary['total_checks']}")
    print(f"  Passed: {summary['passed_checks']}")
    print(f"  Failed: {summary['failed_checks']}")
    print(f"  Success rate: {summary['passed_checks'] / summary['total_checks'] * 100:.1f}%")


def demonstrate_reporting(results):
    """Demonstrate various reporting formats and options."""
    print("\n" + "=" * 50)
    print("=== Reporting Demonstrations ===")
    print("=" * 50)

    # Simple report
    print("\n--- Simple Report ---")
    simple_report = results.report()
    print(simple_report)

    # Detailed text report with rows and summary
    print("\n--- Detailed Text Report (Failed checks only) ---")
    detailed_report = results.report(format="text", include_rows=True, include_summary_header=True, fail_only=True)
    print(detailed_report)

    # Markdown report
    print("\n--- Markdown Report Sample ---")
    markdown_report = results.report(format="markdown", include_rows=True, include_summary_header=True, fail_only=True)
    print("Markdown report generated (first 500 chars):")
    print(markdown_report[:500] + "..." if len(markdown_report) > 500 else markdown_report)

    # HTML report
    print("\n--- HTML Report Sample ---")
    html_report = results.report(format="html", include_rows=True, include_summary_header=True, fail_only=True)
    print("HTML report generated (first 300 chars):")
    print(html_report[:300] + "..." if len(html_report) > 300 else html_report)


def demonstrate_exports(results):
    """Demonstrate export functionality for reports and failed rows."""
    print("\n" + "=" * 50)
    print("=== Export Demonstrations ===")
    print("=" * 50)

    # Create timestamp for file naming
    timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

    # Create directories if they don't exist
    os.makedirs("./demo/reports", exist_ok=True)
    os.makedirs("./demo/exports", exist_ok=True)

    print(f"📁 Using timestamp: {timestamp}")

    # Export reports in different formats
    print("\n--- Exporting Reports ---")
    try:
        # Text report
        text_file = f"./demo/reports/{timestamp}_dq_report.txt"
        results.export_report(text_file, format="text", include_rows=True)
        print(f"✅ Text report exported: {text_file}")

        # Markdown report
        md_file = f"./demo/reports/{timestamp}_dq_report.md"
        results.export_report(md_file, format="markdown", include_rows=True)
        print(f"✅ Markdown report exported: {md_file}")

        # HTML report
        html_file = f"./demo/reports/{timestamp}_dq_report.html"
        results.export_report(html_file, format="html", include_rows=True)
        print(f"✅ HTML report exported: {html_file}")

    except Exception as e:
        print(f"❌ Error exporting reports: {e}")

    # Export failed rows
    print("\n--- Exporting Failed Rows ---")
    try:
        # CSV export
        csv_export = f"./demo/exports/{timestamp}_dq_fails_csv"
        results.export_failed_rows(csv_export, format="csv")
        print(f"✅ Failed rows exported to CSV: {csv_export}")

        # Parquet export
        parquet_export = f"./demo/exports/{timestamp}_dq_fails_parquet"
        results.export_failed_rows(parquet_export, format="parquet")
        print(f"✅ Failed rows exported to Parquet: {parquet_export}")

    except Exception as e:
        print(f"❌ Error exporting failed rows: {e}")

    # Demonstrate S3 export (commented out - requires AWS setup)
    print("\n--- S3 Export Example (Commented) ---")
    print("# Uncomment and configure AWS credentials to test S3 exports:")
    print("# import boto3")
    print("# session = boto3.Session(profile_name='default')")
    print("# bucket = 'your-bucket-name'")
    print("# prefix = f'dq-export-{datetime.now().isoformat()}'")
    print("# results.export_failed_rows(f's3://{bucket}/{prefix}/fails', format='parquet', boto3_session=session)")


def run_other_backend_examples():
    """Placeholder for other backend examples."""
    print("\n" + "=" * 60)
    print("=== Other Backend Examples ===")
    print("=" * 60)

    print("🔧 PySpark Example:")
    print("   - Uncomment run_pyspark_example() in main() to test")
    print("   - Requires: pip install pyspark")

    print("\n🐘 PostgreSQL Example:")
    print("   - Uncomment run_postgresql_example() in main() to test")
    print("   - Requires: pip install psycopg2-binary")
    print("   - Requires: PostgreSQL server setup")

    print("\n☁️  Amazon Athena Example:")
    print("   - Uncomment run_athena_example() in main() to test")
    print("   - Requires: pip install boto3 awswrangler")
    print("   - Requires: AWS credentials and Athena setup")


def run_pyspark_example():
    """Run PySpark example (same as original but with better organization)."""
    print("\n=== PySpark Example ===")
    try:
        from pyspark.sql import SparkSession

        from sqldq import SQLDQ

        df_users, df_orders = create_sample_data()

        spark = (
            SparkSession.builder.appName("SQLDQ_Example").config("spark.sql.adaptive.enabled", "false").getOrCreate()
        )

        # Convert to Spark DataFrames
        spark_df_users = spark.createDataFrame(df_users.to_pandas())
        spark_df_users.createOrReplaceTempView("users")

        spark_df_orders = spark.createDataFrame(df_orders.to_pandas())
        spark_df_orders.createOrReplaceTempView("orders")

        dq = SQLDQ.from_pyspark(spark, default_max_rows=15)
        dq = create_pyspark_data_quality_checks(dq)

        results = dq.execute()
        print(f"Has failures: {results.has_failures()}")
        print(f"Summary: {results.get_summary()}")

        # Export reports
        timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        results.export_report(f"./demo/reports/pyspark_{timestamp}_report.html", format="html", include_rows=True)
        results.export_failed_rows(f"./demo/exports/pyspark_{timestamp}_failed_rows", format="parquet")

        print("\n=== PySpark Markdown Report ===")
        print(results.report(format="markdown", include_rows=True, fail_only=True))

        spark.stop()

    except ImportError as e:
        print(f"PySpark not available: {e}")


def run_postgresql_example():
    """Run PostgreSQL example (same as original but with better organization)."""
    print("\n=== PostgreSQL Example ===")
    try:
        import psycopg2

        from sqldq import SQLDQ

        # Connect to PostgreSQL
        conn = psycopg2.connect(host="postgres", database="sqldq_test", user="admin", password="admin")

        # Initialize SQLDQ with PostgreSQL backend
        dq = SQLDQ.from_postgresql(conn, default_max_rows=25)
        dq = create_postgresql_data_quality_checks(dq)

        results = dq.execute()
        print(f"Summary: {results.get_summary()}")
        print(results.report(format="text", include_rows=True, include_summary_header=True))

        # Export reports
        timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        results.export_report(f"./demo/reports/postgresql_{timestamp}_report.md", include_rows=True)
        results.export_failed_rows(f"./demo/exports/postgresql_{timestamp}_failed_rows")

        conn.close()

    except ImportError as e:
        print(f"PostgreSQL dependencies not available: {e}")


def run_athena_example():
    """Run Athena example (same as original but with better organization)."""
    print("\n=== Amazon Athena Example ===")
    try:
        import awswrangler as wr
        import boto3

        from sqldq import SQLDQ

        # Setup boto3 session
        session = boto3.Session(profile_name="default")

        # Initialize SQLDQ with Athena backend
        dq = SQLDQ.from_athena(
            database="your_database", workgroup="your_workgroup", boto3_session=session, default_max_rows=30
        )

        # Add Athena-specific checks
        dq = create_athena_data_quality_checks(dq)

        results = dq.execute()
        print(f"Summary: {results.get_summary()}")

        # Export to S3
        timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        results.export_report(
            f"s3://your-bucket/reports/athena_{timestamp}_report.html",
            format="html",
            include_rows=True,
            boto3_session=session,
        )

        results.export_failed_rows(
            f"s3://your-bucket/exports/athena_{timestamp}_failed_rows", format="parquet", boto3_session=session
        )

    except ImportError as e:
        print(f"Athena dependencies not available: {e}")


def print_installation_instructions():
    """Print comprehensive installation instructions."""
    print("\n" + "=" * 60)
    print("=== Installation Instructions ===")
    print("=" * 60)

    print("\n🚀 Core Dependencies:")
    print("pip install sqldq polars")

    print("\n📦 Backend-specific Dependencies:")
    print("# DuckDB (included in core)")
    print("pip install duckdb")

    print("\n# PySpark")
    print("pip install pyspark pandas")

    print("\n# PostgreSQL")
    print("pip install psycopg2-binary")

    print("\n# Amazon Athena")
    print("pip install boto3 awswrangler")

    print("\n🔧 Complete Installation:")
    print("pip install sqldq duckdb polars boto3 awswrangler psycopg2-binary pyspark pandas")

    print("\n📋 Additional Setup:")
    print("- For PostgreSQL: Setup PostgreSQL server and database")
    print("- For Athena: Configure AWS credentials and S3 bucket")
    print("- For S3 exports: Configure boto3 session with appropriate permissions")


if __name__ == "__main__":
    # Set up logging to reduce noise
    logging.basicConfig(level=logging.CRITICAL)

    print("🔬 SQLDQ Data Quality Framework - Comprehensive Demo")
    print("=" * 60)

    # Run the main DuckDB example with full demonstrations
    run_duckdb_example()

    # Show information about other backends
    run_other_backend_examples()

    # Uncomment to run additional backend examples:
    # run_pyspark_example()
    # run_postgresql_example()
    # run_athena_example()

    # Print installation instructions
    print_installation_instructions()

    print(f"\n✅ Demo completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Check the ./demo/ directory for exported reports and data files.")
