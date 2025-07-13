import os
import tempfile
from unittest.mock import Mock

import polars as pl
import pytest


# Test the main SQLDQ workflow - what users actually care about
class TestSQLDQWorkflow:
    """Test the complete SQLDQ workflow from a user's perspective."""

    @pytest.fixture
    def mock_backend(self):
        """Mock backend that simulates different query results."""
        backend = Mock()
        backend.execute_select_query = Mock()
        return backend

    @pytest.fixture
    def sqldq(self, mock_backend):
        """SQLDQ instance with mocked backend."""
        from sqldq import SQLDQ

        return SQLDQ(mock_backend)

    def test_user_can_run_checks_and_get_summary_report(self, sqldq, mock_backend):
        """Test: User adds checks, runs them, gets a readable report."""
        # Arrange - simulate some failed rows
        failed_data = pl.DataFrame({
            "user_id": [1, 2],
            "email": ["invalid1", "invalid2"],
            "created_at": ["2023-01-01", "2023-01-02"],
        })
        mock_backend.execute_select_query.return_value = failed_data

        # Act - user workflow
        sqldq.add_check(
            name="Invalid Emails",
            failure_rows_query="SELECT user_id, email, created_at FROM users WHERE email NOT LIKE '%@%'",
        )

        results = sqldq.execute()
        report = results.report()

        # Assert - check the output users see
        assert "Invalid Emails: ❌" in report
        assert "Summary: 1 checks, 1 failed" in report
        assert not results.results["Invalid Emails"].passed
        assert results.has_failures()

    def test_user_gets_clean_report_when_all_checks_pass(self, sqldq, mock_backend):
        """Test: When all checks pass, user gets a clean success report."""
        # Arrange - no failed rows
        mock_backend.execute_select_query.return_value = pl.DataFrame({"user_id": [], "email": [], "created_at": []})

        # Act
        sqldq.add_check(
            name="Valid Emails",
            failure_rows_query="SELECT user_id, email, created_at FROM users WHERE email NOT LIKE '%@%'",
        )

        results = sqldq.execute()
        report = results.report()

        # Assert
        assert "Valid Emails: ✅" in report
        assert "Summary: 1 checks, 0 failed" in report
        assert results.results["Valid Emails"].passed
        assert not results.has_failures()

    def test_user_can_see_failed_rows_in_detailed_report(self, sqldq, mock_backend):
        """Test: User can get detailed report showing actual failed data."""
        # Arrange
        failed_data = pl.DataFrame({"order_id": [101, 102, 103], "amount": [-50, -25, -100], "customer_id": [1, 2, 3]})
        mock_backend.execute_select_query.return_value = failed_data

        # Act
        sqldq.add_check(
            name="Negative Orders",
            failure_rows_query="SELECT order_id, amount, customer_id FROM orders WHERE amount < 0",
        )

        results = sqldq.execute()
        detailed_report = results.report(include_rows=True)

        # Assert - user can see the actual problematic data
        assert "Failed rows" in detailed_report
        assert "101" in detailed_report  # order_id
        assert "-50" in detailed_report  # negative amount
        assert "Negative Orders: ❌" in detailed_report

    def test_user_can_filter_report_to_only_failures(self, sqldq, mock_backend):
        """Test: User can get report showing only failed checks."""

        # Arrange - mix of passing and failing checks
        def mock_query_response(query, limit=None):
            if "< 0" in query.lower():
                return pl.DataFrame({"order_id": [101], "amount": [-50]})
            else:
                return pl.DataFrame({"order_id": [], "amount": []})

        mock_backend.execute_select_query.side_effect = mock_query_response

        # Act
        sqldq.add_check("Negative Orders", "SELECT order_id, amount FROM orders WHERE amount < 0")
        sqldq.add_check("Valid Orders", "SELECT order_id, amount FROM orders WHERE amount > 1000000")

        results = sqldq.execute()
        fail_only_report = results.report(fail_only=True)
        full_report = results.report(fail_only=False)

        # Assert
        assert "Negative Orders" in fail_only_report
        assert "Valid Orders" not in fail_only_report
        assert "Negative Orders" in full_report
        assert "Valid Orders" in full_report

    def test_user_gets_helpful_error_message_when_query_fails(self, sqldq, mock_backend):
        """Test: When SQL query fails, user gets readable error message."""
        # Arrange - simulate query failure
        mock_backend.execute_select_query.side_effect = Exception("Table 'nonexistent' not found")

        # Act
        sqldq.add_check(name="Bad Query", failure_rows_query="SELECT * FROM nonexistent_table")

        results = sqldq.execute()
        report = results.report()

        # Assert
        assert "Bad Query: ❌ ERROR" in report
        assert "Table 'nonexistent' not found" in report
        assert not results.results["Bad Query"].passed


class TestReportFormats:
    """Test different report output formats work correctly."""

    @pytest.fixture
    def sample_results(self):
        """Create sample check results for format testing."""
        from sqldq.sqldq import CheckResult, CheckResultSet

        # Passing check
        passing = CheckResult(name="Passing Check", failed_rows=None, limit=10)

        # Failing check with data
        failing_data = pl.DataFrame({"id": [1, 2], "issue": ["missing email", "invalid date"]})
        failing = CheckResult(name="Failing Check", failed_rows=failing_data, limit=10)

        # Error check
        error = CheckResult(name="Error Check", failed_rows=None, limit=10, error_message="SQL syntax error")

        return CheckResultSet({"passing": passing, "failing": failing, "error": error})

    def test_markdown_report_has_proper_structure(self, sample_results):
        """Test: Markdown report is properly formatted for documentation."""
        from sqldq.sqldq import OutputFormat

        # Act
        markdown = sample_results.report(output_format=OutputFormat.MARKDOWN, include_rows=True)
        print(markdown)

        # Assert
        assert "### Passing Check: ✅" in markdown
        assert "### Failing Check: ❌" in markdown
        assert "### Error Check: ❌ **ERROR**" in markdown
        assert "**Failed rows" in markdown  # Bold formatting
        assert "│" in markdown  # Table formatting

    def test_html_report_has_valid_structure(self, sample_results):
        """Test: HTML report can be saved and viewed in browser."""
        from sqldq.sqldq import OutputFormat

        # Act
        html = sample_results.report(output_format=OutputFormat.HTML, include_rows=True)

        # Assert
        assert "<h3>Passing Check: ✅</h3>" in html
        assert "<h3>Failing Check: ❌" in html
        assert "<table" in html  # Failed rows as HTML table
        assert "<b>Failed rows" in html

    def test_summary_header_provides_useful_overview(self, sample_results):
        """Test: Summary header gives user quick overview of results."""
        # Act
        report = sample_results.report(include_summary_header=True)

        # Assert
        assert "Data Quality Check Summary" in report
        assert "Total Checks: 3" in report
        assert "Passed: 1" in report
        assert "Failed: 2" in report


class TestFileExports:
    """Test file export functionality users rely on."""

    @pytest.fixture
    def temp_dir(self):
        """Temporary directory for file operations."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            yield tmp_dir

    @pytest.fixture
    def results_with_failures(self):
        """Check results with some failures to export."""
        from sqldq.sqldq import CheckResult, CheckResultSet

        failed_data = pl.DataFrame({
            "customer_id": [1, 2, 3],
            "email": ["invalid1", "invalid2", "invalid3"],
            "signup_date": ["2023-01-01", "2023-01-02", "2023-01-03"],
        })

        failing_check = CheckResult(name="Invalid Emails", failed_rows=failed_data, limit=10)

        return CheckResultSet({"invalid_emails": failing_check})

    def test_user_can_export_report_to_file(self, results_with_failures, temp_dir):
        """Test: User can save report to file for sharing."""
        # Act
        report_path = os.path.join(temp_dir, "data_quality_report.md")
        results_with_failures.export_report(report_path)

        # Assert
        assert os.path.exists(report_path)

        with open(report_path) as f:
            content = f.read()

        assert "Data Quality Check Summary" in content
        assert "Invalid Emails: ❌" in content

    def test_user_can_export_failed_rows_as_csv(self, results_with_failures, temp_dir):
        """Test: User can export failed rows for investigation."""
        from sqldq.sqldq import ExportFormat

        # Act
        results_with_failures.export_failed_rows(temp_dir, output_format=ExportFormat.CSV)

        # Assert
        expected_file = os.path.join(temp_dir, "invalid_emails.csv")
        assert os.path.exists(expected_file)

        # Verify the CSV contains the failed data
        exported_df = pl.read_csv(expected_file)
        assert len(exported_df) == 3
        assert "customer_id" in exported_df.columns
        assert "email" in exported_df.columns
        assert exported_df["customer_id"].to_list() == [1, 2, 3]

    def test_user_can_export_failed_rows_as_parquet(self, results_with_failures, temp_dir):
        """Test: User can export failed rows in parquet format."""
        from sqldq.sqldq import ExportFormat

        # Act
        results_with_failures.export_failed_rows(temp_dir, output_format=ExportFormat.PARQUET)

        # Assert
        expected_file = os.path.join(temp_dir, "invalid_emails.parquet")
        assert os.path.exists(expected_file)

        # Verify the parquet contains the failed data
        exported_df = pl.read_parquet(expected_file)
        assert len(exported_df) == 3
        assert "email" in exported_df.columns


class TestCheckConfiguration:
    """Test how users configure and manage checks."""

    @pytest.fixture
    def mock_backend(self):
        backend = Mock()
        backend.execute_select_query.return_value = pl.DataFrame({"id": [], "value": []})
        return backend

    def test_user_can_set_row_limits_per_check(self, mock_backend):
        """Test: User can control how many failed rows are captured per check."""
        from sqldq.sqldq import SQLDQ

        # Arrange
        large_failure_set = pl.DataFrame({
            "id": list(range(100)),  # 100 failed rows
            "issue": ["problem"] * 100,
        })
        mock_backend.execute_select_query.return_value = large_failure_set

        sqldq = SQLDQ(mock_backend)

        # Act - user sets different limits
        sqldq.add_check("Limited Check", "SELECT * FROM bad_data", max_rows=5)
        sqldq.add_check("Default Check", "SELECT * FROM bad_data")  # Uses default

        results = sqldq.execute()

        # Assert - limits are respected
        limited_result = results.results["Limited Check"]
        default_result = results.results["Default Check"]

        assert len(limited_result.failed_rows) == 5
        assert len(default_result.failed_rows) == 10  # Default limit

    def test_user_can_set_global_default_row_limit(self, mock_backend):
        """Test: User can set default row limit for all checks."""
        from sqldq.sqldq import SQLDQ

        # Arrange
        large_failure_set = pl.DataFrame({"id": list(range(50)), "issue": ["problem"] * 50})
        mock_backend.execute_select_query.return_value = large_failure_set

        # Act
        sqldq = SQLDQ(mock_backend, default_max_rows=3)
        sqldq.add_check("Test Check", "SELECT * FROM bad_data")

        results = sqldq.execute()

        # Assert
        assert len(results.results["Test Check"].failed_rows) == 3

    def test_user_can_specify_columns_to_include(self, mock_backend):
        """Test: User can control which columns appear in failed rows."""
        from sqldq import SQLDQ

        # Arrange - query returns many columns, user only wants some
        full_data = pl.DataFrame({
            "id": [1, 2],
            "name": ["John", "Jane"],
            "email": ["invalid1", "invalid2"],
            "created_at": ["2023-01-01", "2023-01-02"],
            "updated_at": ["2023-01-01", "2023-01-02"],
            "internal_field": ["secret1", "secret2"],
        })
        mock_backend.execute_select_query.return_value = full_data

        sqldq = SQLDQ(mock_backend)

        # Act - user specifies only the columns they want to see
        sqldq.add_check(
            "Invalid Emails", "SELECT * FROM users WHERE email NOT LIKE '%@%'", columns=["id", "email", "created_at"]
        )

        results = sqldq.execute()

        # Assert - backend is called with column selection
        expected_query = "WITH failures AS (SELECT * FROM users WHERE email NOT LIKE '%@%') SELECT id, email, created_at FROM failures"
        mock_backend.execute_select_query.assert_called_with(expected_query, limit=11)


class TestSQLDQFactoryMethods:
    """Test the factory methods users rely on for different databases."""

    def test_user_can_chain_method_calls_for_clean_setup(self):
        """Test: User can chain calls for readable setup code."""
        from sqldq import SQLDQ

        # Arrange
        mock_backend = Mock()
        mock_backend.execute_select_query.return_value = pl.DataFrame({"id": []})

        # Act - user can chain calls
        sqldq = (
            SQLDQ(mock_backend)
            .set_default_max_rows(5)
            .add_check("Check 1", "SELECT * FROM table1")
            .add_check("Check 2", "SELECT * FROM table2")
        )

        results = sqldq.execute()

        # Assert
        assert len(results.results) == 2
        assert sqldq.default_max_rows == 5


class TestEdgeCases:
    """Test edge cases users might encounter."""

    def test_empty_dataframe_results_are_handled_correctly(self):
        """Test: Empty results don't break the system."""
        from sqldq.sqldq import CheckResult

        # Act
        result = CheckResult(name="Empty Check", failed_rows=pl.DataFrame(), limit=10)

        # Assert
        assert result.passed is True
        assert result.failure_count == 0
        assert "Empty Check: ✅" in result.format_text()

    def test_no_checks_defined_returns_empty_results(self):
        """Test: Running execute with no checks doesn't crash."""
        from sqldq import SQLDQ

        # Arrange
        mock_backend = Mock()
        sqldq = SQLDQ(mock_backend)

        # Act
        results = sqldq.execute()

        # Assert
        assert len(results.results) == 0
        assert not results.has_failures()
        summary = results.get_summary()
        assert summary["total_checks"] == 0
