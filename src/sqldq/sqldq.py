import io
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Optional, Union
from urllib.parse import urlparse

import boto3
import polars as pl

# Configure logging
logger = logging.getLogger(__name__)


class OutputFormat(Enum):
    """Supported output formats for reports."""

    TEXT = "text"
    MARKDOWN = "markdown"
    HTML = "html"
    RICHTEXT = "richtext"


class ExportFormat(Enum):
    """Supported export formats for failed rows."""

    CSV = "csv"
    PARQUET = "parquet"


@dataclass
class CheckDefinition:
    """Definition of a data quality check."""

    name: str
    failure_rows_query: str
    max_rows: int = 10
    columns: Optional[list[str]] = None


@dataclass
class CheckResult:
    """Result of a data quality check."""

    name: str
    failed_rows: Optional[pl.DataFrame] = None
    limit: int = 10
    error_message: Optional[str] = None

    @property
    def failure_count(self) -> int:
        """Get the number of failed rows."""
        if self.failed_rows is None:
            return 0
        return len(self.failed_rows)

    @property
    def passed(self) -> bool:
        """Check if the test passed."""
        if self.error_message:
            return False
        return self.failure_count < 1

    def format_text(self, include_rows: bool = False, max_rows: int = 10) -> str:
        """Format result as plain text."""
        if self.error_message:
            return f"{self.name}: ❌ ERROR - {self.error_message}"

        if self.passed:
            status = "✅"
        else:
            status = f"❌ (up to {self.limit} failures recorded)" if include_rows else "❌"

        out = [f"{self.name}: {status}"]

        if include_rows and not self.passed and self.failed_rows is not None:
            rows = self.failed_rows.head(max_rows)
            out.append(f"↳ Failed rows (max {max_rows} shown):\n{rows}")

        return "\n".join(out)

    def format_markdown(self, include_rows: bool = False, max_rows: int = 10) -> str:
        """Format result as markdown."""
        if self.error_message:
            return f"### {self.name}: ❌ **ERROR** - {self.error_message}\n"

        if self.passed:
            status = "✅"
        else:
            status = f"❌ **up to {self.limit} failures recorded**" if include_rows else "❌"

        md = f"### {self.name}: {status}\n"

        if include_rows and not self.passed and self.failed_rows is not None:
            md += f"\n**Failed rows (max {max_rows} shown):**\n\n"
            md += f"```\n{self.failed_rows.head(max_rows)}\n```"
            md += "\n"

        return md

    def format_html(self, include_rows: bool = False, max_rows: int = 10) -> str:
        """Format result as HTML."""
        if self.error_message:
            return f"<h3>{self.name}: ❌ <b>ERROR</b> - {self.error_message}</h3>"

        if self.passed:
            status = "✅"
        else:
            status = f"❌ <b>up to {self.limit} failures recorded</b>" if include_rows else "❌"

        html = f"<h3>{self.name}: {status}</h3>"

        if include_rows and not self.passed and self.failed_rows is not None:
            html += f"<p><b>Failed rows (max {max_rows} shown):</b></p>"
            html += self.failed_rows.head(max_rows).to_pandas().to_html(index=False)

        return html

    def format_richtext(self, include_rows: bool = False, max_rows: int = 10) -> str:
        """Format result using rich text (ANSI)."""
        if self.error_message:
            return f"[bold red]{self.name}: ERROR - {self.error_message}[/bold red]"

        if self.passed:
            header = f"[bold green]{self.name}: ✅[/bold green]"
        else:
            if include_rows:
                header = f"[bold red]{self.name}: ❌ (up to {self.limit} failures recorded)[/bold red]"
            else:
                header = f"[bold red]{self.name}: ❌[/bold red]"

        result = [header]
        if include_rows and not self.passed and self.failed_rows is not None:
            rows_df = self.failed_rows.head(max_rows)
            result.append(f"[bold yellow]↳ Failed rows (max {max_rows} shown):[/bold yellow]\n{rows_df}")
        return "\n".join(result)


class DatabaseBackend(ABC):
    """Abstract base class for database backends."""

    @abstractmethod
    def execute_select_query(self, query: str, limit: Optional[int] = None) -> pl.DataFrame:
        """Execute a select query and return results as Polars DataFrame."""
        pass


class DuckDBBackend(DatabaseBackend):
    """DuckDB backend implementation."""

    def __init__(self, connection):
        """Initialize with a DuckDB connection."""
        try:
            import duckdb
        except ImportError as e:
            raise ImportError("duckdb is required. Install with: pip install duckdb") from e

        if not isinstance(connection, duckdb.DuckDBPyConnection):
            raise TypeError("connection must be a DuckDB connection")

        self.connection = connection
        logger.info("DuckDB backend initialized")

    def execute_select_query(self, query: str, limit: Optional[int] = None) -> pl.DataFrame:
        """Execute a select query and return results as Polars DataFrame."""
        try:
            if limit:
                query = f"{query} LIMIT {limit}"
            return self.connection.execute(query).pl()
        except Exception:
            logger.exception("Error executing select query")
            raise


class AthenaBackend(DatabaseBackend):
    """Amazon Athena backend implementation."""

    def __init__(self, database: str, workgroup: str = "default", boto3_session: Optional[boto3.Session] = None):
        """Initialize with Athena database and workgroup."""
        try:
            import awswrangler as wr
        except ImportError as e:
            raise ImportError("awswrangler is required. Install with: pip install awswrangler") from e

        self.database = database
        self.workgroup = workgroup
        self.boto3_session = boto3_session
        self.wr = wr
        logger.info(f"Athena backend initialized with database: {database}")

    def execute_select_query(self, query: str, limit: Optional[int] = None) -> pl.DataFrame:
        """Execute a select query and return results as Polars DataFrame."""
        try:
            if limit:
                query = f"{query} LIMIT {limit}"
            result_df = self.wr.athena.read_sql_query(
                sql=query,
                database=self.database,
                workgroup=self.workgroup,
                boto3_session=self.boto3_session,
                ctas_approach=False,
            )
            return pl.from_pandas(result_df)
        except Exception:
            logger.exception("Error executing select query")
            raise


class PostgreSQLBackend(DatabaseBackend):
    """PostgreSQL backend implementation."""

    def __init__(self, connection: Any):
        """Initialize with a PostgreSQL connection."""
        try:
            import psycopg2  # noqa: F401
        except ImportError as e:
            raise ImportError("psycopg2 is required. Install with: pip install psycopg2-binary") from e

        self.connection: Any = connection
        logger.info("PostgreSQL backend initialized")

    def execute_select_query(self, query: str, limit: Optional[int] = None) -> pl.DataFrame:
        """Execute a select query and return results as Polars DataFrame."""
        try:
            import pandas as pd

            with self.connection.cursor() as cursor:
                if limit:
                    query = f"{query} LIMIT {limit}"
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                rows = cursor.fetchall()
                df = pd.DataFrame(data=rows, columns=columns)  # type: ignore[arg-type]
                return pl.from_pandas(df)
        except Exception:
            logger.exception("Error executing select query")
            raise


class PySparkBackend(DatabaseBackend):
    """PySpark backend implementation."""

    def __init__(self, spark):
        """Initialize with a Spark session."""
        try:
            from pyspark.sql import SparkSession
        except ImportError as e:
            raise ImportError("pyspark is required. Install with: pip install pyspark") from e

        if not isinstance(spark, SparkSession):
            raise TypeError("spark must be a SparkSession")

        self.spark = spark
        logger.info("PySpark backend initialized")

    def execute_select_query(self, query: str, limit: Optional[int] = None) -> pl.DataFrame:
        """Execute a select query and return results as Polars DataFrame."""
        try:
            spark_df = self.spark.sql(query)
            if limit:
                spark_df = spark_df.limit(limit)
            pandas_df = spark_df.toPandas()
            return pl.from_pandas(pandas_df)
        except Exception:
            logger.exception("Error executing select query")
            raise


class BaseExporter(ABC):
    """Abstract base class for exporters."""

    @abstractmethod
    def write_dataframe(self, df: pl.DataFrame, path: str, output_format: ExportFormat) -> None:
        """Write DataFrame to the specified path."""
        pass

    @abstractmethod
    def write_text(self, content: str, path: str) -> None:
        """Write text content to the specified path."""
        pass


class S3Exporter(BaseExporter):
    """Handles S3 export operations."""

    def __init__(self, boto3_session: boto3.Session):
        """Initialize with boto3 session."""
        self.s3_client = boto3_session.client("s3")
        logger.info("S3 exporter initialized")

    def write_dataframe(self, df: pl.DataFrame, path: str, output_format: ExportFormat) -> None:
        """Write DataFrame to S3."""
        try:
            parsed = urlparse(path)
            bucket = parsed.netloc
            key = parsed.path.lstrip("/")

            body = self._serialize_dataframe(df, output_format)

            self.s3_client.put_object(Bucket=bucket, Key=key, Body=body)
            logger.info(f"Successfully exported to s3://{bucket}/{key}")
        except Exception:
            logger.exception("Error writing to S3")
            raise

    def write_text(self, content: str, path: str) -> None:
        """Write text content to S3."""
        try:
            parsed = urlparse(path)
            bucket = parsed.netloc
            key = parsed.path.lstrip("/")

            self.s3_client.put_object(Bucket=bucket, Key=key, Body=content.encode())
            logger.info(f"Successfully exported text to s3://{bucket}/{key}")
        except Exception:
            logger.exception("Error writing text to S3")
            raise

    def _serialize_dataframe(self, df: pl.DataFrame, output_format: ExportFormat) -> bytes:
        """Serialize DataFrame to bytes based on format."""
        if output_format == ExportFormat.CSV:
            return df.write_csv().encode()
        elif output_format == ExportFormat.PARQUET:
            buf = io.BytesIO()
            df.write_parquet(buf)
            buf.seek(0)
            return buf.read()
        else:
            raise ValueError(f"Unsupported export format: {output_format}")


class LocalExporter(BaseExporter):
    """Handles local file export operations."""

    def write_dataframe(self, df: pl.DataFrame, path: str, output_format: ExportFormat) -> None:
        """Write DataFrame to local file."""
        try:
            self._ensure_directory_exists(path)

            if output_format == ExportFormat.CSV:
                df.write_csv(path)
            elif output_format == ExportFormat.PARQUET:
                df.write_parquet(path)
            else:
                self._raise_unsupported_format_error(output_format)  # Abstracted to method

        except Exception:
            logger.exception("Error writing to local file")
            raise
        else:
            logger.info(f"Successfully exported to {path}")

    def _raise_unsupported_format_error(self, output_format: ExportFormat) -> None:
        """Raise error for unsupported export format."""
        raise ValueError(f"Unsupported export format: {output_format}")

    def write_text(self, content: str, path: str) -> None:
        """Write text content to local file."""
        try:
            self._ensure_directory_exists(path)

            with open(path, "w", encoding="utf-8-sig") as f:
                f.write(content)

            logger.info(f"Successfully exported text to {path}")
        except Exception:
            logger.exception("Error writing text to local file")
            raise

    def _ensure_directory_exists(self, path: str) -> None:
        """Ensure the directory for the given path exists."""
        Path(path).parent.mkdir(parents=True, exist_ok=True)


class ExporterFactory:
    """Factory class for creating appropriate exporters."""

    @staticmethod
    def create_exporter(path: str, boto3_session: Optional[boto3.Session] = None) -> BaseExporter:
        """Create appropriate exporter based on path."""
        if path.startswith("s3://"):
            if not boto3_session:
                raise ValueError("S3 export requested but no boto3 session provided")
            return S3Exporter(boto3_session)
        else:
            return LocalExporter()


class CheckResultSet:
    """Container for check results with reporting and export capabilities."""

    def __init__(self, results: dict[str, CheckResult]):
        """Initialize with check results."""
        self.results = results

    def has_failures(self) -> bool:
        """Check if any checks failed."""
        return any(not r.passed for r in self.results.values())

    def report(
        self,
        output_format: Union[OutputFormat, str] = OutputFormat.TEXT,
        include_rows: bool = False,
        max_rows: int = 10,
        fail_only: bool = False,
        include_summary_header: bool = False,
    ) -> str:
        """Generate a report of all checks."""
        if isinstance(output_format, str):
            output_format = OutputFormat(output_format)

        checks = self.results.values() if not fail_only else [r for r in self.results.values() if not r.passed]

        parts = []
        if include_summary_header:
            parts.append(self._generate_summary_header(output_format))

        for result in checks:
            parts.append(self._format_single_result(result, output_format, include_rows, max_rows))

        if not include_summary_header:
            parts.append(self._generate_summary_footer(output_format))

        return "\n\n".join(parts)

    def _generate_summary_header(self, output_format: OutputFormat) -> str:
        """Generate summary header for the report."""
        total_checks = len(self.results)
        failed_checks = sum(1 for r in self.results.values() if not r.passed)
        passed_checks = total_checks - failed_checks

        if output_format == OutputFormat.HTML:
            return f"""<h2>Data Quality Check Summary</h2>
<p><strong>Total Checks:</strong> {total_checks}</p>
<p><strong>Passed:</strong> {passed_checks}</p>
<p><strong>Failed:</strong> {failed_checks}</p>
<hr>"""
        elif output_format == OutputFormat.MARKDOWN:
            return f"""## Data Quality Check Summary

**Total Checks:** {total_checks}
**Passed:** {passed_checks}
**Failed:** {failed_checks}

---"""
        elif output_format == OutputFormat.RICHTEXT:
            return f"""[bold blue]Data Quality Check Summary[/bold blue]

[bold]Total Checks:[/bold] {total_checks}
[bold]Passed:[/bold] [green]{passed_checks}[/green]
[bold]Failed:[/bold] [red]{failed_checks}[/red]

{"=" * 50}"""
        else:
            return f"""Data Quality Check Summary
{"=" * 30}
Total Checks: {total_checks}
Passed: {passed_checks}
Failed: {failed_checks}

{"=" * 50}"""

    def _generate_summary_footer(self, output_format: OutputFormat) -> str:
        """Generate summary footer for the report."""
        total_checks = len(self.results)
        failed_checks = sum(1 for r in self.results.values() if not r.passed)
        summary = f"{total_checks} checks, {failed_checks} failed"

        if output_format == OutputFormat.HTML:
            return f"<p><b>Summary:</b> {summary}</p>"
        elif output_format == OutputFormat.MARKDOWN:
            return f"**Summary:** {summary}"
        elif output_format == OutputFormat.RICHTEXT:
            return f"[bold]Summary:[/bold] {summary}"
        else:
            return f"Summary: {summary}"

    def _format_single_result(
        self, result: CheckResult, output_format: OutputFormat, include_rows: bool, max_rows: int
    ) -> str:
        """Format a single check result."""
        if output_format == OutputFormat.TEXT:
            return result.format_text(include_rows=include_rows, max_rows=max_rows)
        elif output_format == OutputFormat.MARKDOWN:
            return result.format_markdown(include_rows=include_rows, max_rows=max_rows)
        elif output_format == OutputFormat.HTML:
            return result.format_html(include_rows=include_rows, max_rows=max_rows)
        elif output_format == OutputFormat.RICHTEXT:
            return result.format_richtext(include_rows=include_rows, max_rows=max_rows)
        else:
            raise ValueError(f"Unsupported format: {output_format}")

    def export_report(
        self,
        output_path: str,
        output_format: Union[OutputFormat, str] = OutputFormat.MARKDOWN,
        include_rows: bool = False,
        max_rows: int = 10,
        fail_only: bool = False,
        include_summary_header: bool = True,
        boto3_session: Optional[boto3.Session] = None,
    ) -> None:
        """Export report to a file."""
        if isinstance(output_format, str):
            output_format = OutputFormat(output_format)

        content = self._generate_report_content(
            output_format, include_rows, max_rows, fail_only, include_summary_header
        )

        exporter = ExporterFactory.create_exporter(output_path, boto3_session)
        exporter.write_text(content, output_path)

    def _generate_report_content(
        self,
        output_format: OutputFormat,
        include_rows: bool,
        max_rows: int,
        fail_only: bool,
        include_summary_header: bool,
    ) -> str:
        """Generate the complete report content."""
        content = self.report(
            output_format=output_format,
            include_rows=include_rows,
            max_rows=max_rows,
            fail_only=fail_only,
            include_summary_header=include_summary_header,
        )

        if output_format == OutputFormat.HTML:
            content = self._wrap_html_content(content)

        return content

    def _wrap_html_content(self, content: str) -> str:
        """Wrap HTML content with proper document structure."""
        css = """
<style>
    body { font-family: Arial, sans-serif; margin: 20px; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
    th { background-color: #f2f2f2; }
</style>
"""
        return f"<!DOCTYPE html>\n<html>\n<head>\n<title>Data Quality Report</title>\n{css}\n</head>\n<body>\n{content}\n</body>\n</html>"

    def export_failed_rows(
        self,
        output_dir: str,
        output_format: Union[ExportFormat, str] = ExportFormat.CSV,
        max_rows: Optional[int] = None,
        boto3_session: Optional[boto3.Session] = None,
    ) -> None:
        """Export failed rows to files."""
        if isinstance(output_format, str):
            output_format = ExportFormat(output_format)

        exporter = ExporterFactory.create_exporter(output_dir, boto3_session)
        exported_count = 0

        for name, result in self.results.items():
            if self._should_export_result(result):
                file_path = self._generate_file_path(output_dir, name, output_format)
                df_to_export = self._prepare_export_dataframe(result.failed_rows, max_rows)

                exporter.write_dataframe(df_to_export, file_path, output_format)
                exported_count += 1

        logger.info(f"Exported {exported_count} failed row files")

    def _should_export_result(self, result: CheckResult) -> bool:
        """Check if a result should be exported."""
        return result.failed_rows is not None and result.error_message is None and not result.passed

    def _generate_file_path(self, output_dir: str, check_name: str, output_format: ExportFormat) -> str:
        """Generate file path for a check's failed rows."""
        file_name = f"{check_name.replace(' ', '_').lower()}.{output_format.value}"

        if output_dir.startswith("s3://"):
            return f"{output_dir.rstrip('/')}/{file_name}"
        else:
            return os.path.join(output_dir, file_name)

    def _prepare_export_dataframe(self, failed_rows: Optional[pl.DataFrame], max_rows: Optional[int]) -> pl.DataFrame:
        """Prepare DataFrame for export."""
        if failed_rows is None:
            raise ValueError("Failed rows is None")

        if max_rows:
            return failed_rows.head(max_rows)
        return failed_rows

    def get_summary(self) -> dict[str, Any]:
        """Get summary statistics of all checks."""
        total_checks = len(self.results)
        passed_checks = sum(1 for r in self.results.values() if r.passed)
        failed_checks = total_checks - passed_checks

        return {
            "total_checks": total_checks,
            "passed_checks": passed_checks,
            "failed_checks": failed_checks,
        }


class SQLDQ:
    """Simplified SQLDQ class for data quality checks."""

    def __init__(self, backend: DatabaseBackend, default_max_rows: int = 10):
        """Initialize with a database backend and default max rows."""
        self.backend = backend
        self.default_max_rows = default_max_rows
        self.check_definitions: dict[str, CheckDefinition] = {}
        logger.info(f"SQLDQ initialized with {type(backend).__name__} (default_max_rows={default_max_rows})")

    @classmethod
    def from_duckdb(cls, connection, default_max_rows: int = 10) -> "SQLDQ":
        """Create SQLDQ instance with DuckDB backend."""
        backend = DuckDBBackend(connection)
        return cls(backend, default_max_rows)

    @classmethod
    def from_athena(
        cls, database: str, workgroup: str, boto3_session: Optional[boto3.Session] = None, default_max_rows: int = 10
    ) -> "SQLDQ":
        """Create SQLDQ instance with Athena backend."""
        backend = AthenaBackend(database, workgroup, boto3_session)
        return cls(backend, default_max_rows)

    @classmethod
    def from_postgresql(cls, connection, default_max_rows: int = 10) -> "SQLDQ":
        """Create SQLDQ instance with PostgreSQL backend."""
        backend = PostgreSQLBackend(connection)
        return cls(backend, default_max_rows)

    @classmethod
    def from_pyspark(cls, spark, default_max_rows: int = 10) -> "SQLDQ":
        """Create SQLDQ instance with PySpark backend."""
        backend = PySparkBackend(spark)
        return cls(backend, default_max_rows)

    def set_default_max_rows(self, max_rows: int) -> "SQLDQ":
        """Set default max rows for all checks."""
        self.default_max_rows = max_rows
        logger.info(f"Default max rows set to {max_rows}")
        return self

    def add_check(
        self,
        name: str,
        failure_rows_query: str,
        max_rows: Optional[int] = None,
        columns: Optional[list[str]] = None,
    ) -> "SQLDQ":
        """Add a data quality check definition."""
        if name in self.check_definitions:
            logger.warning(f"Check '{name}' already exists, overwriting")

        effective_max_rows = max_rows if max_rows is not None else self.default_max_rows

        self.check_definitions[name] = CheckDefinition(
            name=name, failure_rows_query=failure_rows_query, max_rows=effective_max_rows, columns=columns
        )
        logger.info(f"Added check definition: {name} (max_rows={effective_max_rows})")
        return self

    def execute(self) -> CheckResultSet:
        """Execute all checks and return results."""
        if not self.check_definitions:
            logger.warning("No checks defined")
            return CheckResultSet({})

        results = {}
        for check_name, check_def in self.check_definitions.items():
            results[check_name] = self._execute_single_check(check_def)
        return CheckResultSet(results)

    def _execute_single_check(self, check_def: CheckDefinition) -> CheckResult:
        """Execute a single check definition."""
        logger.info(f"Executing check: {check_def.name}")

        try:
            select_cols = ", ".join(check_def.columns) if check_def.columns else "*"
            query = f"WITH failures AS ({check_def.failure_rows_query}) SELECT {select_cols} FROM failures"

            # Fetch more rows than the limit to detect if there are more failures
            fetch_limit = check_def.max_rows + 1
            failed_rows = self.backend.execute_select_query(query, limit=fetch_limit)

            # Only keep the rows up to the limit for the result
            if len(failed_rows) > check_def.max_rows:
                failed_rows = failed_rows.head(check_def.max_rows)

            result = CheckResult(
                name=check_def.name,
                failed_rows=failed_rows if len(failed_rows) > 0 else None,
                limit=check_def.max_rows,
            )

        except Exception as e:
            error_msg = f"Error running check: {e}"
            logger.exception(f"Check '{check_def.name}': {error_msg}")
            return CheckResult(name=check_def.name, failed_rows=None, limit=check_def.max_rows, error_message=error_msg)
        else:
            # Log status and return result only if check executed successfully
            status = "PASSED" if result.passed else f"FAILED (up to {check_def.max_rows} failures recorded)"
            logger.info(f"Check '{check_def.name}': {status}")
            return result

    def list_checks(self) -> list[str]:
        """List all defined check names."""
        return list(self.check_definitions.keys())

    def get_check_definition(self, name: str) -> Optional[CheckDefinition]:
        """Get a check definition by name."""
        return self.check_definitions.get(name)
