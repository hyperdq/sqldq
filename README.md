# SQLDQ

`SQLDQ` is a Data Quality Check library that keeps it simple.

### Support

You can run data quality checks on:

- In-memory:
    - Pandas (`.from_duckdb`)
    - Polars (`.from_duckdb`)
    - Pyspark (`.from_pyspark`)
- Remotely, only results are collected:
    - Postgres (`.from_postgresql`)
    - AWS Athena (`.from_athena`)
- Everything else supported by DuckDB

### Examples

To see all of its features and examples for all supported backends, see the `demo` folder.

The basic workflow is as follows:

```python
from sqldq import SQLDQ
import duckdb
import polars as pl

# Sample data
df_users = pl.DataFrame({
    "user_id": [1, 2, 2],          # Duplicate user_id 4
    "age": [25, 150, 45],          # Age 150 is outlier
    "email": ["user1@example.com",
              "user2@example.com",
              "invalid-email"],    # Invalid email
})

# Connect via DuckDB
con = duckdb.connect()
con.register("users", df_users)

dq = SQLDQ.from_duckdb(connection=con)

# Define DQ checks
dq = (
    dq.add_check(
        name="check_duplicate_user_id",
        failure_rows_query="""
            WITH duplicate_users AS (
                SELECT user_id, COUNT(*) AS count
                FROM users
                GROUP BY user_id
            )
                SELECT user_id
                FROM duplicate_users
                WHERE count > 1""")
    .add_check(
        name="check_invalid_email",
        failure_rows_query="""
            SELECT user_id
            FROM users
            WHERE email NOT LIKE '%_@__%.__%'
        """)
    .add_check(
        name="check_age_outlier",
        failure_rows_query="""
            SELECT user_id, age
            FROM users
            WHERE age NOT BETWEEN 0 AND 120"""))

# Run checks
result = dq.execute()

# Report on results
report = result.report(include_rows=True,
                       include_summary_header=True,
                       fail_only=True)
print(report)

# Control flow
if result.has_failures():
    print("Checks failed. here we can take custom actions.")
```


# Development

```bash
# open devcontainer
uv sync
source .venv/bin/activate
make check
```
