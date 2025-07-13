import duckdb
import pytest

from sqldq import SQLDQ


@pytest.fixture(scope="session")
def duckdb_connection():
    conn = duckdb.connect(database=":memory:", read_only=False)
    yield conn
    conn.close()


def test_create_sqldq_instance(duckdb_connection):
    dq = SQLDQ.from_duckdb(connection=duckdb_connection, default_max_rows=30)
    assert dq is not None, "Failed to create SQLDQ instance from DuckDB connection"
