import psycopg2
import pytest

from sqldq import SQLDQ


@pytest.fixture(scope="session")
def postgres_connection():
    # Create a PostgreSQL connection
    conn = psycopg2.connect(
        host="postgres",
        port="5432",
        dbname="sqldq_test",
        user="admin",
        password="admin",
    )
    yield conn
    conn.close()


def test_create_sqldq_instance(postgres_connection):
    dq = SQLDQ.from_postgresql(connection=postgres_connection, default_max_rows=30)
    assert dq is not None, "Failed to create SQLDQ instance from PostgreSQL connection"
