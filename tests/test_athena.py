import boto3
import pytest

from sqldq import SQLDQ


@pytest.fixture(scope="session")
def session():
    session = boto3.Session(profile_name="default")
    yield session


def test_create_sqldq_instance(session):
    dq = SQLDQ.from_athena(boto3_session=session, workgroup="primary", database="sqldq_test", default_max_rows=30)
    assert dq is not None, "Failed to create SQLDQ instance from Athena client"
