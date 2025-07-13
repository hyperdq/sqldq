import pytest
from pyspark.sql import SparkSession

from sqldq import SQLDQ


@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder.appName("TestApp").getOrCreate()
    yield spark
    spark.stop()


def test_create_sqldq_instance(spark_session):
    dq = SQLDQ.from_pyspark(spark=spark_session, default_max_rows=30)
    assert dq is not None, "Failed to create SQLDQ instance from Spark session"
