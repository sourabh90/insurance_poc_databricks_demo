import sys
import os
import pytest
from pyspark.sql import SparkSession

# Make src/pipelines importable without installing the package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "pipelines"))


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("insurance-poc-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
