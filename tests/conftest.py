import pytest

from policy_etl.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    """One local Spark session for the whole test run, built with the same config as the pipeline."""
    s = get_spark(app_name="tests", master="local[1]")
    s.conf.set("spark.sql.shuffle.partitions", "1")  # tiny test data: avoid 200 empty shuffle tasks
    yield s
    s.stop()
