from pyspark.sql import SparkSession


def get_spark(app_name: str = "policy_etl", master: str = "local[*]") -> SparkSession:
    """Build or reuse a SparkSession. The master defaults to local[*]"""
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.adaptive.enabled", "true")
    )
    if master.startswith("local"):

        builder = builder.config("spark.driver.bindAddress", "127.0.0.1").config(
            "spark.driver.host", "127.0.0.1").config("spark.driver.memory", "2g"
        )
    return builder.getOrCreate()
