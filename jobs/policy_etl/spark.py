from pyspark.sql import SparkSession


def get_spark(app_name: str = "policy_etl", master: str = "local[*]") -> SparkSession:
    """Local by default; on a cluster the master comes from spark-submit / the platform."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.adaptive.enabled", "true")
    )
    if master.startswith("local"):
        # Bind the driver to loopback when running locally. Otherwise Spark resolves the
        # machine's hostname, which on laptops (VPN, wifi changes) can map to an address it
        # can't bind, failing with "Service 'sparkDriver' failed after 16 retries".
        # Not applied on a cluster, where executors must reach the driver over the network.
        builder = builder.config("spark.driver.bindAddress", "127.0.0.1").config(
            "spark.driver.host", "127.0.0.1").config("spark.driver.memory", "2g"
        )
    return builder.getOrCreate()
