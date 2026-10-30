from pyspark.sql import SparkSession

spark_session = SparkSession.builder.appName("ReadRaw").getOrCreate()


def get_spark_session(app_name: str = "policy_etl", master: str = "local[*]") -> SparkSession:
    """Local by default; on a cluster the master comes from spark-submit / the platform."""
    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

df = spark_session.read.json("policy_data/part-00000-tid-7993614477268990145-86e0c334-dc38-4ec8-8d9d-4217b6f7ac05-49801-1-c000.json")
df.show(20, truncate=False)