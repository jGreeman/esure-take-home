"""Gold layer: modelling the silver data into a star schema for analytics and reporting.
"""
import argparse
import logging
import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

from policy_etl.spark import get_spark

log = logging.getLogger("policy_etl.gold_ingestion")

TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSX"

SILVER_SCHEMA = {
    "event_id" : "string", "policy_id" :"string", "customer_id" : "string", "event_type" : "string",
    "event_ts" : "timestamp", "event_date" : "date",
    "product_type" : "string", "brand" : "string", "region" : "string",
    "premium_amount" : "decimal", "coverage_amount" : "decimal", "age_of_insured" : "int",
    "dq_flags" : , "_source_file" : "string", "_source_file_modified_at" : "timestamp", "_batch_id": "int", "_ingested_at" : "timestamp",
    "_silver_run_id": "int", "_silver_processed_at" : "timestamp"
}


FACT_COLUMNS = []


def validate_silver_schema(df: DataFrame) -> None:
    """Check that the silver DataFrame has the required columns and types"""
    problems = []
    for col, expected in SILVER_SCHEMA.items():
        if col not in df.columns:
            problems.append(f"{col}: missing")
        elif (actual := df.schema[col].dataType.simpleString()) != expected:
            problems.append(f"{col}: expected {expected}, got {actual}")
    if problems:
        raise ValueError("Silver schema mismatch: " + "; ".join(problems))


def read_silver(spark: SparkSession, input_path: str) -> DataFrame:
    """Read existing silver data and validates schema"""
    df = spark.read.parquet(input_path)
    validate_silver_schema(df)
    return df


def create_dim_product(df: DataFrame) -> DataFrame:
    """Create the product dimension table"""
    key = F.concat_ws("|", F.coalesce(F.col("product_type"), F.lit("unknown")), F.coalesce(F.col("brand"), F.lit("unknown")))
    return (df.select("product_type", "brand")
            .distinct()
            .withColumn("product_key", F.xxhash64(key))
           )


def create_dim_region(df: DataFrame) -> DataFrame:
    """Create the region dimension table"""
    return (df.select("region")
            .distinct()
            .withColumn("region_key", F.xxhash64(F.col("region")))
           )


def create_dim_customer(df: DataFrame) -> DataFrame:
    """Create the customer dimension table"""
    return (df.select("customer_id")
            .distinct()
            .withColumn("customer_key", F.xxhash64(F.col("customer_id"), 256))
           )


def create_dim_date(df: DataFrame) -> DataFrame:
    """Create the date dimension table"""
    df = (df.select("event_date").distinct()
         .withColumn("date_id", F.date_format("event_date", "yyyyMMdd").cast("int"))
         .withColumn("date_year", F.year("event_date"))
         .withColumn("date_month", F.month("event_date"))
         .withColumn("date_month_name", F.date_format("event_date", "MMMM"))
         .withColumn("date_day", F.dayofmonth("event_date"))
         .withColumn("date_quarter", F.quarter("event_date"))
         .withColumn("date_day_of_week", F.dayofweek("event_date"))
         .withColumn("date_day_name", F.date_format("event_date", "EEEE"))
         .withColumn("date_is_weekend", F.dayofweek("event_date").isin([1, 7]))
         .withColumnRenamed("event_date", "date")
         .select("date_id", "date", "date_year", "date_quarter", "date_month", "date_month_name",
            "date_day", "date_day_of_week", "date_day_name", "date_is_weekend")
    )
    return df


def create_fact_policy_event(df: DataFrame) -> DataFrame:
    """Create the policy event fact table"""
    df.select("")



# def deduplicate_events(df: DataFrame) -> DataFrame:
#     """Deduplicate events based on event_id, keeping the latest _processed_ts"""
#     window = (Window.partitionBy("event_id").
#               orderBy(F.col("_ingested_at").desc(), F.col("_source_file").desc())
#     )
#     return df.withColumn("row_num", F.row_number().over(window)).filter(F.col("row_num") == 1).drop("row_num")


def write_gold_dataframe(df: DataFrame, output_path: str,
                           write_mode: str = "overwrite") -> DataFrame:
    """Write the gold DataFrame to the output path in parquet format"""
    (df.select(*GOLD_COLUMNS)
    .repartition("event_date")
     .write.mode(write_mode)
     .option("partitionOverwriteMode", "static")
     .partitionBy("event_date")
     .parquet(output_path)
    )
    return df


def process(spark: SparkSession, input_path: str, output_path: str,
            write_mode: str = "overwrite") -> tuple[int, int, int, int]:
    """Reads and runs the transformations to gold layer"""
    df = read_silver(spark, input_path).cache()

    dim_dfs = []
    for dim in (dims):
        dim_dfs.append(create_dim(df))

    fact_df = create_fact(df, dim_dfs)

    write_gold_dataframes(fact_df, dim_dfs, output_path, write_mode)


def main() -> None:
    """Command-line entry point for gold ingestion"""
    p = argparse.ArgumentParser(description="Transform bronze policy event data into silver")
    p.add_argument("--input", default="data/silver/policy_events")
    p.add_argument("--output", default="data/gold/policy_events")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    n_bronze, n_invalid, n_valid, n_silver = process(get_spark(), args.input, args.output)


if __name__ == "__main__":
    main()
