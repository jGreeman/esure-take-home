"""Gold layer: modelling the silver data into a star schema for analytics and reporting.
"""
import argparse
import logging
import os
import shutil
import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from policy_etl.spark import get_spark

log = logging.getLogger("policy_etl.gold_ingestion")

SILVER_SCHEMA = {
    "event_id" : "string", "policy_id" :"string", "customer_id" : "string", "event_type" : "string",
    "event_ts" : "timestamp", "event_date" : "date",
    "product_type" : "string", "brand" : "string", "region" : "string",
    "premium_amount" : "decimal(12,2)", "coverage_amount" : "decimal(14,2)",
    "age_of_insured" : "bigint", "dq_flags" : "array<string>", "_source_file" : "string",
    "_source_file_modified_at" : "timestamp", "_batch_id": "string", "_ingested_at" : "timestamp",
    "_silver_run_id": "string", "_silver_processed_at" : "timestamp"
}


FACT_COLUMNS = [
    "event_id", "policy_id", "event_type", "event_ts", "event_date",
    "customer_key", "product_key", "region_key", "date_key",
    "premium_amount", "coverage_amount", "age_of_insured",
    "dq_flags", "_source_file", "_batch_id", "_silver_run_id"
]


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


def product_key() -> F.Column:
    """Generate key using product_type and brand, Null goes 'unknown' member."""
    return F.xxhash64(
        F.coalesce(F.col("product_type"), F.lit("unknown")),
        F.coalesce(F.col("brand"), F.lit("unknown")),
    )


def region_key() -> F.Column:
    """Generate key for each region"""
    return F.xxhash64(F.coalesce(F.col("region"), F.lit("unknown")))


def customer_key() -> F.Column:
    """Generate key for each customer_id"""
    return F.xxhash64(F.col("customer_id"))


def date_key() -> F.Column:
    """Generate unique key for each date: Integer yyyymmdd, e.g. 20240108."""
    return F.date_format("event_date", "yyyyMMdd").cast("int")


def create_dim_product(df: DataFrame) -> DataFrame:
    """Create the product dimension table"""
    return (df.select(
                product_key().alias("product_key"),
                F.coalesce(F.col("product_type"), F.lit("unknown")).alias("product_type"),
                F.coalesce(F.col("brand"), F.lit("unknown")).alias("brand"),
            )
            .distinct()
           )


def create_dim_region(df: DataFrame) -> DataFrame:
    """Create the region dimension table"""
    return (df.select(
                region_key().alias("region_key"),
                F.coalesce(F.col("region"), F.lit("unknown")).alias("region"),
            )
            .distinct()
           )


def create_dim_customer(df: DataFrame) -> DataFrame:
    """Create the customer dimension table"""
    return (df.select(customer_key().alias("customer_key"), "customer_id")
            .distinct()
           )


def create_dim_date(df: DataFrame) -> DataFrame:
    """Create the date dimension table"""
    df = (df.select("event_date").distinct()
         .withColumn("date_key", date_key())
         .withColumn("date_year", F.year("event_date"))
         .withColumn("date_month", F.month("event_date"))
         .withColumn("date_month_name", F.date_format("event_date", "MMMM"))
         .withColumn("date_day", F.dayofmonth("event_date"))
         .withColumn("date_quarter", F.quarter("event_date"))
         .withColumn("date_day_of_week", F.dayofweek("event_date"))
         .withColumn("date_day_name", F.date_format("event_date", "EEEE"))
         .withColumn("date_is_weekend", F.dayofweek("event_date").isin([1, 7]))
         .withColumnRenamed("event_date", "date")
         .select("date_key", "date", "date_year", "date_quarter", "date_month", "date_month_name",
            "date_day", "date_day_of_week", "date_day_name", "date_is_weekend")
    )
    return df


def create_fact_policy_event(df: DataFrame) -> DataFrame:
    """One row per policy event (the grain), with a foreign key to each dimension.

    Foreign keys use the same key functions as the dimensions' primary keys"""
    return (df.withColumn("product_key", product_key())
              .withColumn("customer_key", customer_key())
              .withColumn("region_key", region_key())
              .withColumn("date_key", date_key())
              .select(*FACT_COLUMNS)
           )


def find_orphan_keys(fact: DataFrame, dim: DataFrame, key: str) -> int:
    """Count fact rows whose foreign key has no matching row in the dimension (nulls included)."""
    return fact.join(dim, on=key, how="left_anti").count()


def validate_gold(tables: dict[str, DataFrame], n_silver: int) -> None:
    """Check the star schema is consistent before anything is written. Raises on any problem.

    - every dimension primary key is unique
    - every fact foreign key resolves to a dimension row (no orphans)
    - the fact table has one row per silver event
    """
    fact = tables["fact_policy_event"]
    problems = []
    foreign_keys = {
    "product_key": "dim_product",
    "customer_key": "dim_customer",
    "region_key": "dim_region",
    "date_key": "dim_date",
    }

    for key, dim_name in foreign_keys.items():
        dim = tables[dim_name]
        n_rows, n_keys = dim.count(), dim.select(key).distinct().count()
        if n_rows != n_keys:
            problems.append(f"{dim_name}: {n_rows - n_keys} duplicate {key} values")
        orphans = find_orphan_keys(fact, dim, key)
        if orphans:
            problems.append(f"fact_policy_event: {orphans} rows with {key} not in {dim_name}")

    n_fact = fact.count()
    if n_fact != n_silver:
        problems.append(f"fact_policy_event has {n_fact} rows, silver has {n_silver}")

    if problems:
        raise ValueError("Gold validation failed: " + "; ".join(problems))



def write_staging_table(df: DataFrame, output_path: str, partition_by: str | None = None) -> None:
    """Full-refresh write of one gold table.

    The fact table is partitioned (one file per partition); dimensions are small,
    so each is written as a single unpartitioned file.
    """
    if partition_by:
        writer = df.repartition(partition_by).write.partitionBy(partition_by)
    else:
        writer = df.coalesce(1).write
    writer.mode("overwrite").option("partitionOverwriteMode", "static").parquet(output_path)


def promote_gold_staging_table(staging_path: str, gold_path: str, table_names: list[str]) -> None:
    """Following successful write to staging folder, moves all tables to data/gold,
    replacing the current live tables.
    """
    for name in table_names:
        live = os.path.join(gold_path, name)
        shutil.rmtree(live, ignore_errors=True)
        os.rename(os.path.join(staging_path, name), live)

    shutil.rmtree(os.path.join(gold_path, "_staging"), ignore_errors=True)



def process(spark: SparkSession, input_path: str, output_path: str,
            run_id: str | None = None) -> dict[str, int]:
    """Build the star schema from silver, validate it, then write and publish it.

    Validates table before writing, moves all tables on successful write to staging.
    """
    run_id = run_id or str(uuid.uuid4())
    log.info("Reading silver data from %s", input_path)
    events = read_silver(spark, input_path).cache()
    n_silver = events.count()

    tables = {
        "dim_product": create_dim_product(events),
        "dim_customer": create_dim_customer(events),
        "dim_region": create_dim_region(events),
        "dim_date": create_dim_date(events),
        "fact_policy_event": create_fact_policy_event(events),
    }
    for df in tables.values():
        df.cache()

    log.info("Validating gold tables")
    validate_gold(tables, n_silver)
    counts = {name: df.count() for name, df in tables.items()}

    staging = os.path.join(output_path, "_staging", run_id)
    log.info("Writing gold tables to staging at %s", staging)
    try:
        for name, df in tables.items():
            write_staging_table(df, os.path.join(staging, name),
                             partition_by="event_date" if name == "fact_policy_event" else None)
    except Exception:
        log.error("Write to staging failed; removing %s. Live gold tables are unchanged.", staging)
        shutil.rmtree(staging, ignore_errors=True)
        raise

    log.info("Promoting gold tables into %s", output_path)
    promote_gold_staging_table(staging, output_path, list(tables))

    for df in [events, *tables.values()]:
        df.unpersist()
    log.info("Gold run %s complete: %s", run_id, counts)
    return counts


def main() -> None:
    """Command-line entry point for gold ingestion"""
    p = argparse.ArgumentParser(description="Model silver policy events into a gold star schema")
    p.add_argument("--input", default="data/silver/policy_events")
    p.add_argument("--output", default="data/gold/")
    p.add_argument("--run-id", default=None)
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    process(get_spark(), args.input, args.output, args.run_id)


if __name__ == "__main__":
    main()
