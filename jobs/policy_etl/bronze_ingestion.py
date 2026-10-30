"""Silver layer: transform bronze data into a more structured format.

- Explicit schema: no inference pass over the data, and stable types.
- PERMISSIVE mode: malformed lines are kept in _corrupt_record and never fail the job.
- Idempotent: files already present in silver are skipped, so a re-run or a
  retry after a failure never duplicates data.
"""
import argparse
import logging
import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from policy_etl.spark import get_spark

log = logging.getLogger("policy_etl.bronze_ingestion")

CORRUPT_RECORD_COL = "_corrupt_record"
PARTITION_COLS = ["_ingest_date"]

RAW_SCHEMA = StructType(
    [
        StructField("policy_id", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("event_type", StringType(), nullable=True),
        StructField("event_timestamp", StringType(), nullable=True),  # ISO-8601, parsed in silver
        StructField("policy_type", StringType(), nullable=True),  # "{'type': 'auto', 'brand': ...}"
        StructField("premium_amount", DoubleType(), nullable=True),
        StructField("coverage_amount", DoubleType(), nullable=True),
        StructField("age_of_insured", LongType(), nullable=True),
        StructField("region", StringType(), nullable=True),
        # Spark fills this with the raw line when a record can't be parsed against the schema
        StructField(CORRUPT_RECORD_COL, StringType(), nullable=True),
    ]
)

def read_raw(spark: SparkSession, input_path: str) -> DataFrame:
    """Read raw JSON files with schema and Permissive Mode"""
    return (
        spark.read.schema(RAW_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COL)
        .json(input_path)
    )


def add_ingestion_metadata(df: DataFrame, batch_id: str) -> DataFrame:
    return (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source_file_modified_at", F.col("_metadata.file_modification_time"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingest_date", F.to_date("_ingested_at"))
        .withColumn("_batch_id", F.lit(batch_id))
    )


def drop_already_ingested(spark: SparkSession, df: DataFrame, target_path: str) -> DataFrame:
    try:
        seen = spark.read.parquet(target_path).select("_source_file").distinct()
    except AnalysisException:  # first run: bronze doesn't exist yet
        return df
    return df.join(F.broadcast(seen), on="_source_file", how="left_anti")


def write_to_bronze(df: DataFrame, target_path: str) -> None:
    df.write.mode("append").partitionBy(*PARTITION_COLS).parquet(target_path)


def process(spark: SparkSession, input_path: str, target_path: str, batch_id: str | None = None) -> int:
    batch_id = batch_id or str(uuid.uuid4())
    df = add_ingestion_metadata(read_raw(spark, input_path), batch_id)
    df = drop_already_ingested(spark, df, target_path).cache()

    total = df.count()
    if total == 0:
        log.info("No new files to ingest from %s", input_path)
        return 0
    corrupt = df.filter(F.col(CORRUPT_RECORD_COL).isNotNull()).count()

    write_to_bronze(df, target_path)
    log.info("batch=%s ingested=%d corrupt=%d -> %s", batch_id, total, corrupt, target_path)
    df.unpersist()
    return total


def main() -> None:
    p = argparse.ArgumentParser(description="Ingest raw policy event JSON into bronze")
    p.add_argument("--input", default="policy_data/*.json")
    p.add_argument("--output", default="data/bronze/policy_events")
    p.add_argument("--batch-id", default=None)
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    process(get_spark(), args.input, args.output, args.batch_id)


if __name__ == "__main__":
    main()
