"""Bronze layer: land raw JSON-lines files as-is, with ingestion metadata.

- Explicit schema: no inference pass over the data, and stable types.
- PERMISSIVE mode: malformed lines are kept in _corrupt_record and never fail the job.
- Idempotent: files already present in bronze are skipped, so a re-run or a
  retry after a failure never duplicates data.
"""
import argparse
import logging
import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from policy_etl.spark import get_spark

log = logging.getLogger("policy_etl.silver_ingestion")


REQUIRED_BRONZE_COLUMNS = {
    "policy_id": "string", "customer_id": "string", "event_type": "string",
    "event_timestamp": "string", "policy_type": "string",
    "premium_amount": "double", "coverage_amount": "double",
    "age_of_insured": "bigint", "region": "string",
    "_corrupt_record": "string", "_source_file": "string",
    "_batch_id": "string", "_ingested_at": "timestamp", "_ingest_date": "date",
}

TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSX"

REQUIRED_FIELDS = ["policy_id", "customer_id", "event_type", "event_timestamp"]
VALID_EVENT_TYPES = ["purchase", "renewal", "claim", "cancellation"]


def validate_bronze_schema(df: DataFrame) -> None:
    """Check that the bronze DataFrame has the required columns and types"""
    problems = []
    for col, expected in REQUIRED_BRONZE_COLUMNS.items():
        if col not in df.columns:
            problems.append(f"{col}: missing")
        elif (actual := df.schema[col].dataType.simpleString()) != expected:
            problems.append(f"{col}: expected {expected}, got {actual}")
    if problems:
        raise ValueError("Bronze schema mismatch: " + "; ".join(problems))


def read_bronze(spark: SparkSession, input_path: str) -> DataFrame:
    """Read existing bronze data and validates schema"""
    df = spark.read.parquet(input_path)
    validate_bronze_schema(df)
    return df


def add_invalid_reason(df: DataFrame) -> DataFrame:
    """Label each row with a reason it's corrupted, Null if valid"""
    reason = F.when(F.col("_corrupt_record").isNotNull(), F.lit("Corrupt record"))
    for field in REQUIRED_FIELDS:
        reason = reason.when(F.col(field).isNull(), F.lit(f"Missing required field: {field}"))
    reason = reason.when(F.col("event_timestamp").isNull(), F.lit("Invalid event_timestamp"))
     #TO DO reused timestamp column so won't be null if parse failed
    reason = reason.when(~F.lower(F.col("event_type"))
                         .isin(VALID_EVENT_TYPES), F.lit("Invalid event_type")
                        )
    return df.withColumn("_invalid_reason", reason)


def split_invalid_rows(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split a DataFrame into valid and invalid rows based on _invalid_reason"""
    df = add_invalid_reason(df)
    valid = (df.filter(F.col("_invalid_reason").isNull())
             .drop("_invalid_reason").drop("_corrupt_record")
            )
    invalid = (df.filter(F.col("_invalid_reason").isNotNull())
               .withColumn("_quarantined_timestamp", F.current_timestamp())
            )
    return valid, invalid


def write_quarantined(invalid: DataFrame, output_path: str) -> int:
    """Write invalid rows to quarantine path"""
    n = invalid.count()
    if n:
        log.warning("Writing %d invalid rows to quarantine at %s", n, output_path)
        invalid.write.mode("append").partitionBy("_ingest_date").parquet(output_path)
    return n


def parse_policy_type(df: DataFrame) -> DataFrame:
    """Parses the policy_type dict string into a struct with policy_type and brand fields"""
    pt_schema = "type STRING, brand STRING"
    parsed = F.from_json(F.regexp_replace("policy_type", r"\bNone\b", "null"), pt_schema)

    return (df.withColumn("_policy_type_parsed", parsed)
            .withColumn("policy_type", F.col("_policy_type_parsed.type"))
            .withColumn("brand", F.col("_policy_type_parsed.brand"))
            .drop("_policy_type_parsed")
    )


def parse_event_timestamp(df: DataFrame) -> DataFrame:
    """Parse the event_timestamp string into a timestamp column"""
    return (df.withColumn("event_timestamp", F.to_timestamp("event_timestamp", TIMESTAMP_FORMAT))
            .withColumn("event_date", F.to_date("event_timestamp")))


def null_invalid_values(df: DataFrame) -> DataFrame:
    """Null out invalid values with Nulls, flag row"""
    return (df.withColumn("coverage_amount",
                          F.when(F.col("coverage_amount") < 0, None)
                          .otherwise(F.col("coverage_amount")))
                          .withColumn("age_of_insured",
                                      F.when(F.col("age_of_insured") < 0 |
                                             (F.col("age_of_insured") > 120), None)
                                      .otherwise(F.col("age_of_insured")))
    )


def add_data_quality_flags(df: DataFrame) -> DataFrame:
    """Add a column with an array for basic data quality flags"""
    flags = F.array(
        F.when(F.col("coverage_amount") < 0, F.lit("negative_coverage_amount")),
        F.when(F.col("age_of_insured") < 18 | F.col("age_of_insured") > 120, F.lit("invalid_age_of_insured")),
        F.when(F.col("premium_amount").isNull(), F.lit("premium_missing")),
        F.when(F.col("product_type").isNull(), F.lit("product_type_missing")),
    )
    return df.withColumn("dq_flags", F.array_compact(flags))


def convert_currencies(df: DataFrame) -> DataFrame:
    """Convert money columns to DecimalType not double, to avoid floating point issues"""
    return (df.withColumn("premium_amount", F.col("premium_amount").cast("decimal(12,2)"))
            .withColumn("coverage_amount", F.col("coverage_amount").cast("decimal(14,2)"))
    )


def add_metadata_(df: DataFrame) -> DataFrame:
    """Add silver ingestion metadata columns to the DataFrame"""

def process(spark: SparkSession, input_path: str, output_path: str, batch_id: str) -> int:
    """Transform bronze data in parquet format, validate, clean, add dq flags, write to silver layer"""
    log.info("Reading bronze data from %s", input_path)
    df = read_bronze(spark, input_path)
    log.info("Parsing event_timestamp")
    df = parse_event_timestamp(df)
    log.info("Splitting corrupt and valid rows")
    valid, invalid = split_invalid_rows(df)
    log.info("Writing %d invalid rows to quarantine", invalid.count())
    write_quarantined(invalid, output_path + "/quarantine")
    log.info("Parsing policy_type into struct")
    valid = parse_policy_type(valid)
    log.info("Adding data quality flags")
    valid = add_data_quality_flags(valid)
    log.info("Nulling invalid values")
    valid = null_invalid_values(valid)
    log.info("Converting currencies")
    valid = convert_currencies(valid)
    total = valid.count()
    log.info("Processing batch %s with %d records", batch_id, total)
    return total


def main() -> None:
    """Command-line entry point for silver ingestion"""
    p = argparse.ArgumentParser(description="Transform bronze policy event data into silver")
    p.add_argument("--input", default="data/bronze/policy_events")
    p.add_argument("--output", default="data/silver/policy_events")
    p.add_argument("--batch-id", default=None)
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    process(get_spark(), args.input, args.output, args.batch_id)


if __name__ == "__main__":
    main()
