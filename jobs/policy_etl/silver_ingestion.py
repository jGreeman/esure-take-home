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
from pyspark.sql import Window

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

SILVER_COLUMNS = [
    "event_id", "policy_id", "customer_id", "event_type", "event_ts", "event_date",
    "product_type", "brand", "region", "premium_amount", "coverage_amount", "age_of_insured",
    "dq_flags", "_source_file", "_source_file_modified_at", "_batch_id", "_ingested_at",
    "_silver_run_id", "_silver_processed_at",
]

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
    reason = reason.when(F.col("event_ts").isNull(), F.lit("Invalid event_timestamp"))
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


def write_quarantined(invalid: DataFrame, output_path: str, write_mode: str = "overwrite") -> int:
    """Write invalid rows to quarantine path"""
    n = invalid.count()
    log.warning("Writing %d invalid rows to quarantine at %s", n, output_path)
    invalid.write.mode(write_mode).partitionBy("_ingest_date").parquet(output_path)
    return n


def parse_policy_type(df: DataFrame) -> DataFrame:
    """Parses the policy_type dict string into a struct with policy_type and brand fields"""
    pt_schema = "type STRING, brand STRING"
    parsed = F.from_json(F.regexp_replace("policy_type", r"\bNone\b", "null"), pt_schema)

    return (df.withColumn("_policy_type_parsed", parsed)
            .withColumn("product_type", F.col("_policy_type_parsed.type"))
            .withColumn("brand", F.col("_policy_type_parsed.brand"))
            .drop("_policy_type_parsed")
    )


def parse_event_timestamp(df: DataFrame) -> DataFrame:
    """Parse the event_timestamp string into a timestamp column"""
    return (df.withColumn("event_ts", F.to_timestamp("event_timestamp", TIMESTAMP_FORMAT))
            .withColumn("event_date", F.to_date("event_ts")))


def null_invalid_values(df: DataFrame) -> DataFrame:
    """Null out invalid values with Nulls, flag row"""
    return (df.withColumn("coverage_amount",
                          F.when(F.col("coverage_amount") < 0, None)
                          .otherwise(F.col("coverage_amount")))
                          .withColumn("age_of_insured",
                                      F.when((F.col("age_of_insured") < 0) |
                                             (F.col("age_of_insured") > 120), None)
                                      .otherwise(F.col("age_of_insured")))
    )


def add_data_quality_flags(df: DataFrame) -> DataFrame:
    """Add a column with an array for basic data quality flags"""
    flags = F.array(
        F.when(F.col("coverage_amount") < 0, F.lit("negative_coverage_amount")),
        F.when((F.col("age_of_insured") < 18) | (F.col("age_of_insured") > 120), F.lit("invalid_age_of_insured")),
        F.when(F.col("premium_amount").isNull(), F.lit("premium_missing")),
        F.when(F.col("product_type").isNull(), F.lit("product_type_missing")),
    )
    return df.withColumn("dq_flags", F.array_compact(flags))


def cast_money_to_decimal(df: DataFrame) -> DataFrame:
    """Convert money columns to DecimalType not double, to avoid floating point issues"""
    return (df.withColumn("premium_amount", F.col("premium_amount").cast("decimal(12,2)"))
            .withColumn("coverage_amount", F.col("coverage_amount").cast("decimal(14,2)"))
    )


def add_event_id(df: DataFrame) -> DataFrame:
    """Add a unique event_id column using a hash based on policy_id, customer_id, event_ts"""
    key = F.concat_ws("|",
                      F.col("policy_id"),
                      F.col("customer_id"),
                      F.date_format("event_ts", "yyyy-MM-dd'T'HH:mm:ss.SSS"),
                      F.lower(F.col("event_type"))
                    )
    return df.withColumn("event_id", F.sha2(key, 256))


def deduplicate_events(df: DataFrame) -> DataFrame:
    """Deduplicate events based on event_id, keeping the latest _processed_ts"""
    window = Window.partitionBy("event_id").orderBy(F.col("_ingested_at").desc(), F.col("_source_file").desc())
    return df.withColumn("row_num", F.row_number().over(window)).filter(F.col("row_num") == 1).drop("row_num")


def add_silver_ingestion_metadata(df: DataFrame, run_id: str) -> DataFrame:
    """Add silver ingestion metadata columns to the DataFrame"""
    return (df.withColumn("_silver_run_id", F.lit(run_id))
            .withColumn("_silver_processed_at", F.current_timestamp()))


def write_silver_dataframe(df: DataFrame, output_path: str, write_mode: str = "overwrite") -> int:
    """Write the silver DataFrame to the output path in parquet format"""
    (df.select(*SILVER_COLUMNS)
     .write.mode(write_mode)
     .option("partitionOverwriteMode", "static")
     .partitionBy("event_date")
     .parquet(output_path)
    )
    return df

def process(spark: SparkSession, input_path: str, output_path: str, quarantine_path: str, write_mode: str = "overwrite") -> int:
    """Transform bronze data in parquet format, validate, clean, add dq flags, write to silver layer"""
    log.info("Reading bronze data from %s", input_path)
    df = read_bronze(spark, input_path)
    log.info("Parsing event_timestamp")
    df = parse_event_timestamp(df)
    log.info("Splitting corrupt and valid rows")
    valid, invalid = split_invalid_rows(df)
    log.info("Writing %d invalid rows to quarantine",
             write_quarantined(invalid, quarantine_path, write_mode))
    log.info("Parsing policy_type into struct")
    valid = parse_policy_type(valid)
    log.info("Adding data quality flags")
    valid = add_data_quality_flags(valid)
    log.info("Nulling invalid values")
    valid = null_invalid_values(valid)
    log.info("Converting currencies")
    valid = cast_money_to_decimal(valid)
    log.info("Adding event_id")
    valid = add_event_id(valid)
    log.info("Deduplicating events")
    valid = deduplicate_events(valid)
    log.info("Adding silver ingestion metadata")
    valid = add_silver_ingestion_metadata(valid, str(uuid.uuid4()))
    valid = valid.drop("event_timestamp").drop("policy_type").drop("_ingest_date")
    silver_df = write_silver_dataframe(valid, output_path, write_mode).cache
    total = silver_df.count()
    silver_df.unpersist()
    
    return total


def main() -> None:
    """Command-line entry point for silver ingestion"""
    p = argparse.ArgumentParser(description="Transform bronze policy event data into silver")
    p.add_argument("--input", default="data/bronze/policy_events")
    p.add_argument("--output", default="data/silver/policy_events")
    p.add_argument("--quarantine", default="data/quarantine/policy_events")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    process(get_spark(), args.input, args.output, args.quarantine)


if __name__ == "__main__":
    main()
