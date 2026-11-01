"""Single python script to run the pipeline from raw to gold layer."""
import argparse
import logging
import uuid

from pyspark.sql import SparkSession

from policy_etl.bronze_ingestion import process as bronze_process
from policy_etl.silver_ingestion import process as silver_process
from policy_etl.gold_ingestion import process as gold_process
from policy_etl.spark import get_spark

log = logging.getLogger("policy_etl.etl_pipeline")


def run_pipeline(spark: SparkSession, input_path: str = "policy_data",
                 bronze_path: str = "data/bronze/policy_events",
                 silver_path: str = "data/silver/policy_events",
                 gold_path: str = "data/gold",
                 quarantine_path: str = "data/quarantine/policy_events",
                 run_id: str | None = None) -> str:
    """Run bronze -> silver -> gold with one shared run_id, so every row in every
    layer can be traced back to the same pipeline run. Returns the run_id."""
    run_id = run_id or str(uuid.uuid4())
    log.info("Starting ETL pipeline run %s", run_id)

    log.info("Starting bronze ingestion from %s", input_path)
    bronze_process(spark, input_path, bronze_path, run_id=run_id)
    log.info("Starting silver ingestion")
    silver_process(spark, bronze_path, silver_path, quarantine_path, run_id=run_id)
    log.info("Starting gold ingestion")
    gold_process(spark, silver_path, gold_path, run_id=run_id)

    log.info("ETL pipeline run %s complete", run_id)
    return run_id


def main() -> None:
    """Command-line entry point for the full pipeline"""
    p = argparse.ArgumentParser(description="Run the policy ETL pipeline: raw JSON -> bronze -> silver -> gold")
    p.add_argument("--input_path", default="policy_data")
    p.add_argument("--bronze_path", default="data/bronze/policy_events")
    p.add_argument("--silver_path", default="data/silver/policy_events")
    p.add_argument("--gold_path", default="data/gold")
    p.add_argument("--quarantine_path", default="data/quarantine/policy_events")
    p.add_argument("--run-id", default=None)
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    run_pipeline(get_spark(), args.input_path, args.bronze_path, args.silver_path,
                 args.gold_path, args.quarantine_path, args.run_id)


if __name__ == "__main__":
    main()
