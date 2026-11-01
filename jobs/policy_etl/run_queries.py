"""Run the analyst SQL in sql/ against the gold star schema and print the results.

Each gold table is registered as a temporary view under its own name, so the SQL
reads exactly as it would against a catalog (Databricks, Glue/Athena, Hive).
"""
import argparse
import logging
import os

from pyspark.sql import DataFrame, SparkSession

from policy_etl.spark import get_spark

log = logging.getLogger("policy_etl.run_queries")

GOLD_TABLES = ["dim_product", "dim_customer", "dim_region", "dim_date", "fact_policy_event"]


def register_gold_views(spark: SparkSession, gold_path: str) -> None:
    """Expose each gold table as a SQL view, e.g. fact_policy_event, dim_product."""
    for table in GOLD_TABLES:
        spark.read.parquet(os.path.join(gold_path, table)).createOrReplaceTempView(table)


def run_query(spark: SparkSession, sql_file: str) -> DataFrame:
    with open(sql_file) as f:
        return spark.sql(f.read())


def main() -> None:
    p = argparse.ArgumentParser(description="Run the analyst queries against the gold layer")
    p.add_argument("--gold", default="data/gold")
    p.add_argument("--sql-dir", default="sql")
    p.add_argument("--query", default=None, help="Run one file only, e.g. 01_active_customers.sql")
    p.add_argument("--rows", type=int, default=20, help="Max rows to show per query")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = get_spark(app_name="policy_etl_queries")
    spark.sparkContext.setLogLevel("ERROR")
    register_gold_views(spark, args.gold)

    files = [args.query] if args.query else sorted(f for f in os.listdir(args.sql_dir) if f.endswith(".sql"))
    for name in files:
        print(f"\n=== {name} ===")
        run_query(spark, os.path.join(args.sql_dir, name)).show(args.rows, truncate=False)


if __name__ == "__main__":
    main()
