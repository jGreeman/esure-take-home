"""End-to-end test of the full pipeline: one run_id flows through every layer."""
import os

from policy_etl.etl_pipeline import run_pipeline

EVENT = ('{"policy_id":"POL-%d","customer_id":"CUS-1","event_type":"renewal",'
         '"event_timestamp":"2024-01-08T10:00:00.000Z","policy_type":"{\'type\': \'auto\', \'brand\': \'X\'}",'
         '"premium_amount":100.0,"coverage_amount":1000.0,"age_of_insured":30,"region":"North"}')


def test_run_pipeline_shares_one_run_id_across_layers(spark, tmp_path):
    """Run the full pipeline and check that the same run_id is present in every layer."""
    landing = tmp_path / "landing"
    landing.mkdir()
    (landing / "events.json").write_text("\n".join(EVENT % i for i in range(3)) + "\n")
    paths = {name: str(tmp_path / name) for name in ("bronze", "silver", "gold", "quarantine")}

    run_id = run_pipeline(spark, str(landing), paths["bronze"], paths["silver"],
                          paths["gold"], paths["quarantine"], run_id="run-123")

    assert run_id == "run-123"
    bronze = spark.read.parquet(paths["bronze"])
    silver = spark.read.parquet(paths["silver"])
    fact = spark.read.parquet(os.path.join(paths["gold"], "fact_policy_event"))
    assert {r._batch_id for r in bronze.collect()} == {"run-123"}
    assert {r._silver_run_id for r in silver.collect()} == {"run-123"}
    assert {r._silver_run_id for r in fact.collect()} == {"run-123"}
    assert fact.count() == silver.count() == bronze.count() == 3
