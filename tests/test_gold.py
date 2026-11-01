"""Unit tests for the gold layer: dimensions, fact keys, validation and the publish step."""
import os
from datetime import date, datetime
from decimal import Decimal

import pytest

from policy_etl import gold_ingestion as gold

SILVER_DDL = ", ".join(f"{col} {dtype}" for col, dtype in gold.SILVER_SCHEMA.items())


def silver_row(**overrides):
    """A valid silver row; override only the fields a test cares about."""
    row = {
        "event_id": "e1", "policy_id": "POL-1", "customer_id": "CUS-1", "event_type": "renewal",
        "event_ts": datetime(2024, 1, 8, 10, 0), "event_date": date(2024, 1, 8),
        "product_type": "auto", "brand": "SafeGuard", "region": "North",
        "premium_amount": Decimal("500.00"), "coverage_amount": Decimal("10000.00"),
        "age_of_insured": 40, "dq_flags": [], "_source_file": "file:/a.json",
        "_source_file_modified_at": datetime(2026, 1, 1), "_batch_id": "b1",
        "_ingested_at": datetime(2026, 1, 1), "_silver_run_id": "s1",
        "_silver_processed_at": datetime(2026, 1, 1),
    }
    row.update(overrides)
    return row


def silver_df(spark, *rows):
    return spark.createDataFrame([tuple(r[c] for c in gold.SILVER_SCHEMA) for r in rows], SILVER_DDL)


def build_tables(events):
    return {
        "dim_product": gold.create_dim_product(events),
        "dim_customer": gold.create_dim_customer(events),
        "dim_region": gold.create_dim_region(events),
        "dim_date": gold.create_dim_date(events),
        "fact_policy_event": gold.create_fact_policy_event(events),
    }


def test_dim_product_has_one_row_per_product_and_an_unknown_member(spark):
    """Null policy types become an explicit 'unknown' row, so those events still join."""
    events = silver_df(spark,
                       silver_row(event_id="e1"),
                       silver_row(event_id="e2"),                       # same product again
                       silver_row(event_id="e3", product_type=None))    # no type in the source
    dim = gold.create_dim_product(events)

    rows = sorted((r.product_type, r.brand) for r in dim.collect())
    assert rows == [("auto", "SafeGuard"), ("unknown", "SafeGuard")]
    assert dim.select("product_key").distinct().count() == 2


def test_dim_date_attributes(spark):
    """2024-01-06 was a Saturday: check the key format and the derived calendar columns."""
    row = gold.create_dim_date(silver_df(spark, silver_row(event_date=date(2024, 1, 6)))).first()
    assert row.date_key == 20240106
    assert (row.date_year, row.date_quarter, row.date_month, row.date_day) == (2024, 1, 1, 6)
    assert (row.date_month_name, row.date_day_name, row.date_is_weekend) == ("January", "Saturday", True)


def test_fact_foreign_keys_match_dimension_primary_keys(spark):
    """Every fact row joins to every dimension, including the 'unknown' product."""
    events = silver_df(spark,
                       silver_row(event_id="e1"),
                       silver_row(event_id="e2", customer_id="CUS-2", product_type=None, region="South",
                                  event_date=date(2024, 2, 1)))
    tables = build_tables(events)
    fact = tables["fact_policy_event"]

    assert fact.count() == 2
    assert fact.columns == gold.FACT_COLUMNS  # descriptive attributes live in the dims, not here
    for key, dim_name in gold.FOREIGN_KEYS.items():
        assert gold.find_orphan_keys(fact, tables[dim_name], key) == 0, key


def test_validate_gold_passes_on_consistent_tables(spark):
    events = silver_df(spark, silver_row(event_id="e1"), silver_row(event_id="e2", product_type=None))
    gold.validate_gold(build_tables(events), n_silver=2)  # no exception


def test_validate_gold_reports_orphans_duplicates_and_row_count(spark):
    events = silver_df(spark, silver_row(event_id="e1"), silver_row(event_id="e2", product_type=None))
    tables = build_tables(events)
    tables["dim_product"] = tables["dim_product"].filter("product_type != 'unknown'")       # orphan e2
    tables["dim_region"] = tables["dim_region"].union(tables["dim_region"])                  # duplicate PK

    with pytest.raises(ValueError) as exc:
        gold.validate_gold(tables, n_silver=3)                                              # wrong count
    message = str(exc.value)
    assert "1 rows with product_key not in dim_product" in message
    assert "dim_region: 1 duplicate region_key values" in message
    assert "fact_policy_event has 2 rows, silver has 3" in message


def write_silver(spark, tmp_path, *rows):
    path = str(tmp_path / "silver")
    silver_df(spark, *rows).write.parquet(path)
    return path


def test_process_publishes_tables_and_replaces_them_on_rerun(spark, tmp_path):
    silver_path = write_silver(spark, tmp_path, silver_row(event_id="e1"),
                               silver_row(event_id="e2", customer_id="CUS-2"))
    gold_path = str(tmp_path / "gold")

    counts = gold.process(spark, silver_path, gold_path, run_id="run1")
    assert counts == {"dim_product": 1, "dim_customer": 2, "dim_region": 1, "dim_date": 1, "fact_policy_event": 2}
    assert spark.read.parquet(os.path.join(gold_path, "fact_policy_event")).count() == 2

    gold.process(spark, silver_path, gold_path, run_id="run2")
    assert spark.read.parquet(os.path.join(gold_path, "fact_policy_event")).count() == 2  # replaced, not appended
    assert sorted(os.listdir(gold_path)) == sorted(counts)  # only the live tables: staging cleaned up


def test_process_leaves_live_gold_untouched_if_a_write_fails(spark, tmp_path, monkeypatch):
    silver_path = write_silver(spark, tmp_path, silver_row())
    gold_path = str(tmp_path / "gold")
    gold.process(spark, silver_path, gold_path, run_id="good")
    live_before = sorted(os.listdir(os.path.join(gold_path, "fact_policy_event")))

    real_write = gold.write_staging_table

    def fail_on_fact(df, path, partition_by=None):
        if path.endswith("fact_policy_event"):
            raise IOError("disk full")
        real_write(df, path, partition_by)

    monkeypatch.setattr(gold, "write_staging_table", fail_on_fact)
    with pytest.raises(IOError):
        gold.process(spark, silver_path, gold_path, run_id="bad")

    assert sorted(os.listdir(os.path.join(gold_path, "fact_policy_event"))) == live_before
    assert not os.path.exists(os.path.join(gold_path, "_staging", "bad"))
