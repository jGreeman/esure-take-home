"""Unit tests for the silver layer.

Each transform is tested in isolation on a few hand-built rows, then one
end-to-end test runs bronze -> silver on a small fixture file.
"""
from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from policy_etl import bronze_ingestion
from policy_etl import silver_ingestion as silver

BRONZE_DDL = (
    "policy_id string, customer_id string, event_type string, event_timestamp string, "
    "policy_type string, premium_amount double, coverage_amount double, age_of_insured bigint, "
    "region string, _corrupt_record string, _source_file string, _source_file_modified_at timestamp, "
    "_batch_id string, _ingested_at timestamp, _ingest_date date"
)
TS = "2024-03-01T10:00:00.000Z"
INGESTED = datetime(2026, 10, 30, 9, 0, 0)


def bronze_row(**overrides):
    """A valid bronze row; override only the fields a test cares about."""
    row = {
        "policy_id": "POL-1",
        "customer_id": "CUS-1",
        "event_type": "renewal",
        "event_timestamp": TS,
        "policy_type": "{'type': 'auto', 'brand': 'SafeGuard'}",
        "premium_amount": 500.0,
        "coverage_amount": 10000.0,
        "age_of_insured": 40,
        "region": "North",
        "_corrupt_record": None,
        "_source_file": "file:/landing/a.json",
        "_source_file_modified_at": INGESTED,
        "_batch_id": "b1",
        "_ingested_at": INGESTED,
        "_ingest_date": INGESTED.date(),
    }
    row.update(overrides)
    return row


def bronze_df(spark, *rows):
    cols = [c.split()[0] for c in BRONZE_DDL.split(", ")]
    return spark.createDataFrame([tuple(r[c] for c in cols) for r in rows], BRONZE_DDL)


def prepared(spark, *rows):
    """Bronze rows after timestamp parsing: the input most silver transforms expect."""
    return silver.parse_event_timestamp(bronze_df(spark, *rows))


# --- validation ---------------------------------------------------------

def test_validate_bronze_schema_reports_missing_and_wrong_type(spark):
    """Silver fails fast, listing every problem, if bronze isn't the shape it expects."""
    df = (bronze_df(spark, bronze_row())
          .drop("region")
          .withColumn("age_of_insured", F.col("age_of_insured").cast("string")))
    with pytest.raises(ValueError) as exc:
        silver.validate_bronze_schema(df)
    assert "region: missing" in str(exc.value)
    assert "age_of_insured: expected bigint, got string" in str(exc.value)


def test_parse_event_timestamp(spark):
    """Valid ISO timestamps parse and give event_date; anything else becomes null."""
    out = prepared(spark,
                   bronze_row(policy_id="ok", event_timestamp="2024-01-08T23:59:59.123Z"),
                   bronze_row(policy_id="bad", event_timestamp="08/01/2024 10:00"))
    rows = {r.policy_id: r for r in out.collect()}
    assert rows["ok"].event_ts == datetime(2024, 1, 8, 23, 59, 59, 123000)
    assert rows["ok"].event_date == date(2024, 1, 8)
    assert rows["bad"].event_ts is None


def test_add_invalid_reason(spark):
    """Each rule produces its reason; valid rows get null; corrupt records win."""
    df = prepared(spark,
                  bronze_row(policy_id="valid"),
                  bronze_row(policy_id="upper_case_type", event_type="RENEWAL"),
                  bronze_row(policy_id="no_customer", customer_id=None),
                  bronze_row(policy_id="bad_ts", event_timestamp="not a date"),
                  bronze_row(policy_id="bad_type", event_type="refund"),
                  bronze_row(policy_id="corrupt", _corrupt_record="not json", customer_id=None))
    got = {r.policy_id: r._invalid_reason for r in silver.add_invalid_reason(df).collect()}
    assert got == {
        "valid": None,
        "upper_case_type": None,
        "no_customer": "Missing required field: customer_id",
        "bad_ts": "Invalid event_timestamp",
        "bad_type": "Invalid event_type",
        "corrupt": "Corrupt record",
    }


def test_split_invalid_rows(spark):
    """Bad rows are separated out and labelled; valid rows lose the helper columns."""
    df = prepared(spark, bronze_row(), bronze_row(policy_id="POL-2", event_type="refund"))
    valid, invalid = silver.split_invalid_rows(df)
    assert valid.count() == 1 and invalid.count() == 1
    assert "_invalid_reason" not in valid.columns and "_corrupt_record" not in valid.columns
    assert "_quarantined_timestamp" in invalid.columns


# --- cleaning -----------------------------------------------------------

def test_parse_policy_type(spark):
    """The Python-dict string splits into product_type and brand, including None."""
    df = bronze_df(spark,
                   bronze_row(policy_id="auto"),
                   bronze_row(policy_id="none", policy_type="{'type': None, 'brand': 'InsureCorp'}"))
    got = {r.policy_id: (r.product_type, r.brand) for r in silver.parse_policy_type(df).collect()}
    assert got == {"auto": ("auto", "SafeGuard"), "none": (None, "InsureCorp")}


def test_dq_flags_then_null_invalid_values(spark):
    """Bad values are flagged, then impossible ones nulled. Under-18 ages are flagged but kept."""
    df = silver.parse_policy_type(bronze_df(
        spark,
        bronze_row(policy_id="clean"),
        bronze_row(policy_id="sentinel", coverage_amount=-2147483649.0),
        bronze_row(policy_id="negative_age", age_of_insured=-10),
        bronze_row(policy_id="child", age_of_insured=12),
        bronze_row(policy_id="gaps", premium_amount=None, policy_type="{'type': None, 'brand': 'X'}"),
    ))
    rows = {r.policy_id: r for r in silver.null_invalid_values(silver.add_data_quality_flags(df)).collect()}

    assert rows["clean"].dq_flags == []
    assert rows["sentinel"].coverage_amount is None
    assert rows["sentinel"].dq_flags == ["negative_coverage_amount"]
    assert rows["negative_age"].age_of_insured is None
    assert rows["child"].age_of_insured == 12 and rows["child"].dq_flags == ["invalid_age_of_insured"]
    assert set(rows["gaps"].dq_flags) == {"premium_missing", "product_type_missing"}


def test_cast_money_to_decimal(spark):
    out = silver.cast_money_to_decimal(bronze_df(spark, bronze_row(premium_amount=123.456)))
    assert dict(out.dtypes)["premium_amount"] == "decimal(12,2)"
    assert out.first().premium_amount == Decimal("123.46")


# --- event id and dedup -------------------------------------------------

def test_event_id(spark):
    """Same business key gives the same id, even if other values differ; a different key gives a new id."""
    df = silver.add_event_id(prepared(
        spark,
        bronze_row(policy_id="POL-1", premium_amount=500.0),
        bronze_row(policy_id="POL-1", premium_amount=999.0),   # corrected re-send
        bronze_row(policy_id="POL-1", customer_id="CUS-2"),     # different customer
    ))
    ids = [r.event_id for r in df.collect()]
    assert ids[0] == ids[1]
    assert ids[0] != ids[2]
    assert len(ids[0]) == 64  # sha256 hex


def test_deduplicate_keeps_latest_ingested_version(spark):
    df = silver.add_event_id(prepared(
        spark,
        bronze_row(premium_amount=100.0, _ingested_at=datetime(2026, 1, 1)),
        bronze_row(premium_amount=200.0, _ingested_at=datetime(2026, 2, 1)),
        bronze_row(customer_id="CUS-2"),  # a different event: kept
    ))
    out = silver.deduplicate_events(df)
    assert out.count() == 2
    assert out.filter("customer_id = 'CUS-1'").first().premium_amount == 200.0


# --- end to end ---------------------------------------------------------

GOOD = ('{"policy_id":"POL-1","customer_id":"CUS-1","event_type":"purchase",'
        '"event_timestamp":"2024-01-01T00:00:00.000Z","policy_type":"{\'type\': \'auto\', \'brand\': \'X\'}",'
        '"premium_amount":100.0,"coverage_amount":-2.147483649E9,"age_of_insured":30,"region":"North"}')
OTHER = GOOD.replace("POL-1", "POL-2")
BAD_TYPE = GOOD.replace("POL-1", "POL-3").replace('"purchase"', '"refund"')
NOT_JSON = "this is not json"


def run_pipeline(spark, tmp_path, lines):
    landing = tmp_path / "landing"
    landing.mkdir()
    (landing / "a.json").write_text("\n".join(lines) + "\n")
    bronze_path, silver_path, quarantine_path = (str(tmp_path / p) for p in ("bronze", "silver", "quarantine"))

    bronze_ingestion.process(spark, str(landing / "*.json"), bronze_path)
    counts = silver.process(spark, bronze_path, silver_path, quarantine_path)
    return counts, spark.read.parquet(silver_path), spark.read.parquet(quarantine_path)


def test_process_end_to_end(spark, tmp_path):
    """JSON -> bronze -> silver: duplicates removed, bad rows quarantined, values cleaned."""
    counts, silver_df, quarantine_df = run_pipeline(
        spark, tmp_path, [GOOD, GOOD, OTHER, BAD_TYPE, NOT_JSON]  # GOOD twice: a duplicate
    )
    n_bronze, n_invalid, n_valid, n_silver = counts

    assert (n_bronze, n_invalid, n_valid, n_silver) == (5, 2, 3, 2)
    assert n_bronze == n_silver + n_invalid + (n_valid - n_silver)  # every row accounted for
    assert sorted(silver_df.columns) == sorted(silver.SILVER_COLUMNS)
    assert sorted(r._invalid_reason for r in quarantine_df.collect()) == ["Corrupt record", "Invalid event_type"]

    row = silver_df.filter("policy_id = 'POL-1'").first()
    assert row.coverage_amount is None and "negative_coverage_amount" in row.dq_flags
    assert row.event_date == date(2024, 1, 1)


def test_process_is_idempotent(spark, tmp_path):
    """Re-running silver gives the same output: no duplicated silver or quarantine rows."""
    run_pipeline(spark, tmp_path, [GOOD, OTHER, BAD_TYPE])
    silver.process(spark, str(tmp_path / "bronze"), str(tmp_path / "silver"), str(tmp_path / "quarantine"))
    assert spark.read.parquet(str(tmp_path / "silver")).count() == 2
    assert spark.read.parquet(str(tmp_path / "quarantine")).count() == 1
