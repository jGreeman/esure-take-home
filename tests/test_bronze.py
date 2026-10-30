from policy_etl import bronze_ingestion


GOOD = '{"policy_id":"POL-1","customer_id":"CUS-1","event_type":"purchase","event_timestamp":"2024-01-01T00:00:00.000Z","policy_type":"{\'type\': \'auto\', \'brand\': \'X\'}","premium_amount":100.0,"coverage_amount":1000.0,"age_of_insured":30,"region":"North"}'
MISSING_FIELDS = '{"policy_id":"POL-2","customer_id":"CUS-2","event_type":"claim","event_timestamp":"2024-01-02T00:00:00.000Z","policy_type":"{\'type\': None, \'brand\': \'X\'}","coverage_amount":1.0,"region":"South"}'
WRONG_TYPE = '{"policy_id":"POL-3","age_of_insured":"forty"}'
NOT_JSON = "this is not json"


def write_lines(path, lines):
    path.write_text("\n".join(lines) + "\n")


def test_ingest_keeps_every_line_and_flags_corrupt(spark, tmp_path):
    src = tmp_path / "in"
    src.mkdir()
    write_lines(src / "a.json", [GOOD, MISSING_FIELDS, WRONG_TYPE, NOT_JSON])
    out = str(tmp_path / "bronze")

    assert bronze_ingestion.process(spark, str(src / "*.json"), out, "b1") == 4
    df = spark.read.parquet(out)
    assert df.count() == 4
    assert df.filter("_corrupt_record IS NOT NULL").count() == 2  # wrong type + not json
    row = df.filter("policy_id = 'POL-2'").first()
    assert row.premium_amount is None and row.age_of_insured is None  # missing keys -> nulls
    assert row._source_file.endswith("a.json") and row._batch_id == "b1"


def test_rerun_is_idempotent_and_picks_up_new_files(spark, tmp_path):
    src = tmp_path / "in"
    src.mkdir()
    write_lines(src / "a.json", [GOOD])
    out = str(tmp_path / "bronze")

    assert bronze_ingestion.process(spark, str(src / "*.json"), out) == 1
    assert bronze_ingestion.process(spark, str(src / "*.json"), out) == 0  # same file again: skipped
    write_lines(src / "b.json", [MISSING_FIELDS])
    assert bronze_ingestion.process(spark, str(src / "*.json"), out) == 1  # only the new file
    assert spark.read.parquet(out).count() == 2


def test_partitioned_by_ingest_date(spark, tmp_path):
    src = tmp_path / "in"
    src.mkdir()
    write_lines(src / "a.json", [GOOD])
    out = tmp_path / "bronze"

    bronze_ingestion.process(spark, str(src / "*.json"), str(out))
    row = spark.read.parquet(str(out)).first()
    assert row._ingest_date == row._ingested_at.date()
    assert list((out / f"_ingest_date={row._ingest_date}").glob("*.parquet"))
