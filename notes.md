spark_session:
In local mode, the session now binds the driver to 127.0.0.1 (localhost) with spark.driver.bindAddress and spark.driver.host.
It only does this when the master is local[...].
On a real cluster the executors are on other machines and need to reach the driver over the network, so forcing localhost there would break the job.

- Partitions, in a production environment I would partition on ingest date, or event data depending on layer, with this volume of data it's not needed but I have kept it to replicate a prod deployment.
  For this takehome task, this ingestion will be a full refresh, rebuilding the dataset completely on each run. It's a small dataset and adding incremental load will overcomplicate it.
  To scale it up I would look at adding an 'incremental' mode to append to each partition, either manually specifying processing window in the args or look at using MERGE with DeltaLake,
  additionally handling late arriving data and updated event data.

silver:
read_bronze: currently no mergeSchema as files are consistent, add it in future if schema is known to evolve, so that multiple files can be combined. basePath
validate that the bronze parquet files have the correct schema before transforming.
event_type is only 4 cases no whitespace or inconsistent typecase, added Lower() as a precaution
separate out the invalid rows from the bronze data, writing to a quarantine area with a reason, returning valid rows with the corrupted record column removed
policy_type contains json, so we parse it as a struct, then add in new columns for product_type and brand
event_timestamp, we parse using hardcoded timestamp format including timezone, then added to invalid reason if failed to parse

- Null Invalid fields: this marks any field with an invalid value e.g. negative age, no coverage etc as Null but retains the event provided there is enough identifiable event data,
  The DQ flags are added to these rows so that they can be identified as having invalid data, and filtered out downstream
  product_type alone is null in ~2500 rows, ignoring would reduce the dataset dramatically.
  If policy id, customer_id, timestamp and type are null then it can be quarantined.

Add event ID:

- no unique ID available in the dataset so we use F.sha2 + concatenate the unique identifiers for an event, to generate a unique hash, which will be consistent for everything with that key,
  not a random UUID, has to run after the null checks. This allows dedup, MERGE and JOIN
  Thoughts:

Pandas/Spark to read JSON and perform transformation
Data model, Facts/Dims vs normalised
Write to -> PostGres DB
Query PostGres + streamlit UI?

Assumptions:

Considerations:
permissive: keeps reading, will ignore additional columns from the given schema and Null missing columns

- failfast stops completely strict DC, dropmalformed drops bad data, lose data and don't see failures

Improvements:

Unit tests, reqs for production:

Scalability into Production
