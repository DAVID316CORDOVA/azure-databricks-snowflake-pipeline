# Databricks notebook source
"""
WHAT this notebook does: reads the final, cleaned Delta table produced
by the DLT pipeline (silver_customers_clean) and writes it out as a
NEW, uniquely-named Parquet file to /clean/ on every run, so
Snowflake's Snowpipe can detect and auto-ingest each export as a
genuinely new file.

WHY a unique timestamped filename instead of overwriting the same path
every time: Snowpipe's AUTO_INGEST mechanism only reacts to files it
has never seen before landing in the monitored container. Overwriting
the same file path repeatedly means Snowpipe never observes a "new"
file event -- there is nothing for it to auto-detect. Using a distinct
timestamp per export guarantees every run produces a genuinely new
object in storage.

WHY no MERGE/upsert logic here: this notebook does a full snapshot
export of the CURRENT state of silver_customers_clean on every run --
it doesn't need to reconcile against a previous export, because each
export is self-contained and independent. Any deduplication against
what Snowflake already has (in case the same customer_id appears in
multiple exports over time) would happen on the Snowflake/dbt side,
using an incremental model with a MERGE strategy there -- not here.
"""

# COMMAND ----------

from pyspark.sql import functions as F

catalog = spark.conf.get("bundle.catalog", "dbw_fintech_fdcg01")
schema = spark.conf.get("bundle.schema", "dev")

df_clean = spark.table(f"{catalog}.{schema}.silver_customers_clean")

# Adds the export timestamp (UTC-5) as a column on every row -- lets
# Snowflake/downstream consumers see exactly when this batch was
# produced, in local business time.
df_clean = df_clean.withColumn(
    "exported_at_utc_minus_5",
    F.from_utc_timestamp(F.current_timestamp(), "Etc/GMT+5")
)

# Builds a single timestamp string (used in the FILE PATH itself, not
# just as a data column) -- this is what makes every export a distinct
# file for Snowpipe to detect.
export_timestamp = (
    df_clean
    .select(F.date_format(F.from_utc_timestamp(F.current_timestamp(), "Etc/GMT+5"), "yyyyMMdd_HHmmss"))
    .first()[0]
)

export_path = f"abfss://clean@stfintechpipeline01.dfs.core.windows.net/{schema}/customers_cleaned_{export_timestamp}"

# mode("overwrite") only matters as a safety net in the extremely
# unlikely case this exact timestamped path already existed -- it is
# not performing the "replace previous export" role anymore, since
# every export now has its own unique path.
df_clean.write.mode("overwrite").parquet(export_path)

print(f"Exported {df_clean.count()} records to {export_path}")