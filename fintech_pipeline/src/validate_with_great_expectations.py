# Databricks notebook source
"""
WHAT this notebook does: validates the exported customer dataset using
Great Expectations, acting as a quality gate BEFORE Snowpipe has a
chance to auto-ingest the Parquet file into Snowflake. This is
deliberately distinct from dbt-expectations: dbt's tests only run
AFTER data is already inserted into a table, whereas this notebook can
catch problems at the ingestion boundary itself, before Snowflake ever
sees a bad batch.

WHY this notebook includes a wide variety of expectation types (not
just the two or three used elsewhere in the project): the goal here is
explicitly educational -- covering column existence, null checks,
uniqueness, value ranges, set membership, string patterns, statistical
properties, and table-level shape checks, to build broad familiarity
with what Great Expectations can express declaratively.
"""

# COMMAND ----------

# Great Expectations is installed here, scoped to this notebook's
# session only -- it is not baked into the cluster's permanent
# environment, so this cell must run every time the cluster restarts.
%pip install great_expectations==0.18.19

# COMMAND ----------

# A %pip install always requires restarting the Python process before
# the newly installed library can be imported -- this is a Databricks
# requirement, not a Great Expectations one.
dbutils.library.restartPython()

# COMMAND ----------

import great_expectations as gx
from pyspark.sql.functions import col

catalog = spark.conf.get("bundle.catalog", "dbw_fintech_fdcg01")
schema = spark.conf.get("bundle.schema", "dev")

# Reads the same cleaned table that export_to_clean.py exports to
# Parquet -- this notebook validates the data BEFORE it becomes the
# Parquet file Snowpipe will pick up.
df_to_validate = spark.table(f"{catalog}.{schema}.silver_customers_clean")


df_to_validate = df_to_validate.withColumn("risk_score", col("metadata.risk_score"))


# get_context() initializes Great Expectations' internal configuration
# object -- it is what every subsequent GE operation is called through.
context = gx.get_context()

# Registers the Spark DataFrame above as a "batch" GE can validate.
# Unlike dbt (which always validates data already sitting in a
# database table), GE can validate an in-memory DataFrame directly --
# this is what allows the check to happen before the data is written
# anywhere permanent.
batch_request = (
    context.sources.add_spark("spark_datasource")
    .add_dataframe_asset("customers_clean_asset")
    .build_batch_request(dataframe=df_to_validate)
)

validator = context.get_validator(batch_request=batch_request)

# COMMAND ----------

# --- TABLE-LEVEL EXPECTATIONS ---
# These check properties of the dataset as a whole, not any single
# column.

# Confirms the expected columns are present, in the schema this
# notebook assumes -- catches upstream schema drift that removed or
# renamed a column entirely (as opposed to a value-level problem).
validator.expect_table_columns_to_match_set(
    column_set=[
        "customer_id", "customer_name", "age", "country", "email",
        "registration_date", "account_type", "account_balance",
        "kyc_status", "device", "language_preference",
        "notifications_enabled", "last_login_ip", "risk_score",
        "had_schema_drift", "is_valid_age", "is_valid_risk_score",
        "exported_at_utc_minus_5",
    ],
    exact_match=False,
)

# Sanity check on volume -- catches the case where an upstream failure
# silently produced an empty or near-empty export instead of failing
# loudly.
validator.expect_table_row_count_to_be_between(min_value=1, max_value=100000)

# COMMAND ----------

# --- NULLNESS / EXISTENCE EXPECTATIONS ---

# customer_id is the primary key downstream -- a null here is
# structurally critical, mirroring the same reasoning behind the
# expect_or_fail used on this same field in the DLT pipeline.
validator.expect_column_values_to_not_be_null("customer_id")

# email being present is treated as important but not as critical as
# the primary key -- mirrors the expect_or_drop severity level used
# for this field in the DLT pipeline.
validator.expect_column_values_to_not_be_null("email")

# COMMAND ----------

# --- UNIQUENESS EXPECTATIONS ---

# Confirms deduplication (already done upstream by dropDuplicates in
# the DLT pipeline) actually held -- a second, independent check on
# the same guarantee, at a different point in the pipeline.
validator.expect_column_values_to_be_unique("customer_id")

# COMMAND ----------

# --- RANGE / NUMERIC EXPECTATIONS ---

# Business rule: this fintech platform requires customers to be 18+.
# This dataset is expected to contain violations on purpose (the data
# generator injects underage records deliberately), so this
# expectation is expected to FAIL here -- that failure is itself the
# proof the check works.
validator.expect_column_values_to_be_between("age", min_value=18, max_value=100)

# risk_score's valid business range is 0-100 -- same expectation as
# used in dbt, checked here again at an earlier point in the pipeline.
validator.expect_column_values_to_be_between("risk_score", min_value=0, max_value=100)

# account_balance should never be negative for this business -- a
# distinct rule from the country-specific upper limit already checked
# in dbt (this is a lower-bound sanity check, not the same rule).
validator.expect_column_values_to_be_between("account_balance", min_value=0)

# COMMAND ----------

# --- SET MEMBERSHIP EXPECTATIONS ---
# Confirms categorical columns only contain values from a known,
# finite set -- catches a new/unexpected category appearing (e.g. a
# typo, or a genuinely new account_type the business hasn't accounted
# for downstream yet).

validator.expect_column_values_to_be_in_set(
    "account_type", value_set=["checking", "savings", "credit"]
)

validator.expect_column_values_to_be_in_set(
    "kyc_status", value_set=["pending", "verified", "rejected"]
)

# COMMAND ----------

# --- STRING PATTERN EXPECTATIONS ---
# Validates the SHAPE of a string value, not just its presence --
# catches malformed data that a simple not_null check would miss
# entirely (a non-null but garbage email still passes not_null).

validator.expect_column_values_to_match_regex(
    "email", regex=r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
)

# COMMAND ----------

# --- STATISTICAL / DISTRIBUTION EXPECTATIONS ---
# These check properties of the column's overall distribution, rather
# than validating each row independently -- useful for catching subtle
# data quality regressions that wouldn't trip a simple range check
# (e.g. a batch where every age is exactly 30, which is individually
# "valid" per-row but statistically implausible for a real customer
# base).

validator.expect_column_mean_to_be_between("age", min_value=18, max_value=80)

validator.expect_column_stdev_to_be_between("account_balance", min_value=1)

# COMMAND ----------

# --- TYPE EXPECTATIONS ---
# Confirms a column's underlying data type matches what's expected --
# catches the same category of problem the explicit .cast("double")
# in silver_customers_unified.sql was written to prevent, but checked
# here as an independent verification rather than relying solely on
# that upstream cast having worked correctly.

validator.expect_column_values_to_be_of_type("customer_id", type_="LongType")
validator.expect_column_values_to_be_of_type("account_balance", type_="DoubleType")

# COMMAND ----------

results = validator.validate()

print(f"Overall success: {results['success']}")
for result in results["results"]:
    expectation_type = result["expectation_config"]["expectation_type"]
    success = result["success"]
    print(f"  {expectation_type}: {'PASS' if success else 'FAIL'}")

# COMMAND ----------

# Note: this deliberately does NOT raise an exception on failure, even
# though several of the expectations above (age range, risk_score
# range) are expected to fail against this dataset's intentionally
# dirty records. Raising here would halt the pipeline every single
# run, which defeats the purpose of a project meant to demonstrate
# quality gates catching KNOWN, EXPECTED issues -- not to permanently
# block ingestion. A production system would likely distinguish
# between expectations whose failure should halt the pipeline (e.g.
# the primary key null check) versus those that should only be logged
# for review (e.g. the age/risk_score business rule violations this
# dataset intentionally contains).
print("Great Expectations validation completed -- see results above.")
