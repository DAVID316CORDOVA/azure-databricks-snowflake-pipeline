# Databricks notebook source

"""
DLT Pipeline: fintech customer data cleaning.

WHAT this file does: reads dirty customer JSON files landed in ADLS Gen2
by Azure Data Factory, cleans them through three progressive stages
(Bronze -> Silver unified -> Silver clean), and exposes data quality
metrics automatically via Delta Live Tables' declarative expectations.

WHY Delta Live Tables (instead of a plain PySpark notebook): DLT manages
the execution order between the three tables automatically (based on
which table reads from which), tracks lineage visually in the Databricks
UI, and turns data quality checks into a first-class, dashboard-visible
concept (@dlt.expect*) instead of manual print() statements buried in
code.

WHY three separate tables instead of one big transformation: each stage
has standalone inspection value. If something looks wrong in the final
clean table, being able to query bronze_customers or
silver_customers_unified directly -- without re-running anything --
makes debugging much faster than a single opaque transformation.
"""


import dlt
from pyspark.sql import functions as F

# Source: where Data Factory lands the raw, deliberately dirty JSON files.
raw_path = "abfss://raw@stfintechpipeline01.dfs.core.windows.net/"

# Autoloader needs a place to store its own bookkeeping: which files it
# has already seen (so it never reprocesses them), and the schema it has
# inferred so far. This lives separately from the actual data.
checkpoint_base = "abfss://clean@stfintechpipeline01.dfs.core.windows.net/_checkpoints"


# =====================================================================
# BRONZE: raw ingestion layer. No cleaning logic here on purpose --
# Bronze's only job is "get the data in, without losing anything."
# =====================================================================
@dlt.table(
    comment="Raw customer data landed via Autoloader with schema evolution (rescue mode)"
)
def bronze_customers():
    return (
        spark.readStream
        # "cloudFiles" is Databricks' Autoloader format. Unlike a plain
        # spark.read(), which re-reads every file in the folder on every
        # run, readStream + cloudFiles remembers (via the checkpoint)
        # which files it already processed, and only picks up new ones
        # on each pipeline run.
        .format("cloudFiles")
        .option("cloudFiles.format", "json")

        # WHY inferColumnTypes=true: without this, Autoloader can
        # sometimes misjudge nested structures (like the "metadata"
        # field, which contains a nested object with device info and a
        # risk_score) as plain strings instead of proper nested structs
        # -- this was hit and confirmed during development
        # (INVALID_EXTRACT_BASE_FIELD_TYPE error). Setting this true
        # makes Autoloader sample more data before deciding the schema,
        # producing a more accurate inference.
        .option("cloudFiles.inferColumnTypes", "true")

        # WHY schemaEvolutionMode="rescue" (not the default
        # "addNewColumns", and not "failOnNewColumns"): the upstream
        # generator deliberately injects schema drift into some records
        # (renamed fields, unexpected extra fields, changed data types)
        # to simulate what a real upstream system doing an uncoordinated
        # change would look like. "rescue" mode means: whatever doesn't
        # cleanly fit the inferred schema gets captured in a special
        # _rescued_data column instead of crashing the pipeline or
        # silently getting dropped. Nothing is ever lost, even when the
        # incoming data doesn't match expectations.
        .option("cloudFiles.schemaEvolutionMode", "rescue")

        .option("cloudFiles.schemaLocation", checkpoint_base + "/bronze_schema")
        .load(raw_path)
    )


# =====================================================================
# SILVER UNIFIED: resolves the KNOWN drift patterns from the generator,
# while preserving evidence that drift happened (rather than silently
# erasing it) -- see the "had_schema_drift" column below.
# =====================================================================
@dlt.table(
    comment="Schema-unified customer data: known drift resolved, evidence preserved"
)
def silver_customers_unified():
    # dlt.read_stream() (not spark.readStream) reads another DLT table
    # defined in THIS SAME file/pipeline, incrementally. This is what
    # lets DLT build its automatic dependency graph: it knows this
    # table depends on bronze_customers just from this line of code,
    # without any manual "run this after that" configuration.
    df = dlt.read_stream("bronze_customers")

    # Because Autoloader's schema can change from run to run (that's
    # the whole point of schema evolution), these drift-related columns
    # might or might not exist depending on what has actually arrived
    # so far. Checking for their existence before referencing them
    # avoids crashing on a column that simply hasn't shown up yet.
    has_rescued_col = "_rescued_data" in df.columns
    has_customer_name_camel = "customerName" in df.columns
    has_experimental_flag = "experimental_flag" in df.columns

    # WHY coalesce instead of just dropping "customerName": the
    # generator sometimes renames customer_name -> customerName to
    # simulate a real upstream rename. That field still carries real,
    # legitimate data (the actual customer's name) -- it would be a
    # genuine data loss to discard it. coalesce() takes whichever of
    # the two columns is non-null, so no legitimate name is lost just
    # because it arrived under the "wrong" column name.
    if has_customer_name_camel:
        df = df.withColumn(
            "customer_name",
            F.coalesce(F.col("customer_name"), F.col("customerName"))
        )

    # WHY track "had_schema_drift" instead of just silently fixing
    # things and moving on: knowing WHICH rows were affected by drift
    # is itself useful information -- for auditing, for debugging a
    # future upstream change, or for deciding whether to trust a
    # record's data as much as one that arrived "clean." This mirrors
    # the same design choice already used for age/risk_score validity
    # flags further down the pipeline: mark, don't hide.
    drift_condition = F.lit(False)
    if has_customer_name_camel:
        drift_condition = drift_condition | F.col("customerName").isNotNull()
    if has_experimental_flag:
        drift_condition = drift_condition | F.col("experimental_flag").isNotNull()
    if has_rescued_col:
        drift_condition = drift_condition | F.col("_rescued_data").isNotNull()

    df = df.withColumn("had_schema_drift", drift_condition)

    # Unlike customerName (which carried real data worth preserving),
    # "experimental_flag" is pure noise from the generator -- it never
    # represents any real business fact. Once its presence has already
    # been captured in had_schema_drift above, the column itself adds
    # nothing and can be safely dropped.
    if has_customer_name_camel:
        df = df.drop("customerName")
    if has_experimental_flag:
        df = df.drop("experimental_flag")

    # WHY an explicit cast here: schema evolution and type inference
    # only handle NEW columns appearing -- they do NOT automatically
    # convert an existing column's type (e.g. account_balance sometimes
    # arriving as a string instead of a number, another drift pattern
    # from the generator). That conversion is always a deliberate,
    # explicit decision in the code, never something DLT infers on its
    # own.
    df = df.withColumn("account_balance", F.col("account_balance").cast("double"))

    return df


# =====================================================================
# SILVER CLEAN: final, deduplicated, quality-checked table -- the one
# that actually gets consumed downstream (exported to Snowflake).
# =====================================================================
@dlt.table(
    comment="Deduplicated, validated customer data ready for Snowflake ingestion",

    # WHY cluster_by here: this table will typically be queried/filtered
    # by country (for regional reporting) and by kyc_status (for
    # compliance reviews of pending/rejected customers). Liquid
    # Clustering physically organizes the underlying files by these
    # columns automatically, on every write -- unlike traditional
    # Z-ORDER, which would need to be manually re-run (OPTIMIZE ... 
    # ZORDER BY ...) after every batch of new data to keep working.
    cluster_by=["country", "kyc_status"]
)
# The five decorators below are DLT's declarative data quality rules
# ("expectations"). Each one is evaluated per-row, automatically, and
# the results show up in the pipeline's built-in data quality
# dashboard -- no manual counting/printing required.

# expect_or_fail: the strictest possible enforcement. customer_id is
# the primary key every downstream join and aggregation depends on. A
# null here is not a "bad row to skip" -- it's a signal that something
# is structurally wrong upstream, serious enough to halt the entire
# pipeline run rather than silently propagate corrupted data forward.
@dlt.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL")

# expect_or_drop: moderate enforcement. A missing name or email is a
# real data quality problem, but not one that should stop everyone
# else's data from being processed -- the bad row is simply excluded
# from the output.
@dlt.expect_or_drop("valid_customer_name", "customer_name IS NOT NULL")
@dlt.expect_or_drop("valid_email", "email IS NOT NULL")

# expect (no suffix): the most lenient enforcement. Age and risk_score
# outside their valid ranges are flagged as failing the rule, but the
# row is KEPT in the table -- these are business rule violations worth
# investigating (e.g. an underage customer, per the fintech 18+
# requirement), not necessarily corrupted data that should disappear.
@dlt.expect("valid_age", "age BETWEEN 18 AND 100")
@dlt.expect("valid_risk_score", "metadata.risk_score BETWEEN 0 AND 100")
def silver_customers_clean():
    df = dlt.read_stream("silver_customers_unified")

    # Deduplicates by customer_id. The generator injects duplicate
    # records (simulating a re-sent or re-processed event) -- this is
    # the step that removes them, keeping one copy per customer_id.
    return df.dropDuplicates(["customer_id"])