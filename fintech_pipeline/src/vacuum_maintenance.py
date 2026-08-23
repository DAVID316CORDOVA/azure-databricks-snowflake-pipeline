# Databricks notebook source
"""
WHAT this notebook does: runs Delta Lake's VACUUM command against the
cleaned customer table, permanently deleting physical files belonging
to old table versions that have exceeded the retention window.

WHY this is a separate, manual step -- even though this project uses
Delta Live Tables: DLT automates several maintenance behaviors (small
file compaction happens automatically as part of the pipeline's normal
operation, and Liquid Clustering keeps the table's physical layout
organized without needing a manual OPTIMIZE ZORDER command). VACUUM is
deliberately NOT automated by DLT, because it is a DESTRUCTIVE
operation -- once old file versions are vacuumed away, Delta's Time
Travel can no longer go back to see the table as it looked before that
point. Databricks intentionally leaves this decision to the engineer
rather than running it silently in the background.

WHY 168 hours (7 days) specifically: this is Delta Lake's own default
and recommended minimum retention window. Going lower risks breaking
any concurrent read that might still be relying on an older table
version (for example, a long-running query, or a downstream process
that hasn't yet caught up to the latest version).
"""

# COMMAND ----------

catalog = spark.conf.get("bundle.catalog", "dbw_fintech_fdcg01")
schema = spark.conf.get("bundle.schema", "dev")

spark.sql(f"VACUUM {catalog}.{schema}.silver_customers_clean RETAIN 168 HOURS")

print(f"VACUUM completed on {catalog}.{schema}.silver_customers_clean")