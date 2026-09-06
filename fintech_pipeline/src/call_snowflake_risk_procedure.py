# Databricks notebook source
"""
WHAT this notebook does: connects to Snowflake and calls the
categorize_customer_risk() stored procedure, which uses Snowpark
(Python running natively inside Snowflake's engine) to categorize
customers by risk score into a new Gold-layer table.
"""

# COMMAND ----------

%pip install snowflake-connector-python

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import snowflake.connector

schema = spark.conf.get("bundle.schema", "dev")
database = "FINTECH_ANALYTICS" if schema == "dev" else "FINTECH_ANALYTICS_PROD"

conn = snowflake.connector.connect(
    account="MZJLBCT-CUC08629",
    user="david",
    password=dbutils.secrets.get(scope="fintech-secrets", key="snowflake-password"),
    role="ACCOUNTADMIN",
    warehouse="COMPUTE_WH",
    database=database,
)

cursor = conn.cursor()
cursor.execute(f"CALL {database}.GOLD.categorize_customer_risk()")
result = cursor.fetchone()
print(f"Snowflake procedure result: {result}")

cursor.close()
conn.close()