# Databricks notebook source
"""
WHAT this notebook does: runs the dbt project against Snowflake using
subprocess (dbt deps -> dbt build -> dbt docs generate), rather than
Databricks' native dbt_task, which proved unreliable for external
Snowflake targets authenticated via secret/password.
"""

# COMMAND ----------

%pip install dbt-core dbt-snowflake

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import subprocess
import os

target = spark.conf.get("bundle.schema", "dev")
bundle_target = spark.conf.get("bundle.target", "dev")

dbt_project_path = (
    f"/Workspace/Users/felix.david.cordova.garcia@gmail.com"
    f"/.bundle/fintech_pipeline/{bundle_target}/files/dbt_project"
)

snowflake_password = dbutils.secrets.get(scope="fintech-secrets", key="snowflake-password")
env = {**os.environ, "DBT_SNOWFLAKE_PASSWORD": snowflake_password}

# COMMAND ----------

deps_result = subprocess.run(
    ["dbt", "deps"],
    cwd=dbt_project_path,
    capture_output=True,
    text=True,
    env=env,
)
print(deps_result.stdout)
print(deps_result.stderr)
if deps_result.returncode != 0:
    raise Exception(f"dbt deps failed with exit code {deps_result.returncode}")

# COMMAND ----------

build_result = subprocess.run(
    ["dbt", "build", "--target", target],
    cwd=dbt_project_path,
    capture_output=True,
    text=True,
    env=env,
)
print(build_result.stdout)
print(build_result.stderr)
if build_result.returncode != 0:
    raise Exception(f"dbt build failed with exit code {build_result.returncode}")

# COMMAND ----------

# dbt deps already ran above in this same session -- no need to repeat
# it before docs generate, since the packages are already present in
# dbt_packages/ for this notebook run.
docs_result = subprocess.run(
    ["dbt", "docs", "generate", "--target", target],
    cwd=dbt_project_path,
    capture_output=True,
    text=True,
    env=env,
)
print(docs_result.stdout)
print(docs_result.stderr)
if docs_result.returncode != 0:
    raise Exception(f"dbt docs generate failed with exit code {docs_result.returncode}")

print("dbt deps + build + docs generate completed successfully")