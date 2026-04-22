# Databricks notebook source
# MAGIC %md
# MAGIC # flood_risk - dev + production notebook
# MAGIC
# MAGIC Task 2 of the ABSA flood-risk pipeline. Reads `address_geocoded` from
# MAGIC Task 1, calls the JBA floodscores + flooddepths endpoints, merges the
# MAGIC two response sets, and writes `address_flood_risk` to Unity Catalog.
# MAGIC Driven by the `flood_risk` task in the `flood` job (see
# MAGIC `bundles/flood.yml`), which depends on `geocode_addresses`.
# MAGIC
# MAGIC The JBA basic-auth credential arrives via a widget that the DAB
# MAGIC populates from a Databricks secret scope using the
# MAGIC `{{secrets/scope/key}}` syntax.

# COMMAND ----------

dbutils.widgets.text("task_dir", "", "Absolute workspace path to the flood_risk task")
dbutils.widgets.text("dt", "", "Data timestamp (YYYY-MM-DD)")
dbutils.widgets.dropdown("mode", "PROD", ["DEV", "PROD"], "Run mode")
dbutils.widgets.text("telm_catalog", "", "Unity Catalog catalog")
dbutils.widgets.text("telm_schema", "", "Unity Catalog schema")
dbutils.widgets.text("jba_basic_auth", "", "JBA basic auth header (supplied via {{secrets/...}})")

task_dir = dbutils.widgets.get("task_dir")
assert task_dir, "task_dir must be supplied by the job (see bundles/flood.yml)"
assert dbutils.widgets.get("telm_catalog"), "telm_catalog must be supplied"
assert dbutils.widgets.get("telm_schema"), "telm_schema must be supplied"
assert dbutils.widgets.get("jba_basic_auth"), "jba_basic_auth must be supplied"

# COMMAND ----------

# MAGIC %pip install "ubunye-engine[spark]==0.1.7"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os

task_dir = dbutils.widgets.get("task_dir")
dt = dbutils.widgets.get("dt") or None
mode = dbutils.widgets.get("mode")

os.environ["TELM_CATALOG"] = dbutils.widgets.get("telm_catalog")
os.environ["TELM_SCHEMA"] = dbutils.widgets.get("telm_schema")
os.environ["JBA_BASIC_AUTH"] = dbutils.widgets.get("jba_basic_auth")

# COMMAND ----------

import ubunye

outputs = ubunye.run_task(
    task_dir=task_dir,
    dt=dt,
    mode=mode,
    lineage=True,
)

print(f"Outputs written: {list(outputs.keys())}")

# COMMAND ----------

for name, df in outputs.items():
    print(f"--- {name} ---")
    print(f"Row count: {df.count()}")
    df.printSchema()
