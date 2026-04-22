# Databricks notebook source
# MAGIC %md
# MAGIC # geocode_addresses - dev + production notebook
# MAGIC
# MAGIC Task 1 of the ABSA flood-risk pipeline. Geocodes rows from the address
# MAGIC source table via TomTom and writes `address_geocoded` back to Unity
# MAGIC Catalog. Driven by the `geocode_addresses_flood` job in
# MAGIC `bundles/flood.yml`; the same notebook runs interactively when a data
# MAGIC scientist attaches to it in the workspace (matches the dev-notebook
# MAGIC pattern documented in `docs/deployment.md`).
# MAGIC
# MAGIC Secrets and UC identifiers arrive as widgets. In the job, the DAB
# MAGIC injects them via `base_parameters` (including `{{secrets/scope/key}}`
# MAGIC interpolation for TomTom); interactively you paste real values.

# COMMAND ----------

dbutils.widgets.text("task_dir", "", "Absolute workspace path to the geocode_addresses task")
dbutils.widgets.text("dt", "", "Data timestamp (YYYY-MM-DD)")
dbutils.widgets.dropdown("mode", "PROD", ["DEV", "PROD"], "Run mode")
dbutils.widgets.text("telm_catalog", "", "Unity Catalog catalog")
dbutils.widgets.text("telm_schema", "", "Unity Catalog schema")
dbutils.widgets.text("address_source_table", "", "Source table name (id, address)")
dbutils.widgets.text("tomtom_api_key", "", "TomTom API key (supplied via {{secrets/...}})")

task_dir = dbutils.widgets.get("task_dir")
assert task_dir, "task_dir must be supplied by the job (see bundles/flood.yml)"
assert dbutils.widgets.get("telm_catalog"), "telm_catalog must be supplied"
assert dbutils.widgets.get("telm_schema"), "telm_schema must be supplied"
assert dbutils.widgets.get("address_source_table"), "address_source_table must be supplied"
assert dbutils.widgets.get("tomtom_api_key"), "tomtom_api_key must be supplied"

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
os.environ["ADDRESS_SOURCE_TABLE"] = dbutils.widgets.get("address_source_table")
os.environ["TOMTOM_API_KEY"] = dbutils.widgets.get("tomtom_api_key")

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
