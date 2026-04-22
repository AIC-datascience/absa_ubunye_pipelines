# Databricks notebook source
# MAGIC %md
# MAGIC # policy_device_mapping - dev + production notebook
# MAGIC
# MAGIC Joins three Unity Catalog tables (policy-device details, user-IMEI,
# MAGIC MI exposure) into a single curated Delta table. Driven by the
# MAGIC `telematics_policy_device_mapping` job in `bundles/telematics.yml`.
# MAGIC Also usable interactively by a data scientist after attaching to the
# MAGIC workspace (matches the dev-notebook pattern in `docs/deployment.md`).

# COMMAND ----------

dbutils.widgets.text("task_dir", "", "Absolute workspace path to the policy_device_mapping task")
dbutils.widgets.text("dt", "", "Data timestamp (YYYY-MM-DD)")
dbutils.widgets.dropdown("mode", "PROD", ["DEV", "PROD"], "Run mode")
dbutils.widgets.text("telm_catalog", "", "Unity Catalog catalog")
dbutils.widgets.text("telm_schema", "", "Unity Catalog schema")

task_dir = dbutils.widgets.get("task_dir")
assert task_dir, "task_dir must be supplied by the job (see bundles/telematics.yml)"
assert dbutils.widgets.get("telm_catalog"), "telm_catalog must be supplied"
assert dbutils.widgets.get("telm_schema"), "telm_schema must be supplied"

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
