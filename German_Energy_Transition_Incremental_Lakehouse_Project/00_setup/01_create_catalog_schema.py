# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ../01_config/01_bronze_config

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup: catalog and schemas
# MAGIC Creates the bronze, silver, gold and orchestration schemas.
# MAGIC Every statement is `IF NOT EXISTS`,
# MAGIC so this notebook is safe to re-run.
# MAGIC
# MAGIC This assumes the catalog itself and the `landing` schema/volume already
# MAGIC exist with the raw OPSD files staged under `year=YYYY` folders - see the
# MAGIC top-level README for how that volume is expected to be laid out.

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.gold")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.orchestration")

print(f"Catalog and all project schemas are ready under {catalog_name}")
