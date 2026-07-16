# Databricks notebook source
# MAGIC %run ./01_bronze_config

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver config
# MAGIC Reuses the catalog and column-scope settings from bronze_config, and adds
# MAGIC the silver-specific table name, merge key and partition columns.

# COMMAND ----------

silver_schema = "silver"
silver_table = f"{catalog_name}.{silver_schema}.germany_energy_clean"

# Same key as bronze: one row per hour, keyed on the UTC timestamp
silver_merge_key = bronze_merge_key

# Silver is partitioned by year and month. At ~50,000 total rows this is not
# needed for performance - it is here to demonstrate a partitioning pattern
# that would matter at a larger data volume (see README).
silver_partition_columns = ["year", "month"]
